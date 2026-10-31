#!/usr/bin/env python3
"""Bounded, source-pinned ForkResolutionSpec fixed/old-product comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET


RUNTIME = "src/main/scala/org/ergoplatform/nodeView/history/ErgoHistoryReader.scala"
TEST = "src/it/scala/org/ergoplatform/it/ForkResolutionSpec.scala"
SCRIPT = ".github/scripts/pr2863-fork-oracle.py"
WORKFLOW = ".github/workflows/pr2863-fork-oracle.yml"
FIXED_RUNTIME_BLOB = "12c825296277a3bbd9c7c408e1512982e60bc652"
OLD_RUNTIME_BLOB = "385bb7893062779b14bd51128cea98dd552d6d44"
IMAGE_TAG = "org.ergoplatform/ergo"
SUITE = "org.ergoplatform.it.ForkResolutionSpec"
CONTROL = "should Fork resolution after isolated mining"
FORK = "should switch a V2 fork with no selected-chain header in its locator"
ADOPTION_WAIT = "forked peer's selected tip after V2 continuation"
STARTED = re.compile(
    r"Started node (?P<label>\d+-node\d+): NodeInfo\([^)]*,(?P<id>[0-9a-f]{64})\)"
)
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")


class Inconclusive(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def command(args: list[str], cwd: Path | None = None, timeout: int = 60) -> str:
    try:
        result = subprocess.run(
            args, cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise Inconclusive("command_unavailable_or_timed_out") from exc
    if result.returncode:
        raise Inconclusive("command_failed")
    return result.stdout.strip()


def git(repo: Path, *args: str) -> str:
    return command(["git", "-C", str(repo), *args])


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def numbered_lines(path: Path, needle: str) -> int:
    if not path.is_file():
        raise Inconclusive("node_log_missing")
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as src:
        for line in src:
            if needle in line:
                count += 1
    return count


def docker_ids() -> list[str]:
    output = command(
        [
            "docker", "ps", "-a", "--no-trunc", "--filter", "name=ergo-itest-",
            "--format", "{{.ID}}",
        ],
        timeout=15,
    )
    return [line for line in output.splitlines() if line]


def cleanup_new_node_containers(pre_arm: set[str]) -> bool:
    """On IT timeout, remove only ergo-itest containers absent before this arm."""
    failed = False
    try:
        current = set(docker_ids())
    except Inconclusive:
        return False
    for container_id in sorted(current - pre_arm):
        try:
            command(["docker", "rm", "-f", container_id], timeout=30)
        except Inconclusive:
            failed = True
    try:
        remaining = set(docker_ids()) - pre_arm
    except Inconclusive:
        return False
    return not failed and not remaining


class ImageObserver:
    """Inspect node containers while alive; the suite removes them on close."""

    def __init__(self) -> None:
        self.seen: dict[str, str] = {}
        self.ready = threading.Event()
        self.stopping = threading.Event()
        self.failed = False
        self.worker = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        while not self.stopping.is_set():
            try:
                for container_id in docker_ids():
                    if container_id in self.seen:
                        continue
                    try:
                        raw = command(["docker", "inspect", container_id], timeout=15)
                        item = json.loads(raw)[0]
                        if str(item.get("Name", "")).startswith("/ergo-itest-"):
                            self.seen[str(item["Id"])] = str(item["Image"])
                    except (Inconclusive, KeyError, IndexError, ValueError):
                        # Removal can race the inspect. The started-id reconciliation
                        # below makes a missed container inconclusive.
                        pass
            except Inconclusive:
                self.failed = True
            self.ready.set()
            self.stopping.wait(0.25)

    def start(self) -> None:
        self.worker.start()
        if not self.ready.wait(15) or self.failed:
            raise Inconclusive("container_observer_unavailable")

    def stop(self) -> None:
        self.stopping.set()
        self.worker.join(timeout=20)
        if self.worker.is_alive() or self.failed:
            raise Inconclusive("container_observer_failed")


def timed_command(args: list[str], cwd: Path, log: Path, deadline: float,
                  env: dict[str, str]) -> tuple[int, bool]:
    seconds = int(deadline - time.monotonic())
    if seconds <= 0:
        return 124, True
    with log.open("xb") as sink:
        try:
            process = subprocess.Popen(args, cwd=cwd, env=env, stdout=sink,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        except OSError as exc:
            raise Inconclusive("sbt_unavailable") from exc
        try:
            return process.wait(timeout=seconds), False
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            # The sbt leader may have exited while its forked test JVM remains.
            # Kill any surviving members of the same process group as well.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                raise Inconclusive("process_group_would_not_stop")
            return 124, True


def sbt_version(log: Path, expected: str) -> str:
    pattern = re.compile(r"welcome to sbt ([0-9][A-Za-z0-9.+-]*)", re.I)
    with log.open("r", encoding="utf-8", errors="replace") as src:
        for line in src:
            match = pattern.search(line)
            if match:
                if match.group(1) != expected:
                    raise Inconclusive("sbt_version_mismatch")
                return match.group(1)
    raise Inconclusive("sbt_version_unobserved")


def image_jar_digest(image_id: str, scratch: Path) -> str:
    container = command(["docker", "create", image_id, "true"], timeout=30)
    if not re.fullmatch(r"[0-9a-f]{64}", container):
        raise Inconclusive("jar_probe_container_id_invalid")
    jar = scratch / "image-ergo.jar"
    try:
        command(["docker", "cp", f"{container}:/opt/ergo/ergo.jar", str(jar)], timeout=60)
        if not jar.is_file():
            raise Inconclusive("image_jar_missing")
        return digest(jar)
    finally:
        command(["docker", "rm", container], timeout=30)


def xml_cases(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
        raise Inconclusive("it_xml_missing_or_large")
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise Inconclusive("it_xml_invalid") from exc
    if root.tag != "testsuite" or root.attrib.get("name") != SUITE:
        raise Inconclusive("it_xml_wrong_suite")
    cases = list(root.findall("testcase"))
    if len(cases) != 2 or root.attrib.get("tests") != "2":
        raise Inconclusive("it_xml_wrong_test_count")
    result: dict[str, dict[str, str]] = {}
    for case in cases:
        name = case.attrib.get("name", "")
        faults = [item for item in case if item.tag in ("failure", "error", "skipped")]
        if len(faults) > 1 or name in result:
            raise Inconclusive("it_xml_ambiguous_case")
        if not faults:
            result[name] = {"status": "passed", "phase": ""}
            continue
        fault = faults[0]
        detail = " ".join((fault.attrib.get("type", ""), fault.attrib.get("message", ""),
                           fault.text or ""))
        phase = "adoption_wait" if (
            "NoProgressException" in detail and ADOPTION_WAIT in detail
            and "node02" in detail and "peersCount=1" in detail
            and "isMining=false" in detail
        ) else "other"
        result[name] = {"status": fault.tag, "phase": phase}
    if set(result) != {CONTROL, FORK}:
        raise Inconclusive("it_xml_wrong_cases")
    return result


def node_observation(worktree: Path, observer: ImageObserver,
                     expected_image: str) -> dict[str, object]:
    test_log = worktree / "target/logs/test.log"
    if not test_log.is_file() or test_log.stat().st_size > 32 * 1024 * 1024:
        raise Inconclusive("test_log_missing_or_large")
    starts: list[tuple[int, str, str]] = []
    with test_log.open("r", encoding="utf-8", errors="replace") as src:
        for line in src:
            match = STARTED.search(line)
            if match:
                label = match.group("label")
                starts.append((int(label.split("-")[0]), label, match.group("id")))
    starts.sort()
    if len(starts) != 21 or len({item[2] for item in starts}) != len(starts):
        raise Inconclusive("node_start_count_unexpected")
    expected_ids = {item[2] for item in starts}
    if set(observer.seen) != expected_ids:
        raise Inconclusive("node_image_observation_incomplete")
    if any(image != expected_image for image in observer.seen.values()):
        raise Inconclusive("node_image_mismatch")
    if not (starts[-2][1].endswith("-node01") and starts[-1][1].endswith("-node02")):
        raise Inconclusive("final_peer_identity_ambiguous")
    log_root = worktree / "target/logs"
    responder = log_root / f"ForkResolutionSpec-{starts[-2][1]}-{starts[-2][2][:12]}.log"
    follower = log_root / f"ForkResolutionSpec-{starts[-1][1]}-{starts[-1][2][:12]}.log"
    branch_lines = numbered_lines(responder, "sending up to 400 header ids from genesis")
    empty_lines = numbered_lines(responder, "Extension is empty while comparison is younger")
    return {
        "started_count": len(starts),
        "container_ids": [item[2] for item in starts],
        "responder_id": starts[-2][2],
        "follower_id": starts[-1][2],
        "responder_log_sha256": digest(responder),
        "follower_log_sha256": digest(follower),
        "from_genesis_count": branch_lines,
        "empty_extension_count": empty_lines,
    }


def verify_source(repo: Path, pr_head: str, base: str, test_blob: str) -> dict[str, str]:
    candidate = git(repo, "rev-parse", "HEAD")
    if command(["git", "-C", str(repo), "status", "--porcelain"]):
        raise Inconclusive("candidate_checkout_dirty")
    command(["git", "-C", str(repo), "merge-base", "--is-ancestor", pr_head, candidate])
    command(["git", "-C", str(repo), "merge-base", "--is-ancestor", base, pr_head])
    paths = set(git(repo, "diff", "--name-only", pr_head, candidate).splitlines())
    if paths != {TEST, SCRIPT, WORKFLOW}:
        raise Inconclusive("candidate_scope_mismatch")
    if git(repo, "rev-parse", f"HEAD:{TEST}") != test_blob:
        raise Inconclusive("test_blob_mismatch")
    if git(repo, "rev-parse", f"HEAD:{RUNTIME}") != FIXED_RUNTIME_BLOB:
        raise Inconclusive("fixed_product_blob_mismatch")
    if git(repo, "rev-parse", f"{base}:{RUNTIME}") != OLD_RUNTIME_BLOB:
        raise Inconclusive("old_product_blob_mismatch")
    properties = (repo / "project/build.properties").read_text(encoding="utf-8")
    if "sbt.version=1.11.1" not in properties:
        raise Inconclusive("sbt_pin_mismatch")
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=20)
    if java.returncode or "1.8.0" not in java.stderr + java.stdout:
        raise Inconclusive("jdk8_not_observed")
    return {
        "candidate_commit": candidate,
        "test_blob": test_blob,
        "script_blob": git(repo, "rev-parse", f"HEAD:{SCRIPT}"),
        "workflow_blob": git(repo, "rev-parse", f"HEAD:{WORKFLOW}"),
        "fixed_product_blob": FIXED_RUNTIME_BLOB,
        "old_product_blob": OLD_RUNTIME_BLOB,
        "sbt_pin": "1.11.1",
        "scala_pin": "2.12.20",
        "jdk_major": "8",
    }


def prepare_worktrees(repo: Path, root: Path, candidate: str, base: str,
                      test_blob: str) -> tuple[Path, Path, dict[str, str]]:
    fixed, old = root / "fixed", root / "old"
    git(repo, "worktree", "add", "--detach", str(fixed), candidate)
    git(repo, "worktree", "add", "--detach", str(old), candidate)
    git(old, "restore", "--source", base, "--staged", "--worktree", "--", RUNTIME)
    if git(fixed, "rev-parse", f"HEAD:{TEST}") != test_blob:
        raise Inconclusive("fixed_test_blob_changed")
    if git(old, "rev-parse", f"HEAD:{TEST}") != test_blob:
        raise Inconclusive("old_test_blob_changed")
    if git(old, "rev-parse", f":{RUNTIME}") != OLD_RUNTIME_BLOB:
        raise Inconclusive("old_runtime_restore_failed")
    if set(git(old, "diff", "--name-only", candidate).splitlines()) != {RUNTIME}:
        raise Inconclusive("old_variant_extra_diff")
    fixed_tree, old_tree = git(fixed, "write-tree"), git(old, "write-tree")
    if set(git(repo, "diff", "--name-only", fixed_tree, old_tree).splitlines()) != {RUNTIME}:
        raise Inconclusive("variant_tree_diff_mismatch")
    return fixed, old, {"fixed_tree": fixed_tree, "old_tree": old_tree}


def run_arm(name: str, worktree: Path, minutes: int) -> dict[str, object]:
    pre_arm_ids = set(docker_ids())
    if pre_arm_ids:
        raise Inconclusive("preexisting_node_container")
    temp = worktree / "tmp"
    temp.mkdir()
    env = dict(os.environ, TMPDIR=str(temp))
    deadline = time.monotonic() + minutes * 60
    build_log = worktree / "build.log"
    build_rc, build_timeout = timed_command(
        ["sbt", "-batch", "-Denv=test", "clean", "++2.12.20", "docker"],
        worktree, build_log, deadline, env,
    )
    if build_timeout or build_rc:
        raise Inconclusive("build_timeout" if build_timeout else "build_failed")
    pin = (worktree / "project/build.properties").read_text(encoding="utf-8")
    expected_sbt = pin.strip().split("=", 1)[-1]
    actual_sbt = sbt_version(build_log, expected_sbt)
    jars = list((worktree / "target/scala-2.12").glob("ergo-*.jar"))
    if len(jars) != 1 or jars[0].stat().st_size < 1024 * 1024:
        raise Inconclusive("assembly_jar_ambiguous")
    jar_sha = digest(jars[0])
    image_id = command(["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE_TAG])
    if not IMAGE_ID.fullmatch(image_id):
        raise Inconclusive("built_image_id_invalid")
    if image_jar_digest(image_id, temp) != jar_sha:
        raise Inconclusive("image_jar_mismatch")
    observer = ImageObserver()
    observer.start()
    test_rc, test_timeout = -1, False
    test_error: Exception | None = None
    observer_error = False
    cleanup_ok = True
    try:
        test_rc, test_timeout = timed_command(
            ["sbt", "-batch", "-Denv=test", "++2.12.20", f"it:testOnly {SUITE}"],
            worktree, worktree / "it.log", deadline, env,
        )
    except Exception as exc:
        test_error = exc
    finally:
        try:
            observer.stop()
        except Inconclusive:
            observer_error = True
        if test_timeout or test_error is not None:
            cleanup_ok = cleanup_new_node_containers(pre_arm_ids)
    if not cleanup_ok:
        raise Inconclusive("it_failure_cleanup_failed")
    if test_error is not None:
        raise test_error
    if test_timeout:
        raise Inconclusive("it_timeout")
    if observer_error:
        raise Inconclusive("container_observer_failed")
    if command(["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE_TAG]) != image_id:
        raise Inconclusive("image_tag_changed_during_test")
    if docker_ids():
        raise Inconclusive("node_container_leftover")
    cases = xml_cases(worktree / "target/it-reports" / f"TEST-{SUITE}.xml")
    nodes = node_observation(worktree, observer, image_id)
    return {
        "arm": name,
        "sbt_version": actual_sbt,
        "build_exit": build_rc,
        "test_exit": test_rc,
        "assembly_jar_sha256": jar_sha,
        "image_id": image_id,
        "image_jar_sha256": jar_sha,
        "xml": cases,
        "nodes": nodes,
    }


def verdict(fixed: dict[str, object], old: dict[str, object]) -> None:
    f_cases, o_cases = fixed["xml"], old["xml"]
    f_nodes, o_nodes = fixed["nodes"], old["nodes"]
    if not isinstance(f_cases, dict) or not isinstance(o_cases, dict):
        raise Inconclusive("xml_result_invalid")
    if not isinstance(f_nodes, dict) or not isinstance(o_nodes, dict):
        raise Inconclusive("node_result_invalid")
    if fixed["test_exit"] != 0 or any(c["status"] != "passed" for c in f_cases.values()):
        raise Inconclusive("fixed_cases_not_green")
    if f_nodes["from_genesis_count"] < 1:
        raise Inconclusive("fixed_response_not_observed")
    if fixed["assembly_jar_sha256"] == old["assembly_jar_sha256"]:
        raise Inconclusive("product_jar_not_distinct")
    if fixed["image_id"] == old["image_id"]:
        raise Inconclusive("product_image_not_distinct")
    if old["test_exit"] == 0:
        raise Inconclusive("old_product_converged")
    if o_cases[CONTROL]["status"] != "passed":
        raise Inconclusive("old_control_not_green")
    if o_cases[FORK] != {"status": "failure", "phase": "adoption_wait"}:
        raise Inconclusive("old_failure_not_adoption")
    if o_nodes["empty_extension_count"] < 1:
        raise Inconclusive("old_empty_response_not_observed")


def write_result(out: Path, facts: dict[str, object]) -> None:
    payload = json.dumps(facts, sort_keys=True, separators=(",", ":")) + "\n"
    if len(payload.encode("utf-8")) > 32768:
        facts = {"schema": "pr2863-fork-oracle-v1", "status": "inconclusive",
                 "reason": "facts_too_large"}
        payload = json.dumps(facts, sort_keys=True, separators=(",", ":")) + "\n"
    with (out / "facts.json").open("x", encoding="utf-8", newline="\n") as target:
        target.write(payload)
    lines = [
        "PR #2863 fork oracle",
        f"status: {facts['status']}",
        f"reason: {facts.get('reason', 'none')}",
        f"candidate: {facts.get('candidate_commit', 'unverified')}",
        f"fixed: {facts.get('fixed_status', 'unverified')}",
        f"old: {facts.get('old_status', 'unverified')}",
    ]
    summary = "\n".join(lines) + "\n"
    with (out / "summary.txt").open("x", encoding="utf-8", newline="\n") as target:
        target.write(summary[:4096])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--pr-head", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--test-blob", required=True)
    parser.add_argument("--arm-minutes", type=int, required=True)
    args = parser.parse_args()
    for value in (args.pr_head, args.base, args.test_blob):
        if not SHA40.fullmatch(value):
            parser.error("commit/blob pins must be lowercase 40-hex")
    if not 5 <= args.arm_minutes <= 60:
        parser.error("arm-minutes must be 5..60")
    repo, out = Path(args.repo).resolve(), Path(args.out).resolve()
    runner_temp = Path(os.environ.get("RUNNER_TEMP", "")).resolve()
    if not os.environ.get("RUNNER_TEMP") or runner_temp not in out.parents:
        parser.error("out must be a new child of RUNNER_TEMP")
    if repo in out.parents or out.exists():
        parser.error("out must be new and outside the source checkout")
    out.mkdir(parents=True)
    facts: dict[str, object] = {
        "schema": "pr2863-fork-oracle-v1",
        "status": "inconclusive",
        "reason": "not_started",
        "pr_head": args.pr_head,
        "base": args.base,
        "test_blob": args.test_blob,
    }
    try:
        pins = verify_source(repo, args.pr_head, args.base, args.test_blob)
        facts.update(pins)
        root = runner_temp / "pr2863-fork-oracle-work"
        if root.exists():
            raise Inconclusive("work_root_exists")
        root.mkdir()
        fixed_path, old_path, trees = prepare_worktrees(
            repo, root, pins["candidate_commit"], args.base, args.test_blob
        )
        facts.update(trees)
        fixed = run_arm("fixed", fixed_path, args.arm_minutes)
        facts["fixed"] = fixed
        facts["fixed_status"] = "passed" if fixed["test_exit"] == 0 else "failed"
        # A failed fixed arm cannot establish that the old product is the cause.
        if fixed["test_exit"] != 0 or any(
            item["status"] != "passed" for item in fixed["xml"].values()
        ) or fixed["nodes"]["from_genesis_count"] < 1:
            raise Inconclusive("fixed_oracle_not_green")
        old = run_arm("old", old_path, args.arm_minutes)
        facts["old"] = old
        facts["old_status"] = "passed" if old["test_exit"] == 0 else "failed"
        verdict(fixed, old)
        facts["status"], facts["reason"] = "decisive", "fixed_green_old_adoption_red"
    except Inconclusive as exc:
        facts["status"], facts["reason"] = "inconclusive", exc.code
    except Exception:
        facts["status"], facts["reason"] = "inconclusive", "internal_error"
    write_result(out, facts)
    print(f"pr2863-fork-oracle: {facts['status']} ({facts['reason']})")
    return 0 if facts["status"] == "decisive" else 1


if __name__ == "__main__":
    sys.exit(main())

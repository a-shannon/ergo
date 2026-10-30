"""Fixed source mutants for one pinned recovered-root contribution."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

BASE = "8d01a1cea57e4604adb41ff4b02f7bd4b5fcd37a"
BASELINE_NATIVE_RUN = 41085026637
BASELINE_NATIVE_RECEIPT_SHA256 = "D7C67FA66AD103A0B3483E5B59B94F00C9FFC6303A8272A1B242319CD258043E"
BRANCH = "refs/heads/a-shannon/pr2513-root-mutation-oracle-20261030"
WORKFLOW = ".github/workflows/pr2513-root-mutation-oracle.yml"
DRIVER = ".github/scripts/pr2513-root-mutation-oracle.py"
SOURCE = "src/main/scala/org/ergoplatform/nodeView/state/UtxoState.scala"
DIRECT = "org.ergoplatform.nodeView.state.UtxoStateRollbackSpecification"
HOLDER = "org.ergoplatform.nodeView.viewholder.ErgoNodeViewHolderSpec"
PINS = {
    SOURCE: "c9c39d804af46395bf169658395274901be2de2b",
    "src/test/scala/org/ergoplatform/nodeView/state/UtxoStateRollbackSpecification.scala":
        "e2650a308ad87ef0ce2772c79b22ebf0a38ed7bf",
    "src/test/scala/org/ergoplatform/nodeView/viewholder/ErgoNodeViewHolderSpec.scala":
        "cf7755c3615a7d029843be35d6a026e08b2671be",
}
HASH = "rollback rejects a recovered root with the wrong hash"
HEIGHT = "rollback rejects a recovered root with the wrong height"
CAUSE = "rollback propagates a prover failure without publishing the requested version"
SUCCESS = "rollback publishes the requested version only with its stored root"
JOIN = "stop without installing a state when rollback recovers a mismatched prover"
GUARD = b'          require(p.digest.sameElements(rootHash), "Unexpected root digest after rollback")'
MUTANTS = {
    "guard_removed": b"          ()",
    "hash32_only": GUARD.replace(b"p.digest.sameElements(rootHash)",
                                b"p.digest.take(32).sameElements(rootHash.take(32))"),
}


class Rejected(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Rejected(reason)


def git(*args):
    result = subprocess.run(["git", *args], capture_output=True, check=False)
    require(result.returncode == 0, "git_check")
    return result.stdout


def expected(mode, suite):
    if suite == HOLDER:
        return {JOIN: ("ErgoNodeViewHolderSpec.scala", 1181)}
    return {HASH: ("UtxoStateRollbackSpecification.scala", 46),
            HEIGHT: ("UtxoStateRollbackSpecification.scala", 46)} if mode == "guard_removed" else {
                HEIGHT: ("UtxoStateRollbackSpecification.scala", 46)}


def parse_report(directory, mode, suite, exit_code):
    """Accept only the pinned assertion failures, never an arbitrary red run."""
    require(exit_code == 1, "process_exit")
    reports = list(directory.glob("*.xml"))
    require(len(reports) == 1, "report_count")
    data = reports[0].read_bytes()
    require(b"<!DOCTYPE" not in data and b"<!ENTITY" not in data, "xml_declaration")
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        raise Rejected("xml_parse") from None
    names = [JOIN] if suite == HOLDER else [HASH, HEIGHT, CAUSE, SUCCESS]
    failures = expected(mode, suite)
    cases = root.findall("testcase")
    require(root.tag == "testsuite" and root.get("name") == suite, "suite_identity")
    require(root.get("tests") == str(len(names)) and len(cases) == len(names), "case_count")
    require(root.get("errors") == "0" and not root.findall(".//error"), "suite_error")
    require(not root.findall(".//skipped"), "skipped_test")
    require(root.get("failures") == str(len(failures)), "failure_count")
    require(sorted(case.get("name", "") for case in cases) == sorted(names), "case_names")
    for case in cases:
        name = case.get("name")
        require(case.get("classname") == suite, "case_class")
        failed = case.findall("failure")
        require(len(failed) == int(name in failures), "case_outcome")
        require(all(child.tag == "failure" for child in case), "case_child")
        if failed:
            failure = failed[0]
            require(failure.get("type") == "class org.scalatest.exceptions.TestFailedException",
                    "failure_type")
            require(failure.get("message") == "false was not equal to true", "assertion_message")
            filename, line = failures[name]
            # The first frame belonging to this spec must be the result.isFailure assertion.
            locations = re.findall(re.escape(filename) + r":([0-9]+)\)", failure.text or "")
            require(bool(locations) and int(locations[0]) == line, "assertion_site")
    return {"mode": mode, "suite": suite, "exit_code": exit_code, "tests": len(names),
            "passed": len(names) - len(failures), "expected_failures": len(failures),
            "failures": [{"name": name, "file": site[0], "line": site[1]}
                         for name, site in sorted(failures.items())]}


def verify_guard(original):
    require(original.count(GUARD) == 1, "guard_count")
    require(len(original.splitlines()) > 57 and original.splitlines()[57] == GUARD, "guard_line")


def check_candidate():
    require(os.environ.get("GITHUB_REPOSITORY") == "a-shannon/ergo", "repository")
    require(os.environ.get("GITHUB_REF") == BRANCH, "branch")
    require(os.environ.get("GITHUB_EVENT_NAME") == "push", "event")
    head = git("rev-parse", "HEAD").decode().strip()
    require(git("rev-list", "--parents", "-n", "1", "HEAD").decode().split() == [head, BASE],
            "candidate_parent")
    changes = git("diff", "--name-status", BASE, "HEAD").decode().splitlines()
    require(sorted(changes) == sorted(["A\t" + WORKFLOW, "A\t" + DRIVER]), "candidate_diff")
    require(not git("status", "--porcelain", "--untracked-files=all",
                    "--ignore-submodules=all").strip(), "dirty_checkout")
    for path, blob in PINS.items():
        require(git("rev-parse", BASE + ":" + path).decode().strip() == blob, "base_blob")
        require(git("hash-object", path).decode().strip() == blob, "working_blob")
    original = Path(SOURCE).read_bytes()
    verify_guard(original)
    return original


def run_mutants():
    original = check_candidate()
    rows = []
    deadline = time.monotonic() + 24 * 60
    try:
        with tempfile.TemporaryDirectory(prefix="root-oracle-", dir=os.environ["RUNNER_TEMP"]) as raw:
            for mode, replacement in MUTANTS.items():
                # Restore exact candidate bytes before each independent one-line mutation.
                Path(SOURCE).write_bytes(original)
                require(git("hash-object", SOURCE).decode().strip() == PINS[SOURCE], "restore_blob")
                Path(SOURCE).write_bytes(original.replace(GUARD, replacement, 1))
                mutant_blob = git("hash-object", SOURCE).decode().strip()
                require(git("diff", "--name-only").decode().splitlines() == [SOURCE], "mutation_diff")
                executions = []
                for suite in (DIRECT, HOLDER):
                    reports = Path(raw) / mode / suite.rsplit(".", 1)[-1]
                    reports.mkdir(parents=True)
                    command = f'testOnly {suite} -- -u "{reports}"'
                    if suite == HOLDER:
                        command += f' -t "{JOIN}"'
                    remaining = int(deadline - time.monotonic())
                    require(remaining > 0, "timeout")
                    # No shell, caller command, raw XML, or raw log is published.
                    with (reports.parent / (suite.rsplit(".", 1)[-1] + ".log")).open("wb") as log:
                        try:
                            result = subprocess.run(
                                ["sbt", "-batch", "-Denv=test", "++2.12.20", command],
                                stdout=log, stderr=subprocess.STDOUT, timeout=remaining, check=False)
                            code = result.returncode
                        except subprocess.TimeoutExpired:
                            raise Rejected("timeout") from None
                    executions.append((reports, suite, code))
                # Both sbt invocations finish before interpreting either expected red result.
                for reports, suite, code in executions:
                    row = parse_report(reports, mode, suite, code)
                    row["mutated_source_blob"] = mutant_blob
                    row["mutated_line"] = replacement.decode().strip()
                    rows.append(row)
    finally:
        Path(SOURCE).write_bytes(original)
        require(git("hash-object", SOURCE).decode().strip() == PINS[SOURCE], "final_restore")
        require(not git("diff", "--name-only").strip(), "final_diff")
    return {"status": "expected_assertions_observed", "source_commit": BASE,
            "source_blobs": PINS, "changed_source": SOURCE, "changed_line": 58,
            # Parent-verified prior evidence; this mutation job does not replay the baseline.
            "baseline_native_run": BASELINE_NATIVE_RUN,
            "baseline_native_receipt_sha256": BASELINE_NATIVE_RECEIPT_SHA256,
            "scala": "2.12.20", "jdk": "8", "results": rows}


def self_test():
    """Synthetic parser checks; these do not execute Scala or establish runtime results."""
    count = 0
    with tempfile.TemporaryDirectory(prefix="root-oracle-parser-") as temporary:
        directory = Path(temporary)
        for mode in MUTANTS:
            for suite in (DIRECT, HOLDER):
                names = [JOIN] if suite == HOLDER else [HASH, HEIGHT, CAUSE, SUCCESS]
                failures = expected(mode, suite)
                root = ET.Element("testsuite", name=suite, tests=str(len(names)), errors="0",
                                  failures=str(len(failures)))
                for name in names:
                    case = ET.SubElement(root, "testcase", name=name, classname=suite)
                    if name in failures:
                        filename, line = failures[name]
                        ET.SubElement(case, "failure",
                                      type="class org.scalatest.exceptions.TestFailedException",
                                      message="false was not equal to true").text = (
                            f"org.scalatest.exceptions.TestFailedException: false was not equal to true"
                            f"\n at example.test({filename}:{line})")
                report = directory / "TEST-fixed.xml"
                valid = ET.tostring(root)
                report.write_bytes(valid)
                parse_report(directory, mode, suite, 1)
                count += 1
                bad_inputs = [
                    valid.replace(b'errors="0"', b'errors="1"'),
                    valid.replace(b'tests="' + str(len(names)).encode() + b'"', b'tests="0"'),
                    valid.replace(b'TestFailedException', b'IllegalStateException'),
                    valid.replace(b'false was not equal to true', b'other assertion'),
                    valid.replace(b':46)', b':43)').replace(b':1181)', b':1182)'),
                    valid.replace(b'<failure ', b'<skipped/><failure ', 1),
                    valid.replace(names[0].encode(), b"unexpected test"),
                    valid.replace(b'<testcase ', b'<error/><testcase ', 1),
                ]
                for bad in bad_inputs:
                    report.write_bytes(bad)
                    try:
                        parse_report(directory, mode, suite, 1)
                    except Rejected:
                        count += 1
                    else:
                        raise Rejected("self_test_false_accept")
                report.write_bytes(valid)
                for code in (0, 2, 124, -9):
                    try:
                        parse_report(directory, mode, suite, code)
                    except Rejected:
                        count += 1
                    else:
                        raise Rejected("self_test_exit_accept")
    print(json.dumps({"synthetic_parser_checks": count, "scala_execution": "not_run"}))


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        require(not sys.argv[1:], "arguments")
        output = Path(os.environ["RUNNER_TEMP"]) / "pr2513-root-mutation-oracle.json"
        require(not output.exists(), "stale_output")
        try:
            receipt = run_mutants()
            exit_code = 0
        except Rejected as error:
            receipt = {"status": "rejected", "reason": str(error), "source_commit": BASE}
            exit_code = 1
        except Exception:
            receipt = {"status": "rejected", "reason": "driver_error", "source_commit": BASE}
            exit_code = 1
        encoded = json.dumps(receipt, sort_keys=True)
        output.write_text(encoded + "\n", encoding="utf-8")
        print(encoded)
        sys.exit(exit_code)

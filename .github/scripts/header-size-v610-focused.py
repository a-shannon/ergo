"""Fixed header-size CI commands; raw logs and reports stay runner-temporary."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


BASE = "c4692c22835f2ae39da5c6cb124eaee63f57bf7e"
SOURCE = "4562758fcf85a085cf777a7537c540f6e283e288"
SOURCE_TREE = "794e4f0d4b01d30df5f31312a05f55df8c5d88cd"
DIAGNOSTIC_PARENT = "4420e06cce55354a10c6ab3b6f6717f74c046222"
PRODUCT = "ergo-core/src/main/scala/org/ergoplatform/modifiers/history/header/HeaderSerializer.scala"
TEST = "ergo-core/src/test/scala/org/ergoplatform/serialization/HeaderSizeSerializationSpec.scala"
PRODUCT_FIXED = "a36301c54af5bad75a939a0b669f0a4e27ab6660"
PRODUCT_BASE = "c7688bac617c18440780d845c8e85651e5a41264"
TEST_BLOB = "401c9e5a34dc1970792bb5ebe5b8441d7fdf59e4"
DIAGNOSTIC_FILES = (
    ".github/scripts/header-size-v610-focused.py",
    ".github/workflows/header-size-v610-focused.yml",
)
REPORT_DIRS = tuple(Path(p) / "target/test-reports" for p in (".", "ergo-core", "ergo-wallet", "avldb"))
HEADER_SUITE = "org.ergoplatform.serialization.HeaderSizeSerializationSpec"
ROUTE_SUITE = "org.ergoplatform.http.routes.BlocksApiRouteSpec"
STANDALONE = "standalone headers retain their serialized size, bytes and identity"
WRAPPED = "history-wrapped headers exclude the modifier type from their size"
PREFIXED = "consecutive headers count only their own bytes after a nonzero reader entry"
HEADER_NAMES = {STANDALONE, WRAPPED, PREFIXED}


class CheckFailure(Exception):
    """Fixed checker messages are safe to retain; test exception text is not."""


def require(condition, message):
    if not condition:
        raise CheckFailure(message)


def git(*args):
    result = subprocess.run(["git", *args], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False)
    require(result.returncode == 0, "git source check failed")
    return result.stdout


def verify_source():
    require(os.environ.get("GITHUB_REPOSITORY") == "a-shannon/ergo", "repository mismatch")
    require(os.environ.get("GITHUB_EVENT_NAME") == "push", "event mismatch")
    require(os.environ.get("GITHUB_REF") == "refs/heads/a-shannon/header-size-v610-validation-20261031", "ref mismatch")
    head = git("rev-parse", "HEAD").decode().strip()
    require(head == os.environ.get("GITHUB_SHA"), "checkout mismatch")
    require(git("show", "-s", "--format=%P", head).decode().split() == [DIAGNOSTIC_PARENT], "diagnostic parent mismatch")
    require(git("show", "-s", "--format=%P", DIAGNOSTIC_PARENT).decode().split() == [SOURCE], "original diagnostic parent mismatch")
    require(git("show", "-s", "--format=%P", SOURCE).decode().split() == [BASE], "source parent mismatch")
    require(git("rev-parse", SOURCE + "^{tree}").decode().strip() == SOURCE_TREE, "source tree mismatch")
    require(set(git("diff", "--name-status", SOURCE, head).decode().splitlines()) ==
            {"A\t" + p for p in DIAGNOSTIC_FILES}, "diagnostic scope mismatch")
    require(set(git("diff", "--name-status", BASE, SOURCE).decode().splitlines()) ==
            {"M\t" + PRODUCT, "A\t" + TEST}, "source scope mismatch")
    require(git("rev-parse", BASE + ":" + PRODUCT).decode().strip() == PRODUCT_BASE, "baseline product mismatch")
    for path, blob in ((PRODUCT, PRODUCT_FIXED), (TEST, TEST_BLOB)):
        require(git("rev-parse", SOURCE + ":" + path).decode().strip() == blob, "source blob mismatch")
        require(git("hash-object", path).decode().strip() == blob, "working blob mismatch")
    require(Path(TEST).read_text().splitlines()[13].strip() ==
            "parsed.sizeOpt shouldBe Some(encoded.length)", "assertion ordinal mismatch")
    git("diff", "--exit-code")
    git("diff", "--cached", "--exit-code")
    return {"diagnostic_commit": head, "diagnostic_parent": DIAGNOSTIC_PARENT,
            "source_commit": SOURCE, "source_tree": SOURCE_TREE,
            "source_parent": BASE, "product_blob": PRODUCT_FIXED, "test_blob": TEST_BLOB}


def read_reports(report_root):
    suites, cases = set(), {}
    files = sorted(report_root.rglob("TEST-*.xml"))
    require(files, "missing fresh XML")
    for path in files:
        root = ET.parse(path).getroot()
        require(root.tag == "testsuite", "unexpected XML root")
        suite = root.attrib["name"]
        require(suite not in suites, "duplicate suite report")
        suites.add(suite)
        for case in root.findall("testcase"):
            name = case.attrib["name"]
            key = suite, name
            require(key not in cases, "duplicate testcase")
            outcomes = [node for node in case if node.tag in ("failure", "error", "skipped")]
            require(len(outcomes) <= 1, "ambiguous testcase")
            node = outcomes[0] if outcomes else None
            outcome = node.tag if node is not None else "passed"
            detail = "" if node is None else node.attrib.get("message", "") + "\n" + "".join(node.itertext())
            cases[key] = (outcome, "" if node is None else node.attrib.get("type", ""), detail)
    return suites, cases


def compilation_diagnostics(clean_log):
    marker = r"(?mi)^\[error\].*(?:compilation failed|CompileFailed|\b\d+ errors? found\b)"
    if not re.search(marker, clean_log):
        return None
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    tracked = set(git("ls-files", "--", "*.scala").decode().splitlines())
    result, seen = [], set()
    for match in re.finditer(r"(?m)^\[error\]\s+(.+?\.scala):(\d+):(?:(\d+):)?\s*(.*)$", clean_log):
        path = Path(match[1])
        try:
            relative = (path if path.is_absolute() else workspace / path).resolve().relative_to(workspace).as_posix()
        except ValueError:
            continue
        if relative not in tracked:
            continue
        message = match[4].lower()
        category = next((category for category, pattern in (
            ("implicit", r"implicit"), ("namespace", r"not a member of"),
            ("symbol", r"not found"), ("type", r"type mismatch|does not conform|type arguments"),
        ) if re.search(pattern, message)), "compilation")
        item = (relative, int(match[2]), int(match[3]) if match[3] else None, category)
        if item not in seen:
            seen.add(item)
            result.append(dict(zip(("file", "line", "column", "category"), item)))
            if len(result) == 8:
                break
    return result


def observe(scala, arm, task, output, rows):
    product_blob = git("hash-object", PRODUCT).decode().strip()
    test_blob = git("hash-object", TEST).decode().strip()
    require(product_blob == (PRODUCT_BASE if arm == "baseline" else PRODUCT_FIXED), "arm product mismatch")
    require(test_blob == TEST_BLOB, "arm test mismatch")
    folder = output / arm
    folder.mkdir()
    # Moving every previous report directory makes a missing new report fail closed.
    for number, report_dir in enumerate(REPORT_DIRS):
        if report_dir.exists():
            previous = folder / "previous" / str(number)
            previous.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(report_dir), str(previous))
    command = ["sbt", "-batch"]
    if arm == "blocks-route":
        command.append("-Denv=test")
    command += ["++" + scala, task]
    log_path = folder / "sbt.log"
    with log_path.open("wb") as log:
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=1800, check=False)
    row = {"arm": arm, "command": command, "exit": completed.returncode,
           "product_blob": product_blob, "test_blob": test_blob}
    rows.append(row)
    reports = folder / "reports"
    reports.mkdir()
    for number, report_dir in enumerate(REPORT_DIRS):
        if report_dir.exists():
            shutil.move(str(report_dir), str(reports / str(number)))
    clean_log = re.sub(r"\x1b\[[0-9;]*m", "", log_path.read_text(errors="replace"))
    diagnostics = compilation_diagnostics(clean_log)
    if diagnostics is not None:
        row.update({"compilation_detected": True, "compile_diagnostics": diagnostics})
    totals = re.findall(r"Tests: succeeded (\d+), failed (\d+), canceled (\d+), ignored (\d+), pending (\d+)", clean_log)
    suite_totals = re.findall(r"Suites: completed (\d+), aborted (\d+)", clean_log)
    require(totals and suite_totals, "missing ScalaTest summary")
    counts = tuple(map(int, totals[-1]))
    suite_counts = tuple(map(int, suite_totals[-1]))
    row.update({"scalatest_counts": dict(zip(("passed", "failed", "canceled", "ignored", "pending"), counts)),
                "suites_completed": suite_counts[0], "suites_aborted": suite_counts[1]})
    suites, cases = read_reports(reports)
    xml_counts = {kind: sum(value[0] == kind for value in cases.values())
                  for kind in ("passed", "failure", "error", "skipped")}
    row.update({"xml_counts": xml_counts, "suite_names": sorted(suites)})
    require(suite_counts[1] == 0 and suite_counts[0] == len(suites), "suite count mismatch")
    require(xml_counts["passed"] == counts[0] and xml_counts["failure"] == counts[1] and
            xml_counts["error"] == 0, "XML and summary mismatch")
    row["selected_cases"] = [{"suite": suite, "name": name, "outcome": outcome[0]}
                             for (suite, name), outcome in sorted(cases.items())
                             if suite in (HEADER_SUITE, ROUTE_SUITE)]
    if arm == "baseline":
        require(completed.returncode == 1 and counts == (1, 2, 0, 0, 0) and
                xml_counts["skipped"] == 0 and suites == {HEADER_SUITE}, "baseline counts or exit mismatch")
        require({name for suite, name in cases} == HEADER_NAMES, "baseline case set mismatch")
        require(cases[HEADER_SUITE, STANDALONE][0] == "passed", "baseline control failed")
        row["assertions"] = []
        for name, delta in ((WRAPPED, 1), (PREFIXED, 7)):
            outcome, failure_type, detail = cases[HEADER_SUITE, name]
            require(outcome == "failure", "baseline rejection was not an assertion failure")
            require(re.search(r"(?:TestFailedException|PropertyCheckFailedException)", failure_type + detail),
                    "baseline assertion exception absent")
            require("HeaderSizeSerializationSpec.scala:14" in detail, "baseline assertion site mismatch")
            pairs = set(re.findall(r"Some\((\d+)\)\s+(?:was not equal to|did not equal)\s+Some\((\d+)\)", detail))
            require(len(pairs) == 1, "baseline size comparison missing or ambiguous")
            actual, expected = map(int, next(iter(pairs)))
            require(actual - expected == delta, "baseline size delta mismatch")
            row["assertions"].append({"name": name, "site": "HeaderSizeSerializationSpec.scala:14",
                                      "size_delta": delta, "failure_type": failure_type})
    elif arm == "fixed-core":
        require(completed.returncode == 0 and counts == (113, 0, 0, 2, 0) and len(suites) == 22,
                "fixed core counts or exit mismatch")
        require(xml_counts["skipped"] in (0, 2), "unexpected core skipped count")
        require({name for suite, name in cases if suite == HEADER_SUITE} == HEADER_NAMES,
                "fixed header case set mismatch")
        require(all(cases[HEADER_SUITE, name][0] == "passed" for name in HEADER_NAMES), "fixed header case failed")
    else:
        require(completed.returncode == 0 and counts == (11, 0, 0, 0, 0) and
                xml_counts["skipped"] == 0 and suites == {ROUTE_SUITE}, "route counts or exit mismatch")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-source", action="store_true")
    parser.add_argument("--scala", choices=("2.12.20", "2.13.18"))
    args = parser.parse_args()
    pins = verify_source()
    if args.check_source:
        require(args.scala is None, "ambiguous mode")
        print("Exact source and two-file diagnostic child verified.")
        return 0
    require(args.scala is not None, "missing Scala configuration")
    output = Path(os.environ["FACTS_DIR"])
    require(output.is_absolute() and output.is_relative_to(Path(os.environ["RUNNER_TEMP"])), "output outside runner temp")
    output.mkdir(exist_ok=False)
    facts = {"schema": "header-size-v610-ci-v1", **pins, "scala": args.scala,
             "run_id": os.environ["GITHUB_RUN_ID"], "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
             "valid": False, "arms": [], "source_restored": False}
    facts_path = output / "facts.json"
    facts_path.write_text(json.dumps(facts, indent=2) + "\n")
    try:
        if args.scala == "2.12.20":
            Path(PRODUCT).write_bytes(git("show", BASE + ":" + PRODUCT))
            require(git("hash-object", PRODUCT).decode().strip() == PRODUCT_BASE, "baseline restore mismatch")
            require(git("hash-object", TEST).decode().strip() == TEST_BLOB, "baseline test drift")
            observe(args.scala, "baseline", "ergoCore/testOnly " + HEADER_SUITE, output, facts["arms"])
        Path(PRODUCT).write_bytes(git("show", SOURCE + ":" + PRODUCT))
        observe(args.scala, "fixed-core", "ergoCore/test", output, facts["arms"])
        if args.scala == "2.12.20":
            observe(args.scala, "blocks-route", "testOnly " + ROUTE_SUITE, output, facts["arms"])
        facts["valid"] = True
    except Exception as error:
        facts["failure_category"] = type(error).__name__
        if isinstance(error, CheckFailure):
            facts["failure_reason"] = str(error)
        # Deliberately retain neither raw exception text nor test values.
    finally:
        try:
            Path(PRODUCT).write_bytes(git("show", SOURCE + ":" + PRODUCT))
            verify_source()
            git("diff", "--check")
            facts["source_restored"] = True
        except Exception as error:
            facts["valid"] = False
            facts["restoration_failure_category"] = type(error).__name__
        facts_path.write_text(json.dumps(facts, indent=2) + "\n")
    print("Sanitized header-size facts: " + ("valid" if facts["valid"] else "INVALID"))
    return 0 if facts["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())

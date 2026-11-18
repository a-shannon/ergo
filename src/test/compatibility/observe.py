"""Read-only observations of the stock devnet rollback test on a hosted runner."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener


def get(port, path):
    with build_opener(ProxyHandler({})).open(f"http://127.0.0.1:{port}{path}", timeout=8) as response:
        return json.load(response)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def info(port):
    value = get(port, "/info")
    return {key: value[key] for key in ("appVersion", "fullHeight", "bestFullHeaderId",
            "fullBlocksScore", "stateVersion", "stateRoot", "genesisBlockId")}


def block(port, ident):
    if not re.fullmatch("[0-9a-f]{64}", ident):
        raise ValueError("invalid identifier in test observation")
    try:
        value = get(port, "/blocks/" + ident)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise
    if value["header"]["id"] != ident or "transactions" not in value["blockTransactions"]:
        raise ValueError("incomplete full-block response")
    return value


def path_to_genesis(port, ident):
    path = {}
    while ident != "00" * 32:
        if ident in path or len(path) >= 4096:
            raise ValueError("unexpected test chain")
        value = block(port, ident)
        if value is None:
            raise ValueError("missing retained full block on observing node")
        header = value["header"]
        path[ident] = [header["height"], int(header["difficulty"]), digest(value)]
        ident = header["parentId"]
    heights = [record[0] for record in path.values()]
    if heights != list(range(heights[0], 0, -1)):
        raise ValueError("nonconsecutive archived heights")
    return path


def snapshot(ports, containers):
    release_first = os.environ["COMPAT_RELEASE_PEER"] == "node01"
    candidate_sha = subprocess.check_output(
        ["docker", "run", "--rm", "--entrypoint", "sha256sum",
         "org.ergoplatform/ergo", "/opt/ergo/ergo.jar"], text=True).split()[0]
    if candidate_sha == os.environ["RELEASE_JAR_SHA"]:
        raise ValueError("both nodes use the released binary")
    hashes = (os.environ["RELEASE_JAR_SHA"], candidate_sha)
    expected = hashes if release_first else hashes[::-1]
    for container, sha in zip(containers, expected):
        if not re.fullmatch("[0-9a-f]{12,64}", container):
            raise ValueError("invalid local container id")
        actual = subprocess.check_output(
            ["docker", "exec", container, "sha256sum", "/opt/ergo/ergo.jar"], text=True).split()[0]
        if actual != sha:
            raise ValueError("running binary does not match assigned version")
    return [info(port) for port in ports], candidate_sha


def deliver(port, value):
    payload = json.dumps(value).encode()
    request = Request(
        f"http://127.0.0.1:{port}/blocks",
        data=payload,
        headers={"Content-Type": "application/json", "api_key": "hello"},
        method="POST",
    )
    with build_opener(ProxyHandler({})).open(request, timeout=8) as response:
        if response.status != 200:
            raise ValueError("block delivery rejected")


if __name__ == "__main__":
    phase, pa, pb, ca, cb = sys.argv[1:]
    ports = tuple(map(int, (pa, pb)))
    evidence = Path(os.environ["COMPAT_RECEIPTS"])
    isolated_file = evidence / "isolated.json"
    tips, candidate_sha = snapshot(ports, (ca, cb))
    if tips[0]["genesisBlockId"] != tips[1]["genesisBlockId"]:
        raise ValueError("different genesis")
    if phase == "isolated":
        if tips[0]["bestFullHeaderId"] == tips[1]["bestFullHeaderId"] or int(tips[0]["fullBlocksScore"]) <= int(tips[1]["fullBlocksScore"]):
            raise ValueError("distinct full tips with a strict work lead required")
        with ThreadPoolExecutor(2) as pool:
            a, losing = [f.result() for f in (
                pool.submit(path_to_genesis, ports[0], tips[0]["bestFullHeaderId"]),
                pool.submit(path_to_genesis, ports[1], tips[1]["bestFullHeaderId"]))]
        common = next((ident for ident in losing if ident in a), None)
        if common is None or common == tips[1]["bestFullHeaderId"]:
            raise ValueError("distinct complete branches and shared ancestor required")
        if sum(item[1] for item in a.values()) != int(tips[0]["fullBlocksScore"]):
            raise ValueError("winner work does not match its full chain")
        if sum(item[1] for item in losing.values()) != int(tips[1]["fullBlocksScore"]):
            raise ValueError("loser work does not match its full chain")
        suffix = [ident for ident in losing if ident not in a]
        for ident in reversed(suffix):
            value = block(ports[1], ident)
            deliver(ports[0], value)
            for attempt in range(25):
                stored = block(ports[0], ident)
                if stored is not None:
                    if digest(stored) != losing[ident][2]:
                        raise ValueError("relayed block differs from original")
                    break
                time.sleep(0.4)
            else:
                raise ValueError("receiving node did not store a full side-chain block")
        after_relay = [info(port) for port in ports]
        if after_relay != tips:
            raise ValueError("branch selection changed during the isolated relay")
        isolated_file.write_text(json.dumps(dict(tips=tips, winner=a, losing=losing,
                                                 candidate_sha=candidate_sha)))
        print("MIXED_ISOLATED", json.dumps(dict(tips=tips,
              winner_blocks=len(a), losing_blocks=len(losing), relayed=len(suffix))), flush=True)
    elif phase == "reconnected":
        before = json.loads(isolated_file.read_text())
        initial = before["tips"]
        if candidate_sha != before["candidate_sha"]:
            raise ValueError("candidate binary changed between observation phases")
        for key in ("fullHeight", "bestFullHeaderId", "fullBlocksScore", "stateVersion", "stateRoot"):
            if not tips[0][key] or tips[0][key] != tips[1][key]:
                raise ValueError("final observation disagreement: " + key)
        winner = tips[0]["bestFullHeaderId"]
        if winner != initial[0]["bestFullHeaderId"]:
            raise ValueError("winning branch tip changed while mining was disabled")
        if tips[0]["stateVersion"] != winner:
            raise ValueError("state has not applied selected full tip")
        with ThreadPoolExecutor(3) as pool:
            a, b, b_losing = [f.result() for f in (
                pool.submit(path_to_genesis, ports[0], winner),
                pool.submit(path_to_genesis, ports[1], winner),
                pool.submit(path_to_genesis, ports[1], initial[1]["bestFullHeaderId"]))]
        losing = before["losing"]
        if a != b or a != before["winner"] or b_losing != losing:
            raise ValueError("winning chain contents or ancestry differ")
        common = next(h for h in losing if h in a)
        losers = [h for h in losing if h not in a]
        if not losers or common == winner:
            raise ValueError("no observed branch switch")
        for chain, score in ((a, tips[0]["fullBlocksScore"]), (losing, initial[1]["fullBlocksScore"])):
            if sum(item[1] for item in chain.values()) != int(score):
                raise ValueError("chain work differs from node score")
        for ident, record in (a | losing).items():
            for port in ports:
                value = block(port, ident)
                if value is None or digest(value) != record[2]:
                    raise ValueError("both peers must retain the same complete blocks")
        for _ in range(3):
            time.sleep(3)
            if [info(port) for port in ports] != tips:
                raise ValueError("observed full tip or state changed after convergence")
        result = dict(before=initial, after=tips, winner_chain_blocks=len(a),
                      common_ancestor_height=a[common][0], losing_suffix_blocks=len(losers),
                      equal_full_union=True,
                      winning_corpus_sha256=digest(a),
                      jar_sha256=[os.environ["RELEASE_JAR_SHA"], candidate_sha])
        (evidence / "result.json").write_text(json.dumps(result, indent=2))
        print("MIXED_RESULT", json.dumps(result), flush=True)
    else:
        raise ValueError("unknown observation phase")

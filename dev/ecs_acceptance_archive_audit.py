"""Compare downloaded synthetic batches with independent final producer counts.

This checks hashes/counts/chains, not a substitute for Monitor signature,
lease, transaction and replay acceptance. No production data or network.
"""
import argparse
import base64
from collections import defaultdict
import hashlib
import json
from ecs_acceptance_run import load, write


def audit(root):
    tasks = json.loads((root / "tasks.json").read_text())
    oracle = {}
    for arn in tasks:
        task = arn.rsplit("/", 1)[-1]
        events = json.loads((root / ("logs-test-synthetic-" + task + ".json")).read_text())
        final = [json.loads(e["message"].removeprefix("SYNTHETIC_ORACLE ")) for e in events if e["message"].startswith("SYNTHETIC_ORACLE ")]
        final = [v for v in final if v["final"]]
        if len(final) != 1:
            raise ValueError("missing or multiple final producer oracles")
        oracle[task] = final[0]["count"]
    groups = defaultdict(dict)
    ids = {}
    for path in (root / "archive").rglob("*.json"):
        envelope = json.loads(path.read_bytes())
        body = base64.b64decode(envelope["body"], validate=True)
        digest = hashlib.sha256(body).hexdigest()
        if digest != envelope["body_sha256"] or path.stem != digest:
            raise ValueError("archive body hash mismatch")
        task = path.parts[-4].split(":", 1)[1]
        payload = json.loads(body)
        identity = (envelope["node"], envelope["lane"], payload["batch_id"])
        if identity in ids and ids[identity] != digest:
            raise ValueError("same batch ID has different content")
        ids[identity] = digest
        groups[(task, envelope["lane"])][digest] = (envelope["previous_body_sha256"], payload)
    result = []
    for (task, lane), batches in sorted(groups.items()):
        roots = [h for h, (prev, _) in batches.items() if not prev]
        missing = [prev for prev, _ in batches.values() if prev and prev not in batches]
        children = defaultdict(list)
        for digest, (previous, _) in batches.items():
            children[previous].append(digest)
        reached, pending = set(), list(roots)
        while pending:
            digest = pending.pop()
            if digest in reached:
                raise ValueError("archive chain contains a cycle")
            reached.add(digest)
            pending.extend(children[digest])
        if len(roots) != 1 or missing or len(reached) != len(batches) or any(len(v) > 1 for v in children.values()):
            raise ValueError("archive chain missing, branched, or disconnected")
        n = sum(len(p.get("events") or []) if lane == "evidence" else sum(s["count"] for s in (p.get("samples") or [])) for _, p in batches.values())
        result.append({"task": task, "lane": lane, "batches": len(batches), "chain_roots": len(roots), "missing_predecessors": len(missing), "archived_records": n, "producer_records": oracle[task], "counts_match": n == oracle[task]})
    if len(result) != len(oracle) * 4:
        raise ValueError("missing task/lane")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    root, _ = load(args.run_dir)
    result = audit(root)
    write(root / "archive-audit.json", result)
    print(json.dumps(result))

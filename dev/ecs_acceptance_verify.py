"""Fail-closed comparison of actual cloud producer, archive, and receiver data."""
import argparse
import datetime as dt
import json
from ecs_acceptance_archive_audit import audit
from ecs_acceptance_report import report
from ecs_acceptance_run import load, write


def final_boundary_mismatches(report_data):
    """Every discovered lane needs exactly one accepted, verified final proof.

    Never let matching request counts alone approve the new boundary protocol.
    Old V1 runs remain useful compatibility fixtures, not new-protocol passes.
    """
    expected = {(s["node"], s["lane"]) for s in report_data["sources"]}
    proofs = {}
    for proof in report_data.get("final_boundaries", []):
        key = (proof["node"], proof["lane"])
        proofs.setdefault(key, []).append(proof)
    failures = []
    for key in sorted(expected | set(proofs)):
        rows = proofs.get(key, [])
        if key not in expected or len(rows) != 1 or rows[0]["status"] != 200 or rows[0]["final_boundary_verified"] != 1:
            failures.append({"node": key[0], "lane": key[1], "reason": "final_boundary_unverified"})
    if not expected:
        failures.append({"reason": "no_sources"})
    return failures


def verify(root):
    archived = audit(root)
    r = report(root)
    tasks = json.loads((root / "tasks.json").read_text())
    objects = json.loads((root / "archive-objects.json").read_text())["Contents"]
    nodes = {(s["task_arn"].rsplit("/", 1)[-1], s["lane"]): s["node"] for s in r["sources"]}
    counts = {lane: {x["node"]: x["n"] for x in r[lane]} for lane in ["access", "error", "reject", "evidence"]}
    mismatches = []
    for a in archived:
        node = nodes.get((a["task"], a["lane"]))
        observed = counts[a["lane"]].get(node, 0)
        if not a["counts_match"] or observed != a["producer_records"]:
            mismatches.append({"task": a["task"], "lane": a["lane"], "expected": a["producer_records"], "observed": observed})
    intervals = []
    for t in tasks.values():
        if t["lastStatus"] != "STOPPED" or not t.get("stoppedAt"):
            raise RuntimeError("test tasks not fully stopped")
        if any(c.get("exitCode") != 0 for c in t["containers"]):
            raise RuntimeError("test container did not exit cleanly")
        intervals += [(dt.datetime.fromisoformat(t["createdAt"]), 1), (dt.datetime.fromisoformat(t["stoppedAt"]), -1)]
    active, maximum = 0, 0
    for _, delta in sorted(intervals):
        active += delta
        maximum = max(active, maximum)
    if maximum > 2:
        raise RuntimeError("test concurrency exceeded two")
    accepted = sum(x["n"] for x in r["receipts"] if x["status"] == 200)
    pending = [x for x in r["receipts"] if x["status"] != 200]
    boundary_mismatches = final_boundary_mismatches(r)
    ready = not mismatches and not boundary_mismatches and not pending and accepted == len(objects)
    summary = {"complete": ready, "objects": len(objects), "accepted": accepted,
               "pending": pending, "mismatches": mismatches, "final_boundary_mismatches": boundary_mismatches, "max_tasks_including_start_stop": maximum,
               "task_count_over_lifetime": len(tasks), "source_count": len(r["sources"]),
               "at": dt.datetime.now(dt.timezone.utc).isoformat()}
    if ready:
        if any(s["stopped_at"] == 0 or s["stop_code"] != "ServiceSchedulerInitiated" for s in r["sources"]):
            raise RuntimeError("stopped source lifecycle not discovered")
        if any(s["last_error"] for s in r["archive_scan"]):
            raise RuntimeError("archive discovery currently failing")
    return summary, r


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--save", choices=["final", "restart"])
    args = parser.parse_args()
    root, _ = load(args.run_dir)
    summary, r = verify(root)
    print(json.dumps(summary))
    if args.save and summary["complete"]:
        write(root / ("verify-" + args.save + ".json"), summary)
        write(root / ("report-" + args.save + ".json"), r)
    if not summary["complete"]:
        raise SystemExit(1)

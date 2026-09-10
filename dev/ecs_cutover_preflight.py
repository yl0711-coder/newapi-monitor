"""Offline, read-only task-responsibility transition check; NEVER a deploy tool.

Inputs are review manifests, not AWS attestations. Passing cannot authorize an
ingest source or establish that the captured task inventory is current/complete.
Only whole NEW task lifetimes may change collector generation. Byte-level
handoff of an existing task is intentionally unsupported and fails closed.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

MAX_BYTES = 4 * 1024 * 1024
MAX_ROWS = 10000
LANES = {"access": "nginx", "error": "nginx", "evidence": "nginx", "reject": "new-api"}
ROW_FIELDS = {"task_arn", "service_arn", "container", "runtime_id", "lane", "collector", "audience"}
TASK_RE = re.compile(r"arn:aws:ecs:([a-z0-9-]+):(\d{12}):task/([A-Za-z0-9_-]+)/([a-f0-9]{32})")
SERVICE_RE = re.compile(r"arn:aws:ecs:([a-z0-9-]+):(\d{12}):service/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)")
AUDIENCE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class InvalidPlan(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidPlan(message)


def exact_fields(value, fields, location):
    require(type(value) is dict and set(value) == fields, f"{location}: invalid or missing fields")


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def load_manifest(path):
    # Bounded read, no subprocess, remote URL, credential access, or output file.
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    require(len(raw) <= MAX_BYTES, "manifest exceeds size limit")
    try:
        return json.loads(raw, object_pairs_hook=no_duplicate_keys)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as exc:
        raise InvalidPlan("invalid manifest JSON") from exc


def indexed_assignments(rows, location):
    require(type(rows) is list and 0 < len(rows) <= MAX_ROWS, f"{location}: empty or oversized responsibility list")
    indexed, tasks = {}, {}
    for row in rows:
        exact_fields(row, ROW_FIELDS, location)
        require(all(type(value) is str for value in row.values()), f"{location}: fields must be strings")
        task, service = TASK_RE.fullmatch(row["task_arn"]), SERVICE_RE.fullmatch(row["service_arn"])
        require(task is not None and service is not None, f"{location}: invalid task/service ARN")
        require(task.groups()[:3] == service.groups()[:3], f"{location}: task outside service account/region/cluster")
        require(row["lane"] in LANES and row["container"] == LANES[row["lane"]], f"{location}: unexpected producer/lane mapping")
        require(row["collector"] in {"legacy", "ecs"}, f"{location}: candidate/shadow is not a formal responsibility")
        require(AUDIENCE_RE.fullmatch(row["audience"]) is not None, f"{location}: invalid receiver audience")
        runtime = row["runtime_id"]
        require(len(runtime) <= 256 and (not runtime or re.fullmatch(r"[A-Za-z0-9._:-]+", runtime)), f"{location}: invalid runtime")
        require(row["collector"] == "legacy" or bool(runtime), f"{location}: ECS runtime evidence required")
        key = (row["task_arn"], row["container"], row["lane"])
        require(key not in indexed, f"{location}: duplicate task/container/lane")
        indexed[key] = row
        tasks.setdefault(row["task_arn"], []).append(row)
    for group in tasks.values():
        require({r["lane"] for r in group} == set(LANES), f"{location}: incomplete task lane responsibilities")
        require(len({(r["service_arn"], r["collector"], r["audience"]) for r in group}) == 1, f"{location}: mixed owners inside one task")
        for container in set(LANES.values()):
            require(len({r["runtime_id"] for r in group if r["container"] == container}) == 1, f"{location}: mixed runtime for one producer")
    return indexed, tasks


def manifest_digest(manifest):
    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def check_transition(baseline, proposal):
    exact_fields(baseline, {"version", "assignments"}, "baseline")
    exact_fields(proposal, {"version", "action", "baseline_sha256", "assignments"}, "proposal")
    require(type(baseline["version"]) is int and baseline["version"] == 1, "unsupported baseline version")
    require(type(proposal["version"]) is int and proposal["version"] == 1, "unsupported proposal version")
    require(type(proposal["action"]) is str and proposal["action"] in {"advance", "rollback"}, "unsupported transition action")
    require(proposal["baseline_sha256"] == manifest_digest(baseline), "baseline changed; review the complete plan again")
    before, before_tasks = indexed_assignments(baseline["assignments"], "baseline")
    after, after_tasks = indexed_assignments(proposal["assignments"], "proposal")
    for key, row in before.items():
        require(after.get(key) == row, "existing task responsibility removed or changed; preserve owner and replay capability")
    new_tasks = set(after_tasks) - set(before_tasks)
    require(bool(new_tasks), "no new task lifetime in plan; this is not a responsibility transition")
    target = "ecs" if proposal["action"] == "advance" else "legacy"
    existing_services = {r["service_arn"] for r in before.values()}
    existing_audiences = {(r["collector"], r["audience"]) for r in before.values()}
    service_receivers = {(r["service_arn"], r["collector"], r["audience"]) for r in before.values()}
    for task in new_tasks:
        for row in after_tasks[task]:
            require(row["collector"] == target, "wrong generation for new tasks in this transition")
            require(row["service_arn"] in existing_services, "plan expands to an unreviewed service")
            if target == "ecs":
                require(("legacy", row["audience"]) not in existing_audiences, "new receiver must not reuse legacy audience")
            else:
                # Rolling back future tasks cannot invent a legacy receiver or
                # change an already accepted ECS task back to a file-head reader.
                require((row["service_arn"], "legacy", row["audience"]) in service_receivers, "rollback receiver not present for this service in baseline")
    new_receivers = {r["audience"] for task in new_tasks for r in after_tasks[task]}
    require(len(new_receivers) == 1, "one reviewed target receiver required per transition")
    return {
        "static_checks_passed": True,
        "production_ready": False,
        "authorization_verified": False,
        "action": proposal["action"],
        "baseline_sha256": manifest_digest(baseline),
        "proposal_sha256": manifest_digest(proposal),
        "retained_tasks": len(before_tasks),
        "new_tasks": len(new_tasks),
        "retained_lanes": len(before),
        "new_lanes": len(after) - len(before),
        "remaining_gates": ["authoritative_current_task_inventory", "deployment_and_scan_evidence", "explicit_release_approval"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="local reviewed baseline JSON")
    parser.add_argument("--proposal", required=True, help="local proposed responsibility JSON")
    args = parser.parse_args(argv)
    try:
        result = check_transition(load_manifest(args.baseline), load_manifest(args.proposal))
    except (InvalidPlan, OSError) as exc:
        # Do not echo source manifests, environment, credentials, or OS paths.
        message = str(exc) if isinstance(exc, InvalidPlan) else "cannot read local manifest"
        print(json.dumps({"static_checks_passed": False, "production_ready": False, "error": message}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

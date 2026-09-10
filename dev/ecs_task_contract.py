"""Offline checks for the reviewed two-producer ECS collector task contract.

Accepts CloudFormation TaskDefinition Properties (not an AWS response). This is
not an IAM evaluator, deployment tool, or proof of current production state.
"""
import hashlib
import json
import re
from pathlib import PurePosixPath

PAIRS = {"nginxcollector": "nginx", "reject-collector": "new-api"}
INIT = "nginxcollector-init"
MIN_STOP_SECONDS = 60  # Includes the agent's bounded archive/finalization budget.
MAX_STOP_SECONDS = 120


def require(condition, message):
    if not condition:
        raise ValueError(message)


def indexed(rows, key, label):
    require(isinstance(rows, list), f"{label}: list required")
    result = {}
    for row in rows:
        require(isinstance(row, dict), f"{label}: object required")
        name = row.get(key)
        require(isinstance(name, str) and bool(name), f"{label}: name required")
        require(name not in result, f"{label}: duplicate name")
        result[name] = row
    return result


def dependencies(containers):
    graph = {}
    for name, container in containers.items():
        deps = indexed(container.get("DependsOn", []), "ContainerName", "dependencies")
        for parent, edge in deps.items():
            require(parent in containers and parent != name, "unknown/self dependency")
            condition = edge.get("Condition")
            require(condition in ("START", "HEALTHY", "SUCCESS", "COMPLETE"), "invalid dependency condition")
            if condition in ("SUCCESS", "COMPLETE"):
                require(containers[parent].get("Essential") is False, "completion dependency must be nonessential")
            if condition == "HEALTHY":
                require(bool(containers[parent].get("HealthCheck")), "HEALTHY dependency lacks health check")
        graph[name] = deps
    pending = set(graph)
    while pending:
        ready = {name for name in pending if not (set(graph[name]) & pending)}
        require(bool(ready), "dependency cycle")
        pending -= ready
    return graph


def mounts(container, volumes):
    result = indexed(container.get("MountPoints", []), "ContainerPath", "mounts")
    for path, mount in result.items():
        parsed = PurePosixPath(path)
        require(parsed.is_absolute() and str(parsed) == path and ".." not in parsed.parts,
                "noncanonical mount path")
        require(mount.get("SourceVolume") in volumes, "undefined volume")
    paths = list(result)
    for i, path in enumerate(paths):
        require(not any(PurePosixPath(path) in PurePosixPath(other).parents or
                        PurePosixPath(other) in PurePosixPath(path).parents
                        for other in paths[i + 1:]), "overlapping mounts")
    require(not container.get("VolumesFrom"), "inherited mounts are not supported")
    return result


def check_collector(name, producer, containers, graph, all_mounts):
    collector = containers[name]
    require(collector.get("Essential") is False, "collector must be nonessential")
    require(collector.get("ReadonlyRootFilesystem") is True, "collector root must be read-only")
    require(collector.get("User") == "100:101", "reviewed nonroot collector user required")
    require(not collector.get("Privileged"), "privileged collector forbidden")
    require(collector.get("LinuxParameters") == {"Capabilities": {"Drop": ["ALL"]}},
            "collector capabilities must be dropped; no extra Linux privileges")
    timeout = collector.get("StopTimeout")
    require(type(timeout) is int and MIN_STOP_SECONDS <= timeout <= MAX_STOP_SECONDS,
            "collector stop budget must be 60..120 seconds")
    require(graph[name] == {INIT: {"ContainerName": INIT, "Condition": "SUCCESS"}},
            "collector must depend only on successful state initialization")
    require(graph[producer].get(name, {}).get("Condition") == "START",
            "producer must start after collector, so it stops first")
    env = {k: row.get("Value") for k, row in indexed(collector.get("Environment", []), "Name", "environment").items()}
    expected = {"ECSLOG_PRODUCER_CONTAINER": producer,
                "ECSLOG_FINAL_FILE_CONTRACT": "newapi-files-v1",
                "ECSLOG_STATE_ROOT": "/data/ecs", "ECSLOG_ARCHIVE_ENABLED": "true",
                "ECSLOG_ARCHIVE_CLOSURE": "true", "ECSLOG_DEFERRED_ARCHIVE_ACK": "true",
                "ECSLOG_SCOPE": "isolated"}
    log_path = "/logs" if producer == "nginx" else "/app/logs"
    expected["ECSLOG_FINAL_LOG_ROOT"] = log_path
    if producer == "nginx":
        expected.update(ECSLOG_KIND="nginx", NGINXCOLLECTOR_LOG_PATH="/logs/nexusapi_access.jsonl",
                        NGINXCOLLECTOR_ERROR_LOG_PATH="/logs/error.log")
    else:
        expected.update(ECSLOG_KIND="reject", COLLECTOR_LOG_GLOB="/app/logs/oneapi-*.log")
    require(all(env.get(k) == v for k, v in expected.items()), "collector file/producer/archive contract mismatch")
    own = all_mounts[name]
    require(set(own) == {log_path, "/data/ecs"}, "collector requires only log and private state mounts")
    require(own[log_path].get("ReadOnly") is True, "collector log mount must be read-only")
    source = all_mounts[producer].get(log_path, {})
    require(source.get("SourceVolume") == own[log_path]["SourceVolume"] and
            source.get("ReadOnly", False) is False, "producer and collector must share the actual log volume")
    log_owners = {c for c, points in all_mounts.items()
                  if any(m["SourceVolume"] == source["SourceVolume"] for m in points.values())}
    require(log_owners <= {name, producer, INIT}, "producer log volumes must be isolated")
    state = own["/data/ecs"]
    require(state.get("ReadOnly", False) is False, "collector state must be writable")
    volume = state["SourceVolume"]
    owners = {c for c, points in all_mounts.items() if any(m["SourceVolume"] == volume for m in points.values())}
    require(owners == {name, INIT}, "state volume must be private to collector and init")
    require(any(m["SourceVolume"] == volume and m.get("ReadOnly", False) is False
                for m in all_mounts[INIT].values()), "init must initialize writable state volume")
    return volume


def check_task(task):
    require(isinstance(task, dict), "task object required")
    require(task.get("NetworkMode") == "awsvpc" and task.get("RequiresCompatibilities") == ["FARGATE"],
            "Fargate awsvpc task required")
    containers = indexed(task.get("ContainerDefinitions"), "Name", "containers")
    require(set(containers) == set(PAIRS) | set(PAIRS.values()) | {INIT}, "reviewed five-container layout required")
    require(containers[INIT].get("Essential") is False, "init must be nonessential")
    volumes = indexed(task.get("Volumes", []), "Name", "volumes")
    require(all(set(v) == {"Name"} for v in volumes.values()), "only task-local volumes are supported")
    graph = dependencies(containers)
    all_mounts = {n: mounts(c, volumes) for n, c in containers.items()}
    states = [check_collector(n, p, containers, graph, all_mounts) for n, p in PAIRS.items()]
    require(len(set(states)) == len(states), "collectors must not share state")
    return {"static_checks_passed": True, "production_ready": False,
            "aws_verified": False, "checked_collectors": sorted(PAIRS)}


def initializer_digest(container):
    """Fingerprint of the entire separately reviewed init definition, not approval."""
    raw = json.dumps(container, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def check_initializer_transition(old, new, reviewed_digest):
    require(set(old) in (set(new), set(new) - {INIT}),
            "only the state initializer may be added; cannot remove or rename existing containers")
    changed = old.get(INIT) != new[INIT]
    if changed or reviewed_digest is not None:
        require(isinstance(reviewed_digest, str) and re.fullmatch(r"[a-f0-9]{64}", reviewed_digest) is not None,
                "initializer addition/change requires a separate reviewed SHA256 fingerprint")
        require(reviewed_digest == initializer_digest(new[INIT]), "initializer differs from reviewed fingerprint")
    return changed


def check_business_unchanged(before, after, *, reviewed_initializer_sha256=None):
    """Only producer START dependencies may be added; never edit business config.

    A newly added/changed initializer needs an explicit separate fingerprint;
    a matching fingerprint is not evidence of who reviewed or authorized it.
    Does not authorize collector/role changes. Both documents must be complete
    reviewed definitions; redacted snapshots cannot prove secret equality.
    """
    check_task(after)
    old = indexed(before.get("ContainerDefinitions"), "Name", "baseline containers")
    new = indexed(after.get("ContainerDefinitions"), "Name", "candidate containers")
    initializer_changed = check_initializer_transition(old, new, reviewed_initializer_sha256)
    for name in PAIRS.values():
        strip = lambda c: {k: v for k, v in c.items() if k != "DependsOn"}
        require(strip(old[name]) == strip(new[name]), "business container configuration changed")
        old_deps = indexed(old[name].get("DependsOn", []), "ContainerName", "old dependencies")
        new_deps = indexed(new[name].get("DependsOn", []), "ContainerName", "new dependencies")
        collector = next(c for c, p in PAIRS.items() if p == name)
        expected = {**old_deps, collector: {"ContainerName": collector, "Condition": "START"}}
        require(collector not in old_deps or old_deps[collector] == expected[collector], "existing producer dependency changed")
        require(new_deps == expected, "business dependencies removed or expanded")
    # Task role and private state volumes require a separate, explicit IAM and
    # storage review. All other task properties (including compute) stay equal.
    excluded = {"ContainerDefinitions", "TaskRoleArn", "Volumes"}
    require({k: v for k, v in before.items() if k not in excluded} ==
            {k: v for k, v in after.items() if k not in excluded}, "unreviewed task-level change")
    old_volumes = indexed(before.get("Volumes", []), "Name", "old volumes")
    new_volumes = indexed(after.get("Volumes", []), "Name", "new volumes")
    require(all(new_volumes.get(k) == v for k, v in old_volumes.items()), "existing volume changed/removed")
    state_volumes = {m["SourceVolume"] for name in PAIRS for m in new[name]["MountPoints"]
                     if m["ContainerPath"] == "/data/ecs"}
    require(set(new_volumes) - set(old_volumes) <= state_volumes, "only private collector state volumes may be added")
    return {"business_configuration_unchanged": True, "production_ready": False,
            "iam_review_required": True, "deployment_authorized": False,
            "initializer_changed": initializer_changed,
            "initializer_fingerprint_matched": reviewed_initializer_sha256 is not None}

"""One explicitly abnormal producer exit in the already isolated zero-task service.

Prepare never starts a task. Use the existing guarded scale action for 0->1->0.
The failure is triggered by scale-to-zero, preventing an automatic restart loop.
"""
import argparse
import copy
import json
import re

from ecs_acceptance_run import ACCOUNT, REGION, SERVICE, aws, identity, load, write

# Reuse the already verified synthetic image. A task stop kills ONLY its own
# synthetic child, then explicitly exits nonzero; desiredCount is already zero.
FAULT_COMMAND = '''[ "$ECSLOG_SCOPE" = isolated ] || exit 64
child=
stop_fault() {
    trap '' TERM INT
    if [ -n "$child" ]; then
        kill -KILL "$child" 2>/dev/null
        wait "$child" 2>/dev/null
    fi
    exit 1
}
trap stop_fault TERM INT
/usr/local/bin/synthetic &
child=$!
wait "$child"
exit 1
'''


def definition_for_fault(definition, plan, outputs):
    expected_role = f"arn:aws:iam::{ACCOUNT}:role/{plan['name']}-taskrole"
    repository = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{plan['name']}"
    if definition.get("family") != plan["name"] or definition.get("taskRoleArn") != expected_role or outputs["Repository"] != repository:
        raise RuntimeError("fault fixture must retain the dedicated test family/role")
    if definition.get("cpu") != "512" or definition.get("memory") != "1024" or definition.get("networkMode") != "awsvpc":
        raise RuntimeError("fault fixture outside approved size/network")
    keys = ["family", "taskRoleArn", "executionRoleArn", "networkMode", "containerDefinitions", "volumes",
            "placementConstraints", "requiresCompatibilities", "cpu", "memory", "runtimePlatform", "ephemeralStorage"]
    result = {key: copy.deepcopy(definition[key]) for key in keys if key in definition}
    containers = {c["name"]: c for c in result["containerDefinitions"]}
    if len(result["containerDefinitions"]) != 4 or set(containers) != {"init", "synthetic", "nginxcollector", "rejectcollector"}:
        raise RuntimeError("unexpected test container inventory")
    if any(not c["image"].startswith(repository + ":") for c in containers.values()):
        raise RuntimeError("test image outside dedicated repository")
    producer = containers["synthetic"]
    if [e.get("value") for e in producer.get("environment", []) if e.get("name") == "ECSLOG_SCOPE"] != ["isolated"]:
        raise RuntimeError("synthetic isolated scope required")
    if producer.get("entryPoint") or producer.get("command"):
        raise RuntimeError("fault already configured")
    producer["entryPoint"] = ["/bin/sh", "-c"]
    producer["command"] = [FAULT_COMMAND]
    return result


def zero_service(root, plan):
    service = aws("ecs", "describe-services", {"cluster": plan["name"], "services": [SERVICE]})
    if service.get("failures") or len(service.get("services", [])) != 1:
        raise RuntimeError("test service unavailable")
    current = service["services"][0]
    if any(current[k] for k in ["desiredCount", "runningCount", "pendingCount"]):
        raise RuntimeError("stop normal test completely before fault preparation")
    known = json.loads((root / "tasks.json").read_text())
    running = aws("ecs", "list-tasks", {"cluster": plan["name"], "desiredStatus": "RUNNING"})["taskArns"]
    tasks = aws("ecs", "describe-tasks", {"cluster": plan["name"], "tasks": sorted(set(known) | set(running))})
    if tasks.get("failures") or any(t["lastStatus"] != "STOPPED" for t in tasks["tasks"]):
        raise RuntimeError("some test tasks still active")
    return current


def prepare(root, plan):
    if (root / "fault-definition.json").exists():
        raise RuntimeError("fault preparation already attempted; do not register repeatedly")
    current = zero_service(root, plan)
    outputs = json.loads((root / "outputs.json").read_text())
    original = aws("ecs", "describe-task-definition", {"taskDefinition": current["taskDefinition"]})["taskDefinition"]
    body = definition_for_fault(original, plan, outputs)
    body["tags"] = [{"key": "MonitorAcceptanceRun", "value": plan["name"]}]
    new = aws("ecs", "register-task-definition", body)["taskDefinition"]
    write(root / "fault-definition.json", {"original": current["taskDefinition"], "new": new["taskDefinitionArn"]})
    result = aws("ecs", "update-service", {"cluster": plan["name"], "service": SERVICE, "taskDefinition": new["taskDefinitionArn"],
        "desiredCount": 0, "deploymentConfiguration": {"maximumPercent": 100, "minimumHealthyPercent": 0}})
    write(root / "fault-prepared.json", result)
    print("Isolated fault revision prepared with zero tasks; start through guarded count=1, then stop with count=0.")


def retire(root, plan):
    zero_service(root, plan)
    revision = json.loads((root / "fault-definition.json").read_text())
    prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{plan['name']}:"
    if any(not re.fullmatch(re.escape(prefix) + r"[0-9]+", revision[key]) for key in ["original", "new"]):
        raise RuntimeError("unexpected fault task definition")
    aws("ecs", "update-service", {"cluster": plan["name"], "service": SERVICE, "taskDefinition": revision["original"], "desiredCount": 0})
    result = aws("ecs", "deregister-task-definition", {"taskDefinition": revision["new"]})
    write(root / "fault-retired.json", {"taskDefinition": revision["new"], "status": result["taskDefinition"]["status"]})
    print("Dedicated fault revision deregistered; original test definition restored with zero tasks.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "retire"])
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    root, plan = load(args.run_dir)
    identity()
    {"prepare": prepare, "retire": retire}[args.action](root, plan)

"""Guarded one-task rolling replacement; never scales the service above one.

MaximumPercent=200 allows exactly two tasks only because DesiredCount is one.
The regular scale action refuses this deployment mode until it is restored.
"""
import datetime as dt
import json


def ready_service(root, plan, aws, service, require_one=True):
    result = aws("ecs", "describe-services", {"cluster": plan["name"], "services": [service]})
    if result.get("failures") or len(result.get("services", [])) != 1:
        raise RuntimeError("test service discovery incomplete")
    current = result["services"][0]
    config = current["deploymentConfiguration"]
    if config["maximumPercent"] != 100 or config["minimumHealthyPercent"] != 0:
        raise RuntimeError("restore bounded deployment settings before scaling")
    deployments = current.get("deployments", [])
    if len(deployments) != 1 or deployments[0].get("rolloutState") != "COMPLETED":
        raise RuntimeError("test deployment not settled")
    desired = current["desiredCount"]
    if desired not in [0, 1, 2] or current["runningCount"] != desired or current["pendingCount"]:
        raise RuntimeError("test task count not settled")
    if require_one and desired != 1:
        raise RuntimeError("rolling test requires exactly one settled task")
    # RUNNING desired-status includes tasks that are still provisioning. The
    # STOPPED list also includes tasks whose containers are still STOPPING.
    arns = set()
    for state in ["RUNNING", "STOPPED"]:
        arns.update(aws("ecs", "list-tasks", {"cluster": plan["name"], "desiredStatus": state})["taskArns"])
    task_file = root / "tasks.json"
    if task_file.exists():
        arns.update(json.loads(task_file.read_text()))
    if arns:
        tasks = aws("ecs", "describe-tasks", {"cluster": plan["name"], "tasks": sorted(arns)})
        if tasks.get("failures"):
            raise RuntimeError("test task discovery incomplete")
        active = [t for t in tasks["tasks"] if t["lastStatus"] != "STOPPED"]
        if len(active) != desired or any(t["lastStatus"] != "RUNNING" for t in active):
            raise RuntimeError("old test tasks still stopping or new tasks not ready")
    elif desired:
        raise RuntimeError("running test tasks absent from discovery")
    return current


def roll(root, plan, aws, write, baseline, service):
    deadline = dt.datetime.fromisoformat(plan["deadline"]).replace(tzinfo=dt.timezone.utc)
    if deadline - dt.datetime.now(dt.timezone.utc) < dt.timedelta(minutes=15):
        raise RuntimeError("insufficient time for rolling test before cutoff")
    out = json.loads((root / "outputs.json").read_text())
    schedule = aws("scheduler", "get-schedule", {"Name": out["StopSchedule"]})
    expected = {"Cluster": plan["name"], "Service": service, "DesiredCount": 0}
    if schedule["State"] != "ENABLED" or json.loads(schedule["Target"]["Input"]) != expected:
        raise RuntimeError("automatic stop guard unavailable")
    ready_service(root, plan, aws, service)
    write(root / "production-before-roll.json", baseline())
    result = aws("ecs", "update-service", {"cluster": plan["name"], "service": service,
        "desiredCount": 1, "forceNewDeployment": True,
        "deploymentConfiguration": {"maximumPercent": 200, "minimumHealthyPercent": 100}})
    write(root / "roll-started.json", result)
    print("Isolated one-task rolling replacement requested; maximum overlap two. Scaling remains locked; stop with count=0 after verification.")

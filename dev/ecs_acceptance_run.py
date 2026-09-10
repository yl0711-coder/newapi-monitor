"""Bounded, explicit cloud acceptance steps. Never updates a production name.

AWS outputs/private configs stay in a 0700 temporary run directory. This tool
does not run an unattended scaling loop and never starts tasks during create.
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import subprocess
import tempfile

from ecs_acceptance_template import template

ACCOUNT = "842806122225"
REGION = "us-west-2"
SERVICE = "synthetic-collector-test"
AWS = "/opt/homebrew/bin/aws"
PROD = ["nexusapi-prod-master", "nexusapi-prod-worker-canary", "nexusapi-prod-worker-buffer-canary"]


def aws_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    # Console login is issued in its configured sign-in region (us-east-1).
    # Export through that profile first; a us-west-2 command override must not
    # redirect the OAuth token exchange to a different region. Credentials stay
    # in memory and the child environment, never argv/files/tool output.
    login = subprocess.run([AWS, "configure", "export-credentials", "--profile", "default", "--format", "process"],
                           capture_output=True, text=True, env=env, timeout=30)
    if login.returncode:
        raise RuntimeError("default console login unavailable; renew yanglei session")
    credentials = json.loads(login.stdout)
    env.update({"AWS_ACCESS_KEY_ID": credentials["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
                "AWS_SESSION_TOKEN": credentials["SessionToken"]})
    return env


def aws(service, operation, data=None):
    env = aws_env()
    args = [AWS, "--region", REGION, "--no-cli-pager", "--output", "json", service, operation]
    if data:
        args += ["--cli-input-json", json.dumps(data)]
    p = subprocess.run(args, capture_output=True, text=True, env=env, timeout=90)
    if p.returncode:
        raise RuntimeError(p.stderr[-2000:])
    return json.loads(p.stdout) if p.stdout.strip() else {}


def write(path, value):
    data = json.dumps(value, indent=2) if not isinstance(value, str) else value
    with open(path, "w", opener=lambda p, flags: os.open(p, flags, 0o600)) as f:
        f.write(data)


def identity():
    who = aws("sts", "get-caller-identity")
    if who["Account"] != ACCOUNT or who["Arn"] != f"arn:aws:iam::{ACCOUNT}:user/yanglei":
        raise RuntimeError("explicitly authorized yanglei identity required")


def baseline():
    result = aws("ecs", "describe-services", {"cluster": "nexusapi-prod-cluster", "services": PROD})
    if result.get("failures") or len(result["services"]) != 3:
        raise RuntimeError("production baseline unavailable; no test mutation permitted")
    return [{k: s.get(k) for k in ["serviceName", "desiredCount", "runningCount", "pendingCount", "taskDefinition", "networkConfiguration", "loadBalancers"]} for s in result["services"]]


def initialize():
    identity()
    before = baseline()
    root = Path(tempfile.mkdtemp(prefix="ecs-cloud-acceptance-", dir="/private/tmp"))
    name = "monitor-ecs-acceptance-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M")
    plan = {"account": ACCOUNT, "region": REGION, "name": name, "service": SERVICE,
            "images": {"nginx": "nginx-final-boundary-20260909", "reject": "reject-final-boundary-20260909", "synthetic": "synthetic-20260909"}}
    write(root / "plan.json", plan)
    write(root / "production-before.json", before)
    print(root)


def load(root):
    root = Path(root)
    if root.parent != Path("/private/tmp") or not root.name.startswith("ecs-cloud-acceptance-") or root.is_symlink() or root.stat().st_mode & 0o077:
        raise RuntimeError("private dedicated run directory required")
    plan = json.loads((root / "plan.json").read_text())
    if plan["account"] != ACCOUNT or plan["region"] != REGION or plan["service"] != SERVICE:
        raise RuntimeError("run identity mismatch")
    if not re.fullmatch(r"monitor-ecs-acceptance-\d{8}-\d{4}", plan["name"]):
        raise RuntimeError("dedicated test cluster name required")
    return root, plan


def prepare(root, plan, url):
    # Deadline starts BEFORE creating resources, so preparation cannot silently
    # lengthen the permitted cloud run. A later run requires explicit approval.
    if plan.get("deadline") or (root / "stack.json").exists():
        raise RuntimeError("run already prepared; cannot extend its cutoff")
    deadline = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=110)).strftime("%Y-%m-%dT%H:%M:%S")
    t = template(plan["name"], url, deadline, plan["images"])
    write(root / "template.json", t)
    plan.update({"url": url, "deadline": deadline})
    write(root / "plan.json", plan)
    print(json.dumps({"name": plan["name"], "resources": len(t["Resources"]), "desiredCount": 0, "deadlineUTC": deadline}))


def create(root, plan):
    identity()
    if (root / "stack.json").exists():
        raise RuntimeError("stack creation already attempted; inspect before retry")
    before = baseline()
    write(root / "production-at-create.json", before)
    t = json.loads((root / "template.json").read_text())
    expected = template(plan["name"], plan["url"], plan["deadline"], plan["images"])
    if t != expected:
        raise RuntimeError("template changed after validation")
    validated = aws("cloudformation", "validate-template", {"TemplateBody": json.dumps(t)})
    write(root / "template-validation.json", validated)
    result = aws("cloudformation", "create-stack", {"StackName": plan["name"], "TemplateBody": json.dumps(t),
        "Capabilities": ["CAPABILITY_NAMED_IAM"], "TimeoutInMinutes": 20, "OnFailure": "ROLLBACK",
        "Tags": [{"Key": "MonitorAcceptanceRun", "Value": plan["name"]}]})
    write(root / "stack.json", result)
    print(json.dumps(result))


def status(root, plan):
    stack_file = root / "stack.json"
    stack_id = json.loads(stack_file.read_text())["StackId"] if stack_file.exists() else plan["name"]
    stack = aws("cloudformation", "describe-stacks", {"StackName": stack_id})["Stacks"][0]
    outputs = {x["OutputKey"]: x["OutputValue"] for x in stack.get("Outputs", [])}
    write(root / "stack-status.json", stack)
    if outputs:
        write(root / "outputs.json", outputs)
    events = aws("cloudformation", "describe-stack-events", {"StackName": stack_id})["StackEvents"]
    write(root / "stack-events.json", events)
    print(json.dumps({"status": stack["StackStatus"], "failures": [{k: e.get(k) for k in ["LogicalResourceId", "ResourceStatus", "ResourceStatusReason"]} for e in events if "FAILED" in e["ResourceStatus"]][:8], "outputs": outputs}))


def receiver_config(root, plan):
    identity()
    out = json.loads((root / "outputs.json").read_text())
    target = root / "receiver.json"
    if target.exists():
        raise RuntimeError("private receiver config already exists; do not rotate identity")
    bridge = aws("secretsmanager", "get-secret-value", {"SecretId": out["BridgeSecret"]})["SecretString"]
    evidence = aws("secretsmanager", "get-secret-value", {"SecretId": out["EvidenceSecret"]})["SecretString"]
    config = {"region": REGION, "audience": plan["name"], "bridge_token": bridge, "evidence_key": evidence,
        "session_key": secrets.token_hex(32), "ingest_key": secrets.token_hex(32),
        "bucket": out["Bucket"], "prefix": "isolated/", "account": ACCOUNT,
        "policies": [{"cluster_arn": out["Cluster"], "service_arn": out["Service"], "task_role_arn": out["TaskRole"], "containers": {"synthetic": ["access", "error", "evidence", "reject"]}}]}
    write(target, config)
    # Source profile is read in the original configuration, NOT recursively
    # from this temporary AWS_CONFIG_FILE. Only the assumed role reaches Monitor.
    credential_process = receiver_credential_process()
    write(root / "aws-config", f"[profile acceptance-receiver]\nregion = {REGION}\nrole_arn = {out['ReceiverRole']}\nsource_profile = acceptance-bootstrap\n\n[profile acceptance-bootstrap]\ncredential_process = {credential_process}\n")
    print("Private receiver config and read-only AWS role profile prepared; no credentials printed.")


def receiver_credential_process():
    # Signing region belongs to the original login profile, not the ECS region.
    unset = ["AWS_CONFIG_FILE", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
             "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    return "/usr/bin/env " + " ".join("-u " + key for key in unset) + f" {AWS} configure export-credentials --profile default --format process"


def recover_failed(root, plan):
    identity()
    stack = aws("cloudformation", "describe-stacks", {"StackName": plan["name"]})["Stacks"][0]
    if stack["StackStatus"] != "ROLLBACK_COMPLETE":
        raise RuntimeError("only fully rolled-back test stacks can be removed")
    resources = aws("cloudformation", "list-stack-resources", {"StackName": plan["name"]})
    if any(r["ResourceStatus"] != "DELETE_COMPLETE" for r in resources["StackResourceSummaries"]):
        raise RuntimeError("rollback has retained resources; inspect manually")
    write(root / "failed-stack-resources.json", resources)
    aws("cloudformation", "delete-stack", {"StackName": stack["StackId"]})
    (root / "stack.json").rename(root / "failed-stack.json")
    write(root / "template.json", template(plan["name"], plan["url"], plan["deadline"], plan["images"]))
    print("Removed empty failed stack record; original cutoff unchanged. Inspect before recreate.")


def push_images(root, plan):
    identity()
    out = json.loads((root / "outputs.json").read_text())
    repo = out["Repository"]
    expected = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{plan['name']}"
    if repo != expected:
        raise RuntimeError("unexpected test repository")
    auth = aws("ecr", "get-authorization-token", {"registryIds": [ACCOUNT]})["authorizationData"][0]
    username, password = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)
    config = root / "docker-config"
    config.mkdir(mode=0o700, exist_ok=True)
    docker = ["docker", "--config", str(config)]
    try:
        subprocess.run(docker + ["login", "--username", username, "--password-stdin", repo.split('/')[0]], input=password, text=True, check=True, capture_output=True, timeout=45)
        local = {"nginx": "newapi-nginxcollector:local-final-boundary-20260909", "reject": "reject-collector:local-final-boundary-20260909", "synthetic": "monitor-ecs-synthetic:acceptance-20260909"}
        tags = plan["images"]
        for kind, source in local.items():
            target = repo + ":" + tags[kind]
            subprocess.run(docker + ["tag", source, target], check=True, timeout=30)
            result = subprocess.run(docker + ["push", target], text=True, capture_output=True, timeout=240)
            write(root / ("push-" + kind + ".log"), result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError("image push failed; inspect private push log")
            print("Pushed test image: " + kind, flush=True)
    finally:
        subprocess.run(docker + ["logout", repo.split('/')[0]], capture_output=True, timeout=30)


def scale(root, plan, count):
    identity()
    if count not in [0, 1, 2]:
        raise RuntimeError("test count must be 0, 1, or 2")
    if count:
        deadline = dt.datetime.fromisoformat(plan["deadline"]).replace(tzinfo=dt.timezone.utc)
        if deadline - dt.datetime.now(dt.timezone.utc) < dt.timedelta(minutes=10):
            raise RuntimeError("insufficient time before mandatory cutoff")
        out = json.loads((root / "outputs.json").read_text())
        schedule = aws("scheduler", "get-schedule", {"Name": out["StopSchedule"]})
        if schedule["State"] != "ENABLED" or json.loads(schedule["Target"]["Input"]) != {"Cluster": plan["name"], "Service": SERVICE, "DesiredCount": 0}:
            raise RuntimeError("automatic stop guard unavailable")
        write(root / "production-before-start.json", baseline())
        from ecs_acceptance_roll import ready_service
        ready_service(root, plan, aws, SERVICE, require_one=False)
    result = aws("ecs", "update-service", {"cluster": plan["name"], "service": SERVICE, "desiredCount": count})
    write(root / f"scale-{dt.datetime.now().strftime('%H%M%S')}-{count}.json", result)
    print(json.dumps({k: result["service"][k] for k in ["serviceName", "desiredCount", "runningCount", "pendingCount"]}))


def inspect_tasks(root, plan):
    service = aws("ecs", "describe-services", {"cluster": plan["name"], "services": [SERVICE]})
    arns = set()
    for state in ["RUNNING", "STOPPED"]:
        arns.update(aws("ecs", "list-tasks", {"cluster": plan["name"], "desiredStatus": state})["taskArns"])
    task_file = root / "tasks.json"
    previous = json.loads(task_file.read_text()) if task_file.exists() else {}
    if arns:
        result = aws("ecs", "describe-tasks", {"cluster": plan["name"], "tasks": sorted(arns)})
        if result.get("failures"):
            raise RuntimeError("task discovery incomplete")
        previous.update({t["taskArn"]: t for t in result["tasks"]})
    write(task_file, previous)
    write(root / "service-current.json", service)
    write(root / ("tasks-observed-" + dt.datetime.now(dt.timezone.utc).strftime("%H%M%S%f") + ".json"),
          {"observed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "service": service, "tasks": previous})
    print(json.dumps({"service": [{k: s[k] for k in ["desiredCount", "runningCount", "pendingCount"]} for s in service["services"]],
        "tasks": [{"id": t["taskArn"].rsplit('/', 1)[-1], "state": t["lastStatus"], "stop": t.get("stoppedReason"),
                   "containers": [{k: c.get(k) for k in ["name", "lastStatus", "exitCode", "reason"]} for c in t["containers"]]} for t in previous.values()]}))


def collect_logs(root, plan):
    from ecs_acceptance_logs import stream_events
    out = json.loads((root / "outputs.json").read_text())
    streams = aws("logs", "describe-log-streams", {"logGroupName": out["TaskLogs"]})["logStreams"]
    summaries = []
    for stream in streams:
        name = stream["logStreamName"]
        if not name.startswith("test/"):
            raise RuntimeError("unexpected test stream")
        events = stream_events(aws, out["TaskLogs"], name)
        write(root / ("logs-" + name.replace('/', '-') + ".json"), events)
        summaries.append({"stream": name, "events": len(events), "last": [e["message"] for e in events[-3:]]})
    print(json.dumps(summaries))


def export_archive(root, plan):
    identity()
    out = json.loads((root / "outputs.json").read_text())
    bucket = out["Bucket"]
    if bucket != plan["name"] + "-" + ACCOUNT:
        raise RuntimeError("unexpected archive bucket")
    listing = aws("s3api", "list-objects-v2", {"Bucket": bucket, "ExpectedBucketOwner": ACCOUNT})
    if any(not o["Key"].startswith("isolated/") for o in listing.get("Contents", [])):
        raise RuntimeError("unexpected archive key")
    write(root / "archive-objects.json", listing)
    destination = root / "archive"
    destination.mkdir(mode=0o700, exist_ok=True)
    result = subprocess.run([AWS, "--region", REGION, "s3", "sync", "s3://" + bucket + "/", str(destination), "--only-show-errors", "--no-progress"],
                            env=aws_env(), capture_output=True, text=True, timeout=300)
    if result.returncode:
        raise RuntimeError("test archive export failed: " + result.stderr[-500:])
    for o in listing.get("Contents", []):
        path = destination / o["Key"]
        if path.is_symlink() or not path.is_file() or path.stat().st_size != o["Size"]:
            raise RuntimeError("archive export verification failed")
        if hashlib.md5(path.read_bytes()).hexdigest() != o["ETag"].strip('"'):
            raise RuntimeError("archive export digest mismatch")
    print(json.dumps({"objects": len(listing.get("Contents", [])), "bytes": sum(o["Size"] for o in listing.get("Contents", [])), "export": str(destination)}))


def cleanup(root, plan):
    identity()
    out = json.loads((root / "outputs.json").read_text())
    service = aws("ecs", "describe-services", {"cluster": plan["name"], "services": [SERVICE]})["services"][0]
    if any(service[k] for k in ["desiredCount", "runningCount", "pendingCount"]):
        raise RuntimeError("test service is not fully stopped")
    known = json.loads((root / "tasks.json").read_text())
    tasks = aws("ecs", "describe-tasks", {"cluster": plan["name"], "tasks": list(known)})
    if tasks.get("failures") or any(t["lastStatus"] != "STOPPED" for t in tasks["tasks"]):
        raise RuntimeError("some test tasks have not stopped")
    bucket = out["Bucket"]
    if bucket != plan["name"] + "-" + ACCOUNT:
        raise RuntimeError("unexpected test bucket")
    current = aws("s3api", "list-objects-v2", {"Bucket": bucket, "ExpectedBucketOwner": ACCOUNT})
    saved = json.loads((root / "archive-objects.json").read_text())
    if current.get("Contents", []) != saved.get("Contents", []):
        raise RuntimeError("archive changed since export; export again before cleanup")
    objects = current.get("Contents", [])
    for o in objects:
        path = root / "archive" / o["Key"]
        if not o["Key"].startswith("isolated/") or path.is_symlink() or hashlib.md5(path.read_bytes()).hexdigest() != o["ETag"].strip('"'):
            raise RuntimeError("local recovery evidence missing or changed")
    for start in range(0, len(objects), 1000):
        result = aws("s3api", "delete-objects", {"Bucket": bucket, "ExpectedBucketOwner": ACCOUNT,
            "Delete": {"Objects": [{"Key": o["Key"]} for o in objects[start:start+1000]], "Quiet": True}})
        if result.get("Errors"):
            raise RuntimeError("test archive cleanup incomplete")
    stack = json.loads((root / "stack.json").read_text())["StackId"]
    if f":stack/{plan['name']}/" not in stack:
        raise RuntimeError("unexpected stack ARN")
    aws("cloudformation", "delete-stack", {"StackName": stack})
    write(root / "production-after.json", baseline())
    write(root / "cleanup-started.json", {"stack": stack, "archivedLocally": len(objects), "at": dt.datetime.now(dt.timezone.utc).isoformat()})
    print("Test objects preserved locally and removed from test bucket; test-stack deletion requested. Production baseline saved.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["init", "prepare", "create", "status", "receiver-config", "recover-failed", "push", "scale", "roll", "tasks", "logs", "export", "cleanup"])
    p.add_argument("--run-dir")
    p.add_argument("--url")
    p.add_argument("--count", type=int)
    args = p.parse_args()
    if args.action == "init":
        return initialize()
    root, plan = load(args.run_dir)
    if args.action == "prepare":
        return prepare(root, plan, args.url)
    if args.action == "create":
        return create(root, plan)
    if args.action == "status":
        return status(root, plan)
    if args.action == "recover-failed":
        return recover_failed(root, plan)
    if args.action == "push":
        return push_images(root, plan)
    if args.action == "scale":
        return scale(root, plan, args.count)
    if args.action == "roll":
        from ecs_acceptance_roll import roll
        identity()
        return roll(root, plan, aws, write, baseline, SERVICE)
    if args.action == "tasks":
        return inspect_tasks(root, plan)
    if args.action == "logs":
        return collect_logs(root, plan)
    if args.action == "export":
        return export_archive(root, plan)
    if args.action == "cleanup":
        return cleanup(root, plan)
    return receiver_config(root, plan)


if __name__ == "__main__":
    main()

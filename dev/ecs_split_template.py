"""Generate a reviewable, digest-gated TWO-producer isolated AWS template.

Pure local generation; importing this module cannot create resources or read
credentials. The legacy cloud runner deliberately remains incompatible.
"""
import copy
import json

from ecs_acceptance_template import ref, sub, template
from ecs_task_contract import INIT, check_task

DIGEST_PARAMETERS = ("NginxImageDigest", "RejectImageDigest", "SyntheticImageDigest")


def environment(container, changes):
    values = {row["Name"]: row["Value"] for row in container.get("Environment", [])}
    values.update(changes)
    container["Environment"] = [{"Name": k, "Value": v} for k, v in values.items()]


def mount(name, path, readonly=False):
    return {"SourceVolume": name, "ContainerPath": path, "ReadOnly": readonly}


def dependency(name, condition="START"):
    return {"ContainerName": name, "Condition": condition}


def split_template(name):
    # Bootstrap-only placeholders are replaced with required CF parameters below.
    result = template(name, "https://review-only.trycloudflare.com", "2099-01-01T00:00:00",
                      {"nginx": "unused-nginx", "reject": "unused-reject", "synthetic": "unused-synthetic"})
    result["Description"] = "Monitor split-producer isolated acceptance; no tasks until digest gate; desired count always zero"
    result["Parameters"] = {
        "ReceiverOrigin": {"Type": "String", "AllowedPattern": r"https://[a-z0-9-]+\.trycloudflare\.com",
                           "Description": "Dedicated isolated receiver only; never production Monitor"},
        "StopAtUTC": {"Type": "String", "AllowedPattern": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
                      "Description": "Fixed UTC cutoff; check future and <=110 minutes before bootstrap; do not extend"},
        "EnableTestDefinition": {"Type": "String", "Default": "no", "AllowedValues": ["no", "yes"]},
    }
    for key in DIGEST_PARAMETERS:
        result["Parameters"][key] = {"Type": "String", "Default": "UNSET",
                                     "AllowedPattern": r"UNSET|sha256:[a-f0-9]{64}",
                                     "Description": "Verified ECR manifest digest; NOT Docker local image ID"}
    pinned = [{"Fn::Not": [{"Fn::Equals": [ref(key), "UNSET"]}]} for key in DIGEST_PARAMETERS]
    result["Conditions"] = {"TestDefinitionEnabled": {"Fn::And": [
        {"Fn::Equals": [ref("EnableTestDefinition"), "yes"]}, *pinned]}}
    result["Rules"] = {"DigestsRequired": {
        "RuleCondition": {"Fn::Equals": [ref("EnableTestDefinition"), "yes"]},
        "Assertions": [{"Assert": {"Fn::And": pinned}, "AssertDescription": "Verify and supply all three registry digests first"}]}}
    resources = result["Resources"]
    task = resources["TaskDefinition"]["Properties"]
    previous = {c["Name"]: c for c in task["ContainerDefinitions"]}
    init = copy.deepcopy(previous["init"])
    init.update(Name=INIT, Image=sub("${Repository.RepositoryUri}@${SyntheticImageDigest}"), User="0",
                MountPoints=[mount("NginxLogs", "/logs"), mount("RejectLogs", "/app/logs"),
                             mount("NginxState", "/state-nginx"), mount("RejectState", "/state-reject")],
                Command=["chown 100:101 /logs /app/logs /state-nginx /state-reject; "
                         "chmod 755 /logs /app/logs; chmod 700 /state-nginx /state-reject"])
    collectors = []
    for old_name, cname, producer, kind, root, logs, state, digest in (
        ("nginxcollector", "nginxcollector", "nginx", "nginx", "/logs", "NginxLogs", "NginxState", "NginxImageDigest"),
        ("rejectcollector", "reject-collector", "new-api", "reject", "/app/logs", "RejectLogs", "RejectState", "RejectImageDigest"),
    ):
        c = copy.deepcopy(previous[old_name])
        c.update(Name=cname, Image=sub("${Repository.RepositoryUri}@${" + digest + "}"), Essential=False,
                 StopTimeout=60, DependsOn=[dependency(INIT, "SUCCESS")],
                 MountPoints=[mount(logs, root, True), mount(state, "/data/ecs")])
        changes = {"ECSLOG_PRODUCER_CONTAINER": producer, "ECSLOG_FINAL_FILE_CONTRACT": "newapi-files-v1",
                   "ECSLOG_MONITOR_URL": ref("ReceiverOrigin"), "ECSLOG_FINAL_LOG_ROOT": root}
        if kind == "nginx":
            changes["NGINXCOLLECTOR_LOG_PATH"] = root + "/nexusapi_access.jsonl"
        else:
            changes["COLLECTOR_LOG_GLOB"] = root + "/oneapi-*.log"
        environment(c, changes)
        collectors.append(c)
    producers = []
    for name, collector, logs, root in (("new-api", "reject-collector", "RejectLogs", "/app/logs"),
                                        ("nginx", "nginxcollector", "NginxLogs", "/logs")):
        c = copy.deepcopy(previous["synthetic"])
        c.update(Name=name, Image=sub("${Repository.RepositoryUri}@${SyntheticImageDigest}"), StopTimeout=120,
                 DependsOn=[dependency(collector)], MountPoints=[mount(logs, root)],
                 HealthCheck={"Command": ["CMD", "/usr/local/bin/synthetic", "--health"],
                              "Interval": 5, "Timeout": 2, "Retries": 3, "StartPeriod": 10})
        if name == "nginx":
            c["DependsOn"].append(dependency("new-api", "HEALTHY"))
        environment(c, {"ECSLOG_SYNTHETIC_KIND": name})
        producers.append(c)
    task["ContainerDefinitions"] = [init, *collectors, *producers]
    task["Volumes"] = [{"Name": n} for n in ("NginxLogs", "RejectLogs", "NginxState", "RejectState")]
    for key in ("TaskDefinition", "Service", "StopSchedule"):
        resources[key]["Condition"] = "TestDefinitionEnabled"
    resources["StopSchedule"]["Properties"]["ScheduleExpression"] = sub("at(${StopAtUTC})")
    resources["Bridge"]["Properties"]["Environment"]["Variables"]["ECS_LOG_MONITOR_REGISTER_URL"] = sub("${ReceiverOrigin}/internal/ecs/v1/register")
    result["Outputs"]["StopSchedule"]["Condition"] = "TestDefinitionEnabled"
    result["Outputs"]["TaskDefinition"] = {"Condition": "TestDefinitionEnabled", "Value": ref("TaskDefinition")}
    # Receiver's bootstrap output Service is an ARN string, not a claim the
    # gated service already exists. No task/service ARN is imported from prod.
    check_task(task)
    raw = json.dumps(result)
    if "review-only.trycloudflare" in raw or "2099-01-01" in raw or "unused-" in raw:
        raise ValueError("unresolved bootstrap placeholder")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    print(json.dumps(split_template(args.name), indent=2))

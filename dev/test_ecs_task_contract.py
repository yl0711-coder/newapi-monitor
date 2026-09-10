"""Synthetic dictionaries only: no credentials, SDK, Docker or network calls."""
import copy
import unittest

from ecs_task_contract import INIT, PAIRS, check_business_unchanged, check_task, initializer_digest


def mount(volume, path, readonly=False):
    return {"SourceVolume": volume, "ContainerPath": path, "ReadOnly": readonly}


def dep(name, condition="START"):
    return {"ContainerName": name, "Condition": condition}


def fixture():
    containers = [{"Name": INIT, "Essential": False, "MountPoints": []}]
    for collector, producer in PAIRS.items():
        kind = "nginx" if producer == "nginx" else "reject"
        root = "/logs" if kind == "nginx" else "/app/logs"
        env = {"ECSLOG_PRODUCER_CONTAINER": producer, "ECSLOG_KIND": kind,
               "ECSLOG_FINAL_FILE_CONTRACT": "newapi-files-v1", "ECSLOG_STATE_ROOT": "/data/ecs",
               "ECSLOG_FINAL_LOG_ROOT": root, "ECSLOG_ARCHIVE_ENABLED": "true",
               "ECSLOG_ARCHIVE_CLOSURE": "true", "ECSLOG_DEFERRED_ARCHIVE_ACK": "true",
               "ECSLOG_SCOPE": "isolated"}
        if kind == "nginx":
            env.update(NGINXCOLLECTOR_LOG_PATH=root + "/nexusapi_access.jsonl",
                       NGINXCOLLECTOR_ERROR_LOG_PATH=root + "/error.log")
        else:
            env["COLLECTOR_LOG_GLOB"] = root + "/oneapi-*.log"
        containers.append({"Name": collector, "Essential": False, "Image": "fixture-collector",
                           "ReadonlyRootFilesystem": True, "User": "100:101", "StopTimeout": 60,
                           "LinuxParameters": {"Capabilities": {"Drop": ["ALL"]}},
                           "DependsOn": [dep(INIT, "SUCCESS")],
                           "Environment": [{"Name": k, "Value": v} for k, v in env.items()],
                           "MountPoints": [mount(kind + "-logs", root, True), mount(kind + "-state", "/data/ecs")]})
        containers.append({"Name": producer, "Essential": True, "Image": "synthetic-not-business",
                           "StopTimeout": 120, "DependsOn": [dep(collector)],
                           "HealthCheck": {"Command": ["CMD", "true"]},
                           "MountPoints": [mount(kind + "-logs", root)]})
        containers[0]["MountPoints"].append(mount(kind + "-state", "/state-" + kind))
    next(c for c in containers if c["Name"] == "nginx")["DependsOn"].append(dep("new-api", "HEALTHY"))
    return {"Family": "offline-contract-fixture", "Cpu": "512", "Memory": "1024",
            "NetworkMode": "awsvpc", "RequiresCompatibilities": ["FARGATE"],
            "TaskRoleArn": "fixture-review-only", "ExecutionRoleArn": "fixture-execution",
            "ContainerDefinitions": containers,
            "Volumes": [{"Name": k + suffix} for k in ("nginx", "reject") for suffix in ("-logs", "-state")]}


class TaskContractTest(unittest.TestCase):
    def setUp(self):
        self.task = fixture()
        self.c = {c["Name"]: c for c in self.task["ContainerDefinitions"]}

    def reject(self, mutation, message):
        mutation()
        with self.assertRaisesRegex(ValueError, message):
            check_task(self.task)

    def test_valid_is_not_deployment_approval(self):
        result = check_task(self.task)
        self.assertTrue(result["static_checks_passed"])
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["aws_verified"])

    def test_production_contract_is_explicit_and_still_not_approval(self):
        for name in PAIRS:
            collector = self.c[name]
            next(e for e in collector["Environment"] if e["Name"] == "ECSLOG_SCOPE")["Value"] = "production"
        result = check_task(self.task, scope="production")
        self.assertTrue(result["static_checks_passed"])
        self.assertFalse(result["production_ready"])
        with self.assertRaisesRegex(ValueError, "contract mismatch"):
            check_task(self.task)

    def test_historical_wrong_stop_order(self):
        self.c["nginx"]["DependsOn"] = [dep("new-api", "HEALTHY")]
        self.reject(lambda: self.c["nginxcollector"].update(DependsOn=[dep("nginx")]), "only on successful")

    def test_dependency_cycle(self):
        self.reject(lambda: self.c[INIT].update(DependsOn=[dep("nginx")]), "cycle")

    def test_unknown_dependency(self):
        self.reject(lambda: self.c[INIT].update(DependsOn=[dep("missing")]), "unknown/self")

    def test_duplicate_dependency(self):
        self.reject(lambda: self.c["nginx"]["DependsOn"].append(dep("new-api")), "duplicate")

    def test_health_check_required(self):
        self.reject(lambda: self.c["new-api"].pop("HealthCheck"), "lacks health check")

    def test_init_must_not_be_essential(self):
        self.reject(lambda: self.c[INIT].update(Essential=True), "init must be nonessential")

    def test_no_collector_health_gating(self):
        self.c["nginxcollector"]["HealthCheck"] = {"Command": ["CMD", "true"]}
        self.reject(lambda: self.c["nginx"]["DependsOn"][0].update(Condition="HEALTHY"), "producer must start")

    def test_unsafe_collector_settings(self):
        for setting, value, error in [("Essential", True, "nonessential"), ("User", "0", "nonroot"),
                                      ("ReadonlyRootFilesystem", False, "read-only"), ("Privileged", True, "privileged"),
                                      ("StopTimeout", 30, "budget"), ("StopTimeout", 121, "budget"),
                                      ("StopTimeout", True, "budget"), ("StopTimeout", None, "budget"),
                                      ("LinuxParameters", {"Capabilities": {"Drop": ["ALL"], "Add": ["NET_ADMIN"]}}, "capabilities")]:
            with self.subTest(setting=setting, value=value):
                candidate = fixture()
                candidate["ContainerDefinitions"][1][setting] = value
                with self.assertRaisesRegex(ValueError, error):
                    check_task(candidate)

    def test_file_archive_scope_contract(self):
        for name, wrong in [("ECSLOG_PRODUCER_CONTAINER", "synthetic"), ("ECSLOG_SCOPE", "production"),
                            ("ECSLOG_FINAL_FILE_CONTRACT", ""), ("ECSLOG_ARCHIVE_CLOSURE", "false"),
                            ("NGINXCOLLECTOR_LOG_PATH", "/logs/access.jsonl")]:
            with self.subTest(name=name):
                candidate = fixture()
                env = candidate["ContainerDefinitions"][1]["Environment"]
                next(e for e in env if e["Name"] == name)["Value"] = wrong
                with self.assertRaisesRegex(ValueError, "contract mismatch"):
                    check_task(candidate)

    def test_duplicate_environment(self):
        env = self.c["nginxcollector"]["Environment"]
        self.reject(lambda: env.append(copy.deepcopy(env[0])), "duplicate")

    def test_readonly_log_volume(self):
        self.reject(lambda: self.c["nginxcollector"]["MountPoints"][0].update(ReadOnly=False), "log mount must be read-only")

    def test_wrong_log_volume(self):
        self.reject(lambda: self.c["nginxcollector"]["MountPoints"][0].update(SourceVolume="reject-logs"), "actual log volume")

    def test_shared_state(self):
        self.reject(lambda: self.c["reject-collector"]["MountPoints"][1].update(SourceVolume="nginx-state"), "private to collector")

    def test_producer_cannot_read_agent_credentials(self):
        self.reject(lambda: self.c["nginx"]["MountPoints"].append(mount("nginx-state", "/credentials", True)), "private to collector")

    def test_producers_cannot_share_log_volume(self):
        self.reject(lambda: self.c["new-api"]["MountPoints"].append(mount("nginx-logs", "/extra")), "log volumes must be isolated")

    def test_init_state_must_be_writable(self):
        self.reject(lambda: self.c[INIT]["MountPoints"][0].update(ReadOnly=True), "initialize writable")

    def test_nested_mount_bypass(self):
        self.reject(lambda: self.c["nginxcollector"]["MountPoints"].append(mount("reject-logs", "/logs/hidden")), "overlapping")

    def test_parent_path_bypass(self):
        self.reject(lambda: self.c["nginxcollector"]["MountPoints"][1].update(ContainerPath="/data/../logs"), "noncanonical")

    def test_inherited_mount_bypass(self):
        self.reject(lambda: self.c["nginxcollector"].update(VolumesFrom=[{"SourceContainer": "new-api"}]), "inherited")

    def test_external_volume(self):
        self.reject(lambda: self.task["Volumes"][0].update(EFSVolumeConfiguration={"FilesystemId": "fs-fake"}), "task-local")

    def test_undefined_volume(self):
        self.reject(lambda: self.task["Volumes"].pop(), "undefined")

    def test_duplicate_container(self):
        self.reject(lambda: self.task["ContainerDefinitions"].append(self.c["nginx"]), "duplicate")


class BusinessInvariantTest(unittest.TestCase):
    def setUp(self):
        self.after = fixture()
        self.before = copy.deepcopy(self.after)
        for c in self.before["ContainerDefinitions"]:
            if c["Name"] in PAIRS.values():
                c["DependsOn"] = [d for d in c["DependsOn"] if d["ContainerName"] not in PAIRS]
            elif c["Name"] in PAIRS:
                c["DependsOn"] = [dep(PAIRS[c["Name"]])]

    def test_only_dependency_reversal_allowed(self):
        result = check_business_unchanged(self.before, self.after)
        self.assertTrue(result["business_configuration_unchanged"])
        self.assertTrue(result["iam_review_required"])
        self.assertFalse(result["deployment_authorized"])

    def test_business_settings_unchanged(self):
        for field, value in {"Image": "another-image", "Command": ["changed"], "StopTimeout": 119,
                             "Environment": [{"Name": "SQL_DSN", "Value": "synthetic"}],
                             "Secrets": [{"Name": "KEY", "ValueFrom": "fixture"}],
                             "PortMappings": [{"ContainerPort": 81}]}.items():
            with self.subTest(field=field):
                candidate = copy.deepcopy(self.after)
                next(c for c in candidate["ContainerDefinitions"] if c["Name"] == "new-api")[field] = value
                with self.assertRaisesRegex(ValueError, "business container configuration"):
                    check_business_unchanged(self.before, candidate)

    def test_cannot_drop_business_dependency(self):
        c = next(c for c in self.after["ContainerDefinitions"] if c["Name"] == "nginx")
        c["DependsOn"] = [dep("nginxcollector")]
        with self.assertRaisesRegex(ValueError, "business dependencies"):
            check_business_unchanged(self.before, self.after)

    def test_cannot_change_compute_or_execution_role(self):
        for field in ["Cpu", "Memory", "ExecutionRoleArn", "NetworkMode"]:
            with self.subTest(field=field):
                candidate = copy.deepcopy(self.after)
                candidate[field] = "changed"
                with self.assertRaises(ValueError):
                    check_business_unchanged(self.before, candidate)

    def test_four_to_five_requires_separate_initializer_review(self):
        self.before["ContainerDefinitions"] = [c for c in self.before["ContainerDefinitions"] if c["Name"] != INIT]
        self.before["Volumes"] = [v for v in self.before["Volumes"] if not v["Name"].endswith("-state")]
        for c in self.before["ContainerDefinitions"]:
            if c["Name"] in PAIRS:
                c["MountPoints"] = [m for m in c["MountPoints"] if m["ContainerPath"] != "/data/ecs"]
        with self.assertRaisesRegex(ValueError, "separate reviewed"):
            check_business_unchanged(self.before, self.after)
        init = next(c for c in self.after["ContainerDefinitions"] if c["Name"] == INIT)
        result = check_business_unchanged(self.before, self.after, reviewed_initializer_sha256=initializer_digest(init))
        self.assertTrue(result["initializer_changed"])
        self.assertTrue(result["initializer_fingerprint_matched"])
        self.assertFalse(result["deployment_authorized"])
        self.assertFalse(result["production_ready"])

    def test_existing_initializer_cannot_change_silently(self):
        init = next(c for c in self.after["ContainerDefinitions"] if c["Name"] == INIT)
        original_digest = initializer_digest(init)
        init["Command"] = ["changed-init-command"]
        with self.assertRaisesRegex(ValueError, "separate reviewed"):
            check_business_unchanged(self.before, self.after)
        with self.assertRaisesRegex(ValueError, "differs from reviewed"):
            check_business_unchanged(self.before, self.after, reviewed_initializer_sha256=original_digest)

    def test_review_fingerprint_must_match_even_when_init_unchanged(self):
        for value in ["", True, {}, "A" * 64, "0" * 64]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                check_business_unchanged(self.before, self.after, reviewed_initializer_sha256=value)

    def test_cannot_remove_or_rename_existing_container(self):
        for name in ["new-api", "nginx", "nginxcollector", "reject-collector"]:
            with self.subTest(name=name):
                before = copy.deepcopy(self.before)
                before["ContainerDefinitions"] = [c for c in before["ContainerDefinitions"] if c["Name"] != name]
                with self.assertRaisesRegex(ValueError, "only the state initializer"):
                    check_business_unchanged(before, self.after)

    def test_initializer_fingerprint_does_not_allow_business_drift(self):
        init = next(c for c in self.after["ContainerDefinitions"] if c["Name"] == INIT)
        self.before["ContainerDefinitions"] = [c for c in self.before["ContainerDefinitions"] if c["Name"] != INIT]
        next(c for c in self.after["ContainerDefinitions"] if c["Name"] == "new-api")["Image"] = "wrong-image"
        with self.assertRaisesRegex(ValueError, "business container configuration"):
            check_business_unchanged(self.before, self.after, reviewed_initializer_sha256=initializer_digest(init))

    def test_initializer_digest_is_order_independent_but_value_sensitive(self):
        self.assertEqual(initializer_digest({"a": 1, "b": 2}), initializer_digest({"b": 2, "a": 1}))
        self.assertNotEqual(initializer_digest({"a": 1}), initializer_digest({"a": 2}))


if __name__ == "__main__":
    unittest.main()

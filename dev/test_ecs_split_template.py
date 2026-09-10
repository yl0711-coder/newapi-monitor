import json
import unittest

from ecs_acceptance_template import template
from ecs_split_template import DIGEST_PARAMETERS, split_template
from ecs_task_contract import check_task


class SplitTemplateTest(unittest.TestCase):
    def setUp(self):
        self.name = "monitor-ecs-acceptance-split-review"
        self.t = split_template(self.name)
        self.r = self.t["Resources"]

    def test_generated_task_passes_actual_contract(self):
        result = check_task(self.r["TaskDefinition"]["Properties"])
        self.assertTrue(result["static_checks_passed"])
        self.assertFalse(result["production_ready"])

    def test_initializer_keeps_only_the_reviewed_chown_capability(self):
        containers = self.r["TaskDefinition"]["Properties"]["ContainerDefinitions"]
        initializer = next(c for c in containers if c["Name"] == "nginxcollector-init")
        self.assertEqual(initializer["User"], "0")
        self.assertTrue(initializer["ReadonlyRootFilesystem"])
        self.assertNotIn("LinuxParameters", initializer)
        self.assertFalse(initializer.get("Environment"))
        self.assertFalse(initializer.get("Secrets"))

    def test_no_tasks_before_explicit_gate_and_three_registry_digests(self):
        self.assertEqual(self.t["Parameters"]["EnableTestDefinition"]["Default"], "no")
        condition = self.t["Conditions"]["TestDefinitionEnabled"]["Fn::And"]
        self.assertEqual(len(condition), 4)
        for key in DIGEST_PARAMETERS:
            self.assertEqual(self.t["Parameters"][key]["Default"], "UNSET")
            self.assertIn({"Fn::Not": [{"Fn::Equals": [{"Ref": key}, "UNSET"]}]}, condition)
        for key in ("TaskDefinition", "Service", "StopSchedule"):
            self.assertEqual(self.r[key]["Condition"], "TestDefinitionEnabled")
        self.assertEqual(self.r["Service"]["Properties"]["DesiredCount"], 0)
        self.assertEqual(self.t["Outputs"]["StopSchedule"]["Condition"], "TestDefinitionEnabled")

    def test_images_are_local_repository_digest_references(self):
        containers = self.r["TaskDefinition"]["Properties"]["ContainerDefinitions"]
        expected = {"nginxcollector": "NginxImageDigest", "reject-collector": "RejectImageDigest"}
        for c in containers:
            key = expected.get(c["Name"], "SyntheticImageDigest")
            self.assertEqual(c["Image"], {"Fn::Sub": "${Repository.RepositoryUri}@${" + key + "}"})
            self.assertNotIn("PortMappings", c)

    def test_no_production_inputs_or_expensive_shared_network(self):
        raw = json.dumps(self.t)
        for forbidden in ("nexusapi-prod", "SQL_DSN", "DATABASE_URL", "Fn::ImportValue", "AWS::RDS",
                          "AWS::EC2::NatGateway", "AWS::EC2::VPCPeeringConnection", "AWS::ElasticLoadBalancing"):
            self.assertNotIn(forbidden, raw)
        self.assertEqual(self.r["SecurityGroup"]["Properties"]["SecurityGroupIngress"], [])
        self.assertNotIn("LoadBalancers", self.r["Service"]["Properties"])
        task = self.r["TaskDefinition"]["Properties"]
        self.assertEqual((task["Cpu"], task["Memory"]), ("512", "1024"))
        self.assertEqual(task["RuntimePlatform"]["CpuArchitecture"], "X86_64")

    def test_security_resources_unchanged_from_reviewed_legacy_template(self):
        before = template(self.name, "https://review-only.trycloudflare.com", "2099-01-01T00:00:00",
                          {"nginx": "unused-nginx", "reject": "unused-reject", "synthetic": "unused-synthetic"})["Resources"]
        for name, resource in before.items():
            if resource["Type"].startswith(("AWS::IAM::", "AWS::S3::")) or name in ("RegisterMethod", "BridgeInvoke", "SecurityGroup"):
                self.assertEqual(self.r[name], resource, name)

    def test_cutoff_and_receiver_require_explicit_parameters(self):
        for key in ("ReceiverOrigin", "StopAtUTC"):
            self.assertNotIn("Default", self.t["Parameters"][key])
        self.assertEqual(self.r["StopSchedule"]["Properties"]["ScheduleExpression"], {"Fn::Sub": "at(${StopAtUTC})"})
        self.assertNotIn("review-only.trycloudflare", json.dumps(self.t))
        self.assertNotIn("2099-01-01", json.dumps(self.t))

    def test_reject_production_namespace(self):
        with self.assertRaises(ValueError):
            split_template("nexusapi-prod-cluster")


if __name__ == "__main__":
    unittest.main()

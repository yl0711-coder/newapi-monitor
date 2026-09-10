import copy
import unittest
import os
import select
import subprocess

from ecs_acceptance_fault import definition_for_fault, FAULT_COMMAND
from ecs_acceptance_run import ACCOUNT, REGION


class FaultDefinitionTest(unittest.TestCase):
    def setUp(self):
        self.plan = {"name": "monitor-ecs-acceptance-20260909-1522"}
        self.repository = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{self.plan['name']}"
        self.outputs = {"Repository": self.repository}
        self.definition = {
            "family": self.plan["name"], "taskRoleArn": f"arn:aws:iam::{ACCOUNT}:role/{self.plan['name']}-taskrole",
            "cpu": "512", "memory": "1024", "networkMode": "awsvpc",
            "taskDefinitionArn": "read-only-response-field",
            "containerDefinitions": [{"name": name, "image": self.repository + ":fixture", "environment": [{"name": "ECSLOG_SCOPE", "value": "isolated"}]}
                                     for name in ["init", "synthetic", "nginxcollector", "rejectcollector"]],
        }

    def test_changes_only_synthetic_start_wrapper(self):
        before = copy.deepcopy(self.definition)
        result = definition_for_fault(self.definition, self.plan, self.outputs)
        self.assertEqual(self.definition, before)
        self.assertNotIn("taskDefinitionArn", result)
        for original, changed in zip(before["containerDefinitions"], result["containerDefinitions"]):
            if original["name"] == "synthetic":
                self.assertEqual(changed["image"], original["image"])
                self.assertEqual(changed["environment"], original["environment"])
                self.assertEqual(changed["entryPoint"], ["/bin/sh", "-c"])
                self.assertEqual(changed["command"], [FAULT_COMMAND])
            else:
                self.assertEqual(changed, original)

    def test_scope_and_size_mismatch_rejected(self):
        for key, value in [("family", "production"), ("taskRoleArn", "production"),
                           ("cpu", "1024"), ("memory", "2048"), ("networkMode", "host")]:
            with self.subTest(key=key):
                candidate = dict(self.definition, **{key: value})
                with self.assertRaises(RuntimeError):
                    definition_for_fault(candidate, self.plan, self.outputs)

    def test_foreign_image_and_existing_fault_rejected(self):
        for foreign in [True, False]:
            candidate = copy.deepcopy(self.definition)
            producer = candidate["containerDefinitions"][1]
            if foreign:
                producer["image"] = "production:latest"
            else:
                producer["command"] = ["already configured"]
            with self.assertRaises(RuntimeError):
                definition_for_fault(candidate, self.plan, self.outputs)

    def test_wrapper_stops_own_child_and_returns_failure(self):
        command = FAULT_COMMAND.replace("/usr/local/bin/synthetic &", "/bin/sh -c 'echo ready; exec sleep 30' &")
        process = subprocess.Popen(["/bin/sh", "-c", command], env=dict(os.environ, ECSLOG_SCOPE="isolated"),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 3)[0], "synthetic child did not start")
            self.assertEqual(process.stdout.readline().strip(), "ready")
            process.terminate()
            process.communicate(timeout=3)
            self.assertEqual(process.returncode, 1)
        finally:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=3)

    def test_wrapper_refuses_nonisolated_scope(self):
        result = subprocess.run(["/bin/sh", "-c", FAULT_COMMAND], env=dict(os.environ, ECSLOG_SCOPE="production"),
                                capture_output=True, timeout=3)
        self.assertEqual(result.returncode, 64)


if __name__ == "__main__":
    unittest.main()

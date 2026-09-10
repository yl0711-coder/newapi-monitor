import json
from pathlib import Path
import tempfile
import unittest
from ecs_acceptance_roll import ready_service
from ecs_acceptance_run import prepare, receiver_credential_process
import shlex


class ConcurrencyGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.service = {"desiredCount": 1, "runningCount": 1, "pendingCount": 0,
                        "deploymentConfiguration": {"maximumPercent": 100, "minimumHealthyPercent": 0},
                        "deployments": [{"rolloutState": "COMPLETED"}]}
        self.tasks = [{"taskArn": "test-task", "lastStatus": "RUNNING"}]

    def aws(self, _, operation, data):
        if operation == "describe-services":
            return {"services": [self.service]}
        if operation == "list-tasks":
            return {"taskArns": [t["taskArn"] for t in self.tasks]}
        if operation == "describe-tasks":
            return {"tasks": self.tasks}
        self.fail("unexpected mutation: " + operation)

    def ready(self, require_one=True):
        return ready_service(self.root, {"name": "test"}, self.aws, "synthetic", require_one)

    def test_settled_one_ready(self):
        self.assertEqual(self.ready()["desiredCount"], 1)

    def test_stopping_task_blocks_new_capacity(self):
        self.tasks.append({"taskArn": "old", "lastStatus": "STOPPING"})
        with self.assertRaisesRegex(RuntimeError, "still stopping"):
            self.ready(False)

    def test_rolling_mode_blocks_scale(self):
        self.service["deploymentConfiguration"]["maximumPercent"] = 200
        with self.assertRaisesRegex(RuntimeError, "bounded deployment"):
            self.ready(False)

    def test_two_cannot_start_roll(self):
        self.service.update(desiredCount=2, runningCount=2)
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            self.ready()

    def test_unfinished_deployment_blocks(self):
        self.service["deployments"][0]["rolloutState"] = "IN_PROGRESS"
        with self.assertRaisesRegex(RuntimeError, "deployment not settled"):
            self.ready(False)

    def test_cannot_extend_deadline(self):
        with self.assertRaisesRegex(RuntimeError, "cannot extend"):
            prepare(self.root, {"deadline": "already-set"}, "https://fixture.trycloudflare.com")

    def test_receiver_refresh_uses_original_login_profile_and_region(self):
        args = shlex.split(receiver_credential_process())
        removed = {args[i + 1] for i, value in enumerate(args) if value == "-u"}
        self.assertTrue({"AWS_CONFIG_FILE", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
                         "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} <= removed)
        self.assertEqual(args[-4:], ["--profile", "default", "--format", "process"])


if __name__ == "__main__":
    unittest.main()

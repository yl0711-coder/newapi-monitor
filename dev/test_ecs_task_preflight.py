"""Local JSON fixtures only, no AWS, Docker, secrets or deployment calls."""
import contextlib
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path

from ecs_task_preflight import check_plan, main
from test_ecs_task_contract import INIT, PAIRS, fixture


class TaskPreflightTest(unittest.TestCase):
    def setUp(self):
        self.after = fixture()
        self.before = copy.deepcopy(self.after)
        for c in self.before["ContainerDefinitions"]:
            if c["Name"] in PAIRS.values():
                c["DependsOn"] = [d for d in c["DependsOn"] if d["ContainerName"] not in PAIRS]

    def run_cli(self, before, after, *args):
        with tempfile.TemporaryDirectory() as folder:
            baseline, proposal = Path(folder) / "before.json", Path(folder) / "after.json"
            baseline.write_text(before)
            proposal.write_text(after)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = main(["--baseline", str(baseline), "--proposal", str(proposal), *args])
            self.assertEqual(baseline.read_text(), before)
            self.assertEqual(proposal.read_text(), after)
            return code, json.loads(output.getvalue())

    def test_valid_report_never_authorizes_deployment(self):
        report = check_plan(self.before, self.after)
        self.assertTrue(report["static_checks_passed"])
        for key in ["production_ready", "deployment_authorized", "authorization_verified"]:
            self.assertFalse(report[key])
        self.assertEqual(report["contract_scope"], "isolated")
        self.assertNotEqual(report["baseline_sha256"], report["proposal_sha256"])

    def test_cli_is_read_only_and_does_not_print_configuration(self):
        secret = "synthetic-secret-must-not-appear"
        for task in [self.before, self.after]:
            next(c for c in task["ContainerDefinitions"] if c["Name"] == "new-api")["Environment"] = [{"Name": "SECRET", "Value": secret}]
        code, output = self.run_cli(json.dumps(self.before), json.dumps(self.after))
        self.assertEqual(code, 0)
        self.assertNotIn(secret, json.dumps(output))
        self.after["ContainerDefinitions"].append({"Name": secret})
        code, output = self.run_cli(json.dumps(self.before), json.dumps(self.after))
        self.assertEqual(code, 1)
        self.assertNotIn(secret, json.dumps(output))

    def test_duplicate_keys_malformed_and_wrong_schema_fail_closed(self):
        for value in ['{"secret":"x","secret":"y"}', '[]', 'null', '"private-input"',
                      '{"containerDefinitions":[]}', '{"ContainerDefinitions":[]}', '{"broken":']:
            with self.subTest(value=value):
                code, output = self.run_cli(value, json.dumps(self.after))
                self.assertEqual(code, 1)
                self.assertFalse(output["static_checks_passed"])

    def test_production_scope_is_not_opened(self):
        collector = next(c for c in self.after["ContainerDefinitions"] if c["Name"] in PAIRS)
        next(e for e in collector["Environment"] if e["Name"] == "ECSLOG_SCOPE")["Value"] = "production"
        with self.assertRaisesRegex(ValueError, "contract mismatch"):
            check_plan(self.before, self.after)


if __name__ == "__main__":
    unittest.main()

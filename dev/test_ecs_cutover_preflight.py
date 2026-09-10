"""Synthetic offline transition tests; no AWS, Docker, or live database."""
import contextlib
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ecs_cutover_preflight as gate


def task_rows(number, collector="legacy"):
    return [{
        "task_arn": f"arn:aws:ecs:us-west-2:123456789012:task/fixture/{number:032x}",
        "service_arn": "arn:aws:ecs:us-west-2:123456789012:service/fixture/worker",
        "container": container, "lane": lane,
        "runtime_id": f"runtime-{number}-{container}" if collector == "ecs" else "",
        "collector": collector,
        "audience": "fixture-new" if collector == "ecs" else "fixture-old",
    } for lane, container in gate.LANES.items()]


def fixture(action="advance"):
    # An existing ECS task must survive rollback unchanged, alongside old tasks.
    baseline = {"version": 1, "assignments": task_rows(1) + task_rows(2) + task_rows(3, "ecs")}
    proposed = {"version": 1, "action": action, "baseline_sha256": gate.manifest_digest(baseline),
                "assignments": copy.deepcopy(baseline["assignments"]) + task_rows(4, "ecs" if action == "advance" else "legacy")}
    return baseline, proposed


class CutoverPreflightTests(unittest.TestCase):
    def test_advance_retains_history_but_does_not_approve_production(self):
        before, after = fixture()
        original = copy.deepcopy((before, after))
        result = gate.check_transition(before, after)
        self.assertTrue(result["static_checks_passed"])
        self.assertFalse(result["production_ready"])
        self.assertFalse(result["authorization_verified"])
        self.assertEqual((result["retained_tasks"], result["new_tasks"]), (3, 1))
        self.assertEqual((result["retained_lanes"], result["new_lanes"]), (12, 4))
        self.assertEqual((before, after), original)

    def test_rollback_changes_only_future_tasks(self):
        before, after = fixture("rollback")
        result = gate.check_transition(before, after)
        self.assertTrue(result["static_checks_passed"])
        self.assertEqual(after["assignments"][8:12], before["assignments"][8:12])
        # Returning the ALREADY accepted ECS task to legacy would reread files.
        after["assignments"][8:12] = task_rows(3)
        with self.assertRaisesRegex(gate.InvalidPlan, "existing task responsibility"):
            gate.check_transition(before, after)

    def test_cannot_remove_stopped_tasks_to_hide_replay_responsibility(self):
        before, after = fixture()
        after["assignments"] = after["assignments"][4:]
        with self.assertRaisesRegex(gate.InvalidPlan, "existing task responsibility"):
            gate.check_transition(before, after)

    def test_existing_identity_is_immutable(self):
        for field, value in {"audience": "other", "runtime_id": "replacement", "collector": "ecs"}.items():
            with self.subTest(field=field):
                before, after = fixture()
                for row in after["assignments"][:4]:
                    row[field] = value
                    if field == "collector":
                        row["runtime_id"] = "replacement"
                with self.assertRaises(gate.InvalidPlan):
                    gate.check_transition(before, after)

    def test_missing_duplicate_and_mixed_lanes_fail(self):
        for mutate in [lambda rows: rows.pop(), lambda rows: rows.append(copy.deepcopy(rows[-1])),
                       lambda rows: rows[-1].update(container="nginx"),
                       lambda rows: rows[-1].update(collector="legacy"),
                       lambda rows: rows[-2].update(runtime_id="different-runtime")]:
            with self.subTest(mutate=mutate):
                before, after = fixture()
                mutate(after["assignments"])
                with self.assertRaises(gate.InvalidPlan):
                    gate.check_transition(before, after)

    def test_unknown_fields_and_shadow_cannot_be_promoted(self):
        for field, value in [("collector", "shadow"), ("approved", True), ("cost", 100)]:
            with self.subTest(field=field):
                before, after = fixture()
                after["assignments"][-1][field] = value
                with self.assertRaises(gate.InvalidPlan):
                    gate.check_transition(before, after)

    def test_scope_cannot_expand(self):
        for substitution in ["other", "us-east-1", "999999999999"]:
            with self.subTest(substitution=substitution):
                before, after = fixture()
                for row in after["assignments"][-4:]:
                    if substitution == "other":
                        row["service_arn"] += "-other"
                    else:
                        old = "us-west-2" if substitution == "us-east-1" else "123456789012"
                        row["task_arn"] = row["task_arn"].replace(old, substitution)
                        row["service_arn"] = row["service_arn"].replace(old, substitution)
                with self.assertRaisesRegex(gate.InvalidPlan, "unreviewed service"):
                    gate.check_transition(before, after)

    def test_audience_isolated_and_rollback_known(self):
        for action, audience in [("advance", "fixture-old"), ("rollback", "unknown")]:
            with self.subTest(action=action):
                before, after = fixture(action)
                for row in after["assignments"][-4:]:
                    row["audience"] = audience
                with self.assertRaises(gate.InvalidPlan):
                    gate.check_transition(before, after)

    def test_baseline_change_invalidates_plan(self):
        before, after = fixture()
        before["assignments"] += task_rows(5)
        with self.assertRaisesRegex(gate.InvalidPlan, "baseline changed"):
            gate.check_transition(before, after)

    def test_rollback_cannot_borrow_another_services_receiver(self):
        before, after = fixture("rollback")
        others = task_rows(5)
        for row in others:
            row["service_arn"] += "-other"
            row["audience"] = "other-service-receiver"
        before["assignments"] += copy.deepcopy(others)
        after["assignments"] += others
        after["baseline_sha256"] = gate.manifest_digest(before)
        for row in after["assignments"][12:16]:
            row["audience"] = "other-service-receiver"
        with self.assertRaisesRegex(gate.InvalidPlan, "for this service"):
            gate.check_transition(before, after)

    def test_same_task_cannot_be_reused_even_with_new_runtime(self):
        before, after = fixture()
        after["assignments"][-4:] = task_rows(1, "ecs")
        with self.assertRaisesRegex(gate.InvalidPlan, "duplicate"):
            gate.check_transition(before, after)

    def test_noop_and_wrong_generation_fail(self):
        before, after = fixture()
        after["assignments"] = copy.deepcopy(before["assignments"])
        with self.assertRaisesRegex(gate.InvalidPlan, "no new task"):
            gate.check_transition(before, after)
        after["assignments"] += task_rows(4)
        with self.assertRaisesRegex(gate.InvalidPlan, "wrong generation"):
            gate.check_transition(before, after)

    def test_incomplete_or_empty_baseline_never_passes(self):
        for rows in [[], task_rows(1)[:3]]:
            before, after = fixture()
            before["assignments"] = rows
            after["baseline_sha256"] = gate.manifest_digest(before)
            with self.assertRaises(gate.InvalidPlan):
                gate.check_transition(before, after)

    def test_types_are_strict_and_errors_do_not_dump_input(self):
        for field, value in [("version", True), ("action", []), ("assignments", {})]:
            before, after = fixture()
            after[field] = value
            with self.assertRaises(gate.InvalidPlan):
                gate.check_transition(before, after)
        before, after = fixture()
        after["assignments"][-1]["audience"] = "private-example-value\n"
        with self.assertRaises(gate.InvalidPlan) as err:
            gate.check_transition(before, after)
        self.assertNotIn("private-example-value", str(err.exception))

    def test_cli_read_only_and_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ["baseline.json", "proposal.json"]]
            for path, data in zip(paths, fixture()):
                path.write_text(json.dumps(data))
            original = [path.read_bytes() for path in paths]
            args = ["--baseline", str(paths[0]), "--proposal", str(paths[1])]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(gate.main(args), 0)
            self.assertFalse(json.loads(output.getvalue())["production_ready"])
            self.assertEqual([path.read_bytes() for path in paths], original)
            self.assertEqual(len(list(Path(directory).iterdir())), 2)
            paths[1].write_text('{"version":1,"version":1}')
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(gate.main(args), 1)
            with patch.object(gate, "MAX_BYTES", 4):
                with self.assertRaisesRegex(gate.InvalidPlan, "size limit"):
                    gate.load_manifest(paths[0])


if __name__ == "__main__":
    unittest.main()

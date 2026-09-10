"""Orchestrator safety controls; all Docker calls are mocked in these tests."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ecs_image_acceptance as image


class ImageRunnerSafetyTests(unittest.TestCase):
    def test_remote_context_rejected_before_any_image_or_container_operation(self):
        with patch.object(image, "docker", return_value="tcp://example.invalid:2376") as docker:
            with self.assertRaisesRegex(RuntimeError, "local OrbStack"):
                image.local_images()
            self.assertEqual(docker.call_count, 1)

    def test_collector_root_image_is_rejected(self):
        calls = ["unix:///Users/fixture/.orbstack/run/docker.sock",
                 json.dumps([{"Architecture": "amd64", "Os": "linux", "Config": {"User": "root"}}])]
        with patch.object(image, "docker", side_effect=calls):
            with self.assertRaisesRegex(RuntimeError, "non-root"):
                image.local_images()

    def test_fixture_overrides_inherited_volumes_and_tracks_container_before_start(self):
        with tempfile.TemporaryDirectory() as directory:
            run = image.LocalRun(Path(directory), "normal", {"fixture": "fixture-id"})
            cid = "a" * 64
            with patch.object(image, "docker", side_effect=[cid, json.dumps([{"Mounts": []}]), cid]) as docker:
                run.start("receiver", "fixture-id", ["--network", "none"])
            args = docker.call_args_list[0].args
            self.assertIn("/data:rw,size=16m", args)
            self.assertIn("/backup:rw,size=16m", args)
            self.assertIn("/evidence:rw,size=16m", args)
            self.assertIn("--read-only", args)
            self.assertIn("never", args)
            self.assertEqual(json.loads((run.root / "containers.json").read_text()), {"receiver": cid})

    def test_unexpected_mount_is_rejected_before_container_start(self):
        with tempfile.TemporaryDirectory() as directory:
            run = image.LocalRun(Path(directory), "normal", {"fixture": "fixture-id"})
            answers = ["a" * 64, json.dumps([{"Mounts": [{"Type": "bind", "Source": "/fixture-unapproved"}]}])]
            with patch.object(image, "docker", side_effect=answers) as docker:
                with self.assertRaisesRegex(RuntimeError, "unexpected host bind"):
                    run.start("receiver", "fixture-id", ["--network", "none"])
            self.assertEqual(docker.call_count, 2)
            self.assertIn("receiver", run.containers)  # tracked for safe cleanup

    def test_cleanup_rejects_unrelated_container(self):
        with tempfile.TemporaryDirectory() as directory:
            run = image.LocalRun(Path(directory), "normal", {})
            run.containers["receiver"] = "a" * 64
            with patch.object(image, "docker", return_value=json.dumps([{"Config": {"Labels": {image.LABEL: "other-run"}}}])) as docker:
                with self.assertRaisesRegex(RuntimeError, "unrelated"):
                    run.remove_container("receiver")
            self.assertEqual(docker.call_count, 1)

    def test_cleanup_removes_only_tracked_container_and_its_anonymous_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            run = image.LocalRun(Path(directory), "normal", {})
            cid = "a" * 64
            run.containers["receiver"] = cid
            answers = [json.dumps([{"Config": {"Labels": {image.LABEL: run.name}}}]), "fixture logs", cid]
            with patch.object(image, "docker", side_effect=answers) as docker:
                run.remove_container("receiver")
            self.assertEqual(docker.call_args_list[-1].args, ("rm", "-f", "-v", cid))
            self.assertEqual(run.containers, {})


if __name__ == "__main__":
    unittest.main()

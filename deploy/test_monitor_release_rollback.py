import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest

import monitor_release_rollback as rollback


class RollbackReferenceTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        self.backup = root / "backup"
        self.live = root / "data"
        self.backup.mkdir()
        self.live.mkdir()
        self.store_paths = ("/data/nexus_monitor.db", "/data/usage-facts.db")
        digest = hashlib.sha256(("main-facts\0" + "\0".join(self.store_paths)).encode()).hexdigest()[:32]
        self.reference = self.backup / (".pre-migration-plan-" + digest + ".json")
        self.pinned = self.backup / "pre-migrate-correct"
        self.pinned.mkdir()
        (self.backup / "pre-migrate-newest").mkdir()
        raw = json.dumps({"format_version": 1, "migration_plan": "plan", "stores": [
            {"present": True, "role": "main", "source_file": "nexus_monitor.db", "snapshot_file": "monitor.db"},
            {"present": True, "role": "facts", "source_file": "usage-facts.db", "snapshot_file": "usage-facts.db"},
        ]}).encode()
        (self.pinned / "manifest.json").write_bytes(raw)
        hash_value = hashlib.sha256(raw).hexdigest()
        (self.pinned / "READY").write_text("sha256:" + hash_value + "\n")
        self.reference.write_text(json.dumps({"format_version": 1, "migration_plan": "plan",
                                              "snapshot_dir": self.pinned.name, "manifest_sha256": hash_value}))

    def resolve(self, paths=None):
        return rollback.pinned_rollback_paths(
            backup_root=self.backup, data_root=self.live, store_paths=self.store_paths if paths is None else paths)

    def test_resolves_go_reference_not_newest_directory(self):
        pinned, live = self.resolve()
        self.assertEqual(pinned, (self.pinned / "monitor.db", self.pinned / "usage-facts.db"))
        self.assertEqual(live, (self.live / "nexus_monitor.db", self.live / "usage-facts.db"))

    def test_rejects_wrong_or_symlink_reference(self):
        wrong = ("/data/../credentials.db", self.store_paths[-1])
        with self.assertRaisesRegex(ValueError, "outside"):
            self.resolve(wrong)
        original = self.reference.read_bytes()
        self.reference.unlink()
        target = self.backup / "reference.json"
        target.write_bytes(original)
        self.reference.symlink_to(target)
        with self.assertRaises(OSError):
            self.resolve()

    def test_modified_manifest_skips_optional_helper(self):
        (self.pinned / "manifest.json").write_text("{}")
        def not_called(**kwargs):
            self.fail("read-ahead started on unverifiable metadata")
        result = rollback._read_ahead_stopped_mounts(backup_root=self.backup, data_root=self.live,
                                                    store_paths=self.store_paths, seconds=30, read_ahead=not_called)
        self.assertEqual(result, {"status": "skipped", "reason": "pinned_reference_unavailable"})

    def test_reference_is_resolved_by_the_bounded_child(self):
        self.reference.unlink()
        result = rollback.warm_stopped_mounts(backup_root=self.backup, data_root=self.live,
                                             store_paths=self.store_paths, deadline=time.monotonic()+90)
        self.assertEqual((result["status"], result["reason"]), ("skipped", "pinned_reference_unavailable"))

    def test_reference_is_not_opened_when_time_is_reserved_for_old_image(self):
        result = rollback.warm_stopped_mounts(backup_root=self.backup, data_root=self.live,
                                             store_paths=self.store_paths, deadline=time.monotonic()+10)
        self.assertEqual((result["status"], result["reason"]), ("skipped", "rollback_time_reserved"))


class ComposeRollbackTest(unittest.TestCase):
    active = "registry/test@sha256:" + "b" * 64
    old = "registry/test@sha256:" + "a" * 64
    prefix = ["docker", "compose", "-p", "test-monitor"]
    deadline = 300

    def setUp(self):
        self.now = 0
        self.history = []
        self.current = self.container(self.active)
        self.other_writer = False
        self.refuse_stop = False
        self.warm_mode = "complete"
        self.stop_delay = 5
        self.start_delay = 6
        self.probe_delays = []
        self.probe_healthy = []

    def advance(self, seconds):
        self.now += seconds

    def container(self, digest, running=True):
        return {"Name": "/test-monitor", "State": {"Running": running},
                "Config": {"Image": digest, "Labels": {"com.docker.compose.project": "test-monitor",
                                                        "com.docker.compose.service": "monitor"}}}

    def inspect(self, timeout):
        self.assertGreater(timeout, 0)
        return self.current

    def verify(self, timeout):
        self.history.append(("verify", timeout))
        if self.other_writer:
            raise RuntimeError("other writer exists")
        self.advance(2)

    def warm(self, **kwargs):
        self.history.append(("warm", self.now, kwargs["deadline"]))
        if self.warm_mode == "timeout":
            self.now = kwargs["deadline"]
        elif self.warm_mode == "exception":
            raise OSError("fake I/O failure")
        self.advance(20)
        return {"status": "completed"}

    def checked(self, argv, name, *, timeout):
        self.history.append((argv, self.now, timeout))
        if name == "stop candidate monitor":
            self.advance(self.stop_delay)
            if not self.refuse_stop:
                self.current = self.container(self.active, running=False)
        else:
            self.advance(self.start_delay)
            self.current = self.container(self.old)

    def probe(self, deadline):
        self.history.append(("probe", self.now, deadline))
        if self.probe_delays:
            self.advance(self.probe_delays.pop(0))
        if self.probe_healthy and not self.probe_healthy.pop(0):
            return None
        return {"monitor": True, "usage": True, "stores": True}

    def run_phase(self, **kwargs):
        fields = dict(active_compose=self.prefix + ["-f", "new.yml"], old_compose=self.prefix + ["-f", "old.yml"],
                      project="test-monitor", container_name="test-monitor", active_digest=self.active,
                      old_digest=self.old, inspect_container=self.inspect, assert_no_other_writer=self.verify,
                      checked=self.checked, probe_ready=self.probe, data_root="/test/data", backup_root="/test/backup",
                      store_paths=("/data/main.db", "/data/facts.db"), stable_seconds=4,
                      clock=lambda: self.now, sleep=self.advance, warmer=self.warm)
        fields.update(kwargs)
        return rollback.rollback_compose_monitor(**fields)

    def test_exact_service_stop_warm_start_and_one_deadline(self):
        result = self.run_phase()
        self.assertEqual(result["elapsed_ms"], 37000)
        self.assertEqual(self.history[0][0], self.prefix + ["-f", "new.yml", "stop", "monitor"])
        self.assertEqual(self.history[2], ("warm", 7, 300))
        self.assertEqual(self.history[3][0],
                         self.prefix + ["-f", "old.yml", "up", "-d", "--no-deps", "--pull", "never", "monitor"])
        self.assertTrue(all(item[2] == 300 for item in self.history[4:]))

    def test_second_writer_or_unstopped_candidate_blocks_warm_and_start(self):
        for stop_failed, other_writer in ((True, False), (False, True)):
            with self.subTest(stop_failed=stop_failed, other_writer=other_writer):
                self.current, self.history = self.container(self.active), []
                self.refuse_stop = stop_failed
                self.other_writer = other_writer
                with self.assertRaises(RuntimeError):
                    self.run_phase()
                self.assertFalse(any(item[0] == "warm" for item in self.history))
                self.assertEqual(sum(isinstance(item[0], list) for item in self.history), 1)

    def test_unexpected_or_running_old_container_is_not_stopped(self):
        self.current = self.container(self.old)
        with self.assertRaisesRegex(RuntimeError, "identity"):
            self.run_phase()
        self.assertFalse(self.history)

    def test_cold_backup_failure_can_resume_a_known_stopped_old_image(self):
        self.current = self.container(self.old, running=False)
        result = self.run_phase()
        self.assertEqual(result["elapsed_ms"], 32000)
        self.assertEqual(self.history[0][0], "verify")

    def test_optional_io_failure_does_not_block_previous_image(self):
        self.warm_mode = "exception"
        self.assertEqual(self.run_phase()["read_ahead"]["reason"], "OSError")
        self.assertEqual(self.current["Config"]["Image"], self.old)

    def test_no_second_window_on_timeout_inside_warm_or_probe(self):
        self.warm_mode = "timeout"
        with self.assertRaisesRegex(TimeoutError, "old image start"):
            self.run_phase()
        self.assertEqual(self.current["Config"]["Image"], self.active)
        self.warm_mode, self.now = "complete", 0
        self.current = self.container(self.active)
        self.probe_delays = [267]
        with self.assertRaisesRegex(TimeoutError, "readiness result"):
            self.run_phase()

    def test_flapping_resets_old_readiness_stability(self):
        self.probe_healthy = [True, False, True, True, True]
        self.assertEqual(self.run_phase()["elapsed_ms"], 41000)

    def test_rejects_cross_project_and_tagged_image_before_mutations(self):
        for opts in ({"old_compose": ["docker", "compose", "-p", "other-project"]},
                     {"old_digest": "registry/test:latest"}):
            with self.subTest(opts=opts), self.assertRaises(ValueError):
                self.run_phase(**opts)
        self.assertEqual(self.history, [])

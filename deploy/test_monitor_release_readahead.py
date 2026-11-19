import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import monitor_release_readahead as readahead


class ReleaseReadAheadTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.pinned = tuple(self.root / s for s in ("pinned-main", "pinned-facts"))
        self.live = tuple(self.root / s for s in ("live-main", "live-facts"))
        for i, path in enumerate((*self.pinned, *self.live), start=1):
            path.write_bytes(bytes([i]) * 32)

    def warm(self, **kwargs):
        opts = {"pinned": self.pinned, "live": self.live,
                "memory_available": lambda: 2048 * readahead.MIB}
        return readahead.read_ahead_sqlite_pair(**(opts | kwargs))

    def test_reads_both_pairs_and_existing_wal_without_modifying_any(self):
        wal = self.live[-1].with_name("live-facts-wal")
        wal.write_bytes(b"wal")
        paths = [*self.pinned, *self.live, wal]
        before = {p: (p.read_bytes(), readahead.fingerprint(p.stat())) for p in paths}
        result = self.warm()
        self.assertEqual((result["status"], result["bytes_read"]), ("completed", 131))
        self.assertEqual({p: (p.read_bytes(), readahead.fingerprint(p.stat())) for p in paths}, before)
        self.assertEqual(set(self.root.iterdir()), set(paths))

    def test_insufficient_memory_or_byte_budget_reads_nothing(self):
        for opts, reason in (({"memory_available": lambda: 640 * readahead.MIB}, "host_memory_budget"),
                             ({"max_bytes": 63}, "byte_budget")):
            with self.subTest(reason=reason), patch.object(readahead.os, "read") as read:
                result = self.warm(**opts)
                self.assertEqual((result["status"], result["reason"]), ("skipped", reason))
                read.assert_not_called()

    def test_smaller_plan_reads_only_live_pair_and_wal(self):
        wal = self.live[-1].with_name("live-facts-wal")
        wal.write_bytes(b"wal")
        for opts in ({"max_bytes": 95},
                     {"memory_available": lambda: 640 * readahead.MIB + 70}):
            with self.subTest(opts=opts):
                result = self.warm(**opts)
                self.assertEqual((result["status"], result["scope"], result["bytes_read"]),
                                 ("completed", "live_only", 67))

    def test_data_bearing_pinned_sidecar_is_not_a_warming_plan(self):
        self.pinned[-1].with_name("pinned-facts-wal").write_bytes(b"unexpected")
        self.assertEqual(self.warm()["reason"], "sealed_sidecar_has_data")

    def test_noncanonical_missing_and_linked_files_are_not_read(self):
        first, second = self.pinned
        for target in (self.root / "missing", self.root / "link"):
            if target.name == "link":
                target.symlink_to(first)
            result = self.warm(pinned=(target, second))
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["bytes_read"], 0)
        os.link(first, self.root / "hardlink")
        self.assertEqual(self.warm()["reason"], "not_an_independent_regular_file")

    def test_duplicate_and_nonregular_paths_are_not_read(self):
        self.assertEqual(self.warm(live=self.pinned)["reason"], "duplicate_store")
        self.assertEqual(self.warm(live=(self.root, self.live[-1]))["reason"],
                         "not_an_independent_regular_file")

    def test_budget_expiry_is_reported_as_an_interruption(self):
        future = iter((0, 0, 31, 31))
        result = self.warm(clock=lambda: next(future))
        self.assertEqual((result["status"], result["reason"], result["bytes_read"]),
                         ("interrupted", "time_budget", 32))

    def test_memory_pressure_after_first_file_is_reported(self):
        available = iter((2048 * readahead.MIB, 2048 * readahead.MIB, 511 * readahead.MIB))
        result = self.warm(memory_available=lambda: next(available))
        self.assertEqual((result["status"], result["reason"], result["bytes_read"]),
                         ("interrupted", "host_memory_budget", 32))

    def test_memory_pressure_stops_before_next_chunk_in_same_file(self):
        with self.pinned[0].open("r+b") as f:
            f.truncate(2 * readahead.MIB)
        available = iter((2048 * readahead.MIB, 2048 * readahead.MIB, 511 * readahead.MIB))
        result = self.warm(memory_available=lambda: next(available))
        self.assertEqual((result["status"], result["reason"], result["bytes_read"]),
                         ("interrupted", "host_memory_budget", readahead.MIB))

    def test_memory_pressure_after_last_chunk_is_not_completed(self):
        available = iter([2048 * readahead.MIB] * 5 + [511 * readahead.MIB])
        result = self.warm(memory_available=lambda: next(available))
        self.assertEqual((result["status"], result["reason"], result["bytes_read"]),
                         ("interrupted", "host_memory_budget", 128))

    def test_new_wal_during_read_is_not_reported_completed(self):
        new_wal = self.live[0].with_name("live-main-wal")
        actual_read = os.read
        def add_wal_on_first_read(fd, n):
            if not new_wal.exists():
                new_wal.write_bytes(b"layout changed")
            return actual_read(fd, n)
        with patch.object(readahead.os, "read", side_effect=add_wal_on_first_read):
            result = self.warm()
        self.assertEqual((result["status"], result["reason"]), ("interrupted", "file_changed"))

    def test_changed_file_is_not_reported_completed(self):
        actual_read = os.read
        def mutate_after_first_read(fd, n):
            block = actual_read(fd, n)
            if self.pinned[0].stat().st_size == 32:
                self.pinned[0].write_bytes(b"changed")
            return block
        with patch.object(readahead.os, "read", side_effect=mutate_after_first_read):
            self.assertEqual(self.warm()["reason"], "file_changed")

    def test_os_read_error_does_not_change_files(self):
        with patch.object(readahead.os, "read", side_effect=OSError):
            result = self.warm()
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(self.pinned[0].read_bytes(), bytes([1]) * 32)

    def test_watchdog_reserves_time_in_shared_rollback_window(self):
        with patch.object(readahead.subprocess, "Popen") as start:
            result = readahead.run_bounded_read_ahead(
                pinned=self.pinned, live=self.live, rollback_deadline=time.monotonic()+25)
        self.assertEqual((result["status"], result["reason"]), ("skipped", "rollback_time_reserved"))
        start.assert_not_called()

    def test_watchdog_kills_stuck_child_using_remaining_shared_time(self):
        script = self.root / "slow.py"
        script.write_text("import time\ntime.sleep(15)\n")
        started = time.monotonic()
        result = readahead.run_bounded_read_ahead(
            pinned=self.pinned, live=self.live, rollback_deadline=started+.2,
            startup_reserve_seconds=0, script=script)
        self.assertEqual((result["status"], result["reason"]), ("interrupted", "watchdog_timeout"))
        self.assertLess(time.monotonic()-started, 2)

    def test_watchdog_reports_invalid_child_result_without_extending_budget(self):
        script = self.root / "unparseable.py"
        for payload in ("not JSON", '{"status":"completed"}'):
            with self.subTest(payload=payload):
                script.write_text(f"print({payload!r})\n")
                result = readahead.run_bounded_read_ahead(
                    pinned=self.pinned, live=self.live, rollback_deadline=time.monotonic()+3,
                    startup_reserve_seconds=0, script=script)
                self.assertEqual((result["status"], result["reason"]), ("interrupted", "helper_failed"))

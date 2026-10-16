import copy
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("retention", Path(__file__).with_name("release_backup_retention.py"))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def can_read_proc():
    if not sys.platform.startswith("linux"):
        return False
    try:
        r.no_open_files([])
        return True
    except (PermissionError, FileNotFoundError):
        return False


PROC_READABLE = can_read_proc()


class ReleaseBackupRehearsal(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / "releases"
        self.root.mkdir()
        self.accept_base = time.time() - 4 * 86400
        self.names = [f"monitor-v1.18.{n}-202610{n:02d}" for n in (11, 12, 13, 14)]
        self.images = {n: "registry/monitor@sha256:" + str(i + 1) * 64 for i, n in enumerate(self.names)}
        for i, name in enumerate(self.names):
            self.release(name, i)
        self.state = [{"Id": "monitor-container", "Name": "/nexusapi-monitor", "RestartCount": 0,
                       "Config": {"Image": self.images[self.names[-1]], "Labels": {"org.opencontainers.image.revision": "abc"}},
                       "State": {"Status": "running", "StartedAt": r.dt.datetime.fromtimestamp(self.accept_base - 1000, r.dt.timezone.utc).isoformat(), "OOMKilled": False, "Health": {"Status": "healthy"}},
                       "Mounts": [{"Source": str(self.root.parent / "live"), "Destination": "/data"}]},
                      {"Id": "eval-container", "Name": "/api-evaluator", "RestartCount": 0,
                       "Config": {"Image": "eval:unchanged"},
                       "State": {"Status": "running", "StartedAt": "2026-09-01T00:00:00Z", "OOMKilled": False, "Health": {"Status": "healthy"}}, "Mounts": []}]

    def release(self, name, i):
        base = self.root / name
        base.mkdir()
        checks = {}
        for relative in (*r.DATABASES, "gift-handoff/usage-facts.db"):
            area, name_part = relative.split("/", 1)
            p = base / "cold-backup" / relative
            p.parent.mkdir(parents=True, exist_ok=True)
            with contextlib.closing(sqlite3.connect(p)) as conn:
                conn.execute("CREATE TABLE amounts (value INTEGER)")
                conn.execute("INSERT INTO amounts VALUES (123456)")
                conn.commit()
            checks["/" + area] = checks.get("/" + area, {"source_hashes": {}, "sqlite_checked_as_uid_1000": []})
            checks["/" + area]["source_hashes"][name_part] = r.file_hash(p)
            checks["/" + area]["sqlite_checked_as_uid_1000"].append(name_part)
        (base / "backup-checks.json").write_text(json.dumps(checks))
        sample = {"image": self.images[name], "revision": "abc", "checked_at": self.accept_base + i * 86400,
                  "health": "healthy", "reports_ok": True, "state": "running", "oom": False,
                  "restarts": 0, "ready": {"status": "ready"}}
        rows = [{"sample": sample, "decision": {"action": action}, "same_mounts": True, "other_services_unchanged": True} for action in ("WAIT", "READY")]
        (base / "acceptance.json").write_text(json.dumps(rows))
        (base / "config").mkdir()
        (base / "config" / ".env").write_text("synthetic-only-no-secrets")
        (base / "cold-backup" / "data" / "repair-evidence.db").write_bytes(b"never auto-delete")

    def plan(self):
        return r.make_plan(self.root, self.state)

    def apply(self, plan=None, **kwargs):
        return r.apply_plan(self.root, r.digest(plan or self.plan()), state_provider=lambda: self.state,
                            open_guard=kwargs.pop("open_guard", lambda _: None), **kwargs)

    def snapshot(self):
        return {str(p.relative_to(self.root)): r.file_hash(p) for p in self.root.rglob("*")
                if p.is_file() and not any(part.startswith(".retention") or part == ".deployment.lock" for part in p.parts)}

    def test_plan_only_no_writes_then_success_and_restore(self):
        before = self.snapshot()
        plan = self.plan()
        self.assertEqual(before, self.snapshot())
        self.assertEqual(len(plan["files"]), 6)
        self.assertEqual([x["release"] for x in plan["keep"]], list(reversed(self.names[-2:])))
        state_before = copy.deepcopy(self.state)
        result = self.apply(plan)
        self.assertEqual(result["deleted_files"], 6)
        self.assertEqual(result["verified_databases"], 8)
        deleted = {str(Path(x["path"]).relative_to(self.root)) for x in plan["files"]}
        self.assertEqual(self.snapshot(), {k: v for k, v in before.items() if k not in deleted})
        self.assertEqual(self.state, state_before)
        for name in self.names[-2:]:
            for relative in (*r.DATABASES, "gift-handoff/usage-facts.db"):
                src = self.root / name / "cold-backup" / relative
                destination = self.root.parent / (name + "-" + relative.replace("/", "-"))
                destination.write_bytes(src.read_bytes())
                with contextlib.closing(sqlite3.connect(destination)) as conn:
                    self.assertEqual(conn.execute("PRAGMA quick_check").fetchall(), [("ok",)])
                    self.assertEqual(conn.execute("SELECT value FROM amounts").fetchall(), [(123456,)])
        receipt = [json.loads(line) for line in Path(result["receipt"]).read_text().splitlines()]
        self.assertEqual(sum("intent" in row for row in receipt), 6)
        self.assertEqual(sum("deleted" in row for row in receipt), 6)
        self.assertEqual(self.apply()["mode"], "no_changes")

    def test_mounts_including_stopped_and_keep_pin(self):
        self.state[1]["State"]["Status"] = "exited"
        self.state[1]["Mounts"] = [{"Source": str(self.root / self.names[0] / "cold-backup" / "data"), "Destination": "/data"}]
        (self.root / self.names[1] / "KEEP_BACKUP").touch()
        before = self.snapshot()
        plan = self.plan()
        self.assertEqual(plan["files"], [])
        self.assertEqual({p["reason"] for p in plan["protected"]}, {"container_mount", "pinned"})
        self.assertEqual(self.apply(plan)["mode"], "no_changes")
        self.assertEqual(before, self.snapshot())

    def test_failed_acceptance_never_allows_current_release_cleanup(self):
        for field in ("decision", "reports", "ready", "mounts"):
            with self.subTest(field=field):
                path = self.root / self.names[-1] / "acceptance.json"
                original = path.read_text()
                rows = json.loads(original)
                if field == "decision": rows[-1]["decision"]["action"] = "ROLLBACK"
                elif field == "reports": rows[-1]["sample"]["reports_ok"] = False
                elif field == "ready": rows[-1]["sample"]["ready"]["status"] = "degraded"
                else: rows[-1]["same_mounts"] = False
                path.write_text(json.dumps(rows))
                before = self.snapshot()
                with self.assertRaises(RuntimeError): self.plan()
                self.assertEqual(before, self.snapshot())
                path.write_text(original)

    def test_missing_acceptance_is_preserved(self):
        (self.root / self.names[0] / "acceptance.json").unlink()
        plan = self.plan()
        self.assertEqual(len(plan["files"]), 3)
        self.assertIn({"release": self.names[0], "reason": "not_accepted"}, plan["protected"])

    def test_rollback_restart_unhealthy_blocks(self):
        original = copy.deepcopy(self.state)
        for problem in ("rollback", "restart", "health", "oom", "revision"):
            with self.subTest(problem=problem):
                self.state = copy.deepcopy(original)
                if problem == "rollback": self.state[0]["Config"]["Image"] = self.images[self.names[-2]]
                elif problem == "restart": self.state[0]["State"]["StartedAt"] = r.dt.datetime.fromtimestamp(time.time() + 3600, r.dt.timezone.utc).isoformat()
                elif problem == "health": self.state[0]["State"]["Health"]["Status"] = "unhealthy"
                elif problem == "oom": self.state[0]["State"]["OOMKilled"] = True
                else: self.state[0]["Config"]["Labels"]["org.opencontainers.image.revision"] = "different"
                with self.assertRaises(RuntimeError): self.plan()
        self.state = original

    def test_keeper_corruption_blocks_all_deletes(self):
        plan = self.plan()
        p = self.root / self.names[-1] / "cold-backup" / r.DATABASES[0]
        p.write_bytes(b"corrupted")
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "checksum"): self.apply(plan)
        self.assertEqual(before, self.snapshot())

    def test_invalid_sqlite_even_with_matching_hash_blocks(self):
        base = self.root / self.names[-1]
        p = base / "cold-backup" / r.DATABASES[0]
        p.write_bytes(b"not a database")
        checks = json.loads((base / "backup-checks.json").read_text())
        checks["/data"]["source_hashes"]["nexus_monitor.db"] = r.file_hash(p)
        (base / "backup-checks.json").write_text(json.dumps(checks))
        before = self.snapshot()
        with self.assertRaises(sqlite3.DatabaseError): self.apply()
        self.assertEqual(before, self.snapshot())

    def test_plan_change_mount_change_and_open_file_block(self):
        plan = self.plan()
        (self.root / self.names[0] / "KEEP_BACKUP").touch()
        with self.assertRaisesRegex(RuntimeError, "plan changed"): self.apply(plan)
        (self.root / self.names[0] / "KEEP_BACKUP").unlink()
        with patch.object(r, "verify_recovery", side_effect=lambda *args: self.state[1]["Mounts"].append(
                {"Source": str(self.root / self.names[0]), "Destination": "/live"})):
            before = self.snapshot()
            with self.assertRaisesRegex(RuntimeError, "changed during verification"): self.apply()
            self.assertEqual(before, self.snapshot())
        self.state[1]["Mounts"] = []
        before = self.snapshot()
        def deny(_): raise RuntimeError("candidate is open")
        with self.assertRaisesRegex(RuntimeError, "open"): self.apply(open_guard=deny)
        self.assertEqual(before, self.snapshot())

    def test_links_and_sidecars_block(self):
        p = self.root / self.names[0] / "cold-backup" / r.DATABASES[0]
        for suffix in ("-wal", "-shm", "-journal"):
            side = Path(str(p) + suffix); side.touch()
            with self.assertRaisesRegex(RuntimeError, "sidecar"): self.plan()
            side.unlink()
        link = p.with_name("linked")
        os.link(p, link)
        with self.assertRaisesRegex(RuntimeError, "single-link"): self.plan()
        link.unlink()
        p.rename(link); p.symlink_to(link)
        with self.assertRaisesRegex(RuntimeError, "symlink"): self.plan()

    def test_budget_and_control_path_block(self):
        with patch.object(r, "MAX_DELETE_BYTES", 1):
            with self.assertRaisesRegex(RuntimeError, "per-run limit"): self.plan()
        p = self.root / self.names[-1] / "backup-checks.json"
        checks = json.loads(p.read_text())
        checks["/data"]["source_hashes"]["../../outside"] = "0" * 64
        p.write_text(json.dumps(checks))
        with self.assertRaisesRegex(RuntimeError, "unsafe recovery"): self.apply()

    def test_deployment_lock_refuses_concurrent_cleanup(self):
        plan = self.plan()
        with r.deployment_lock(self.root):
            with self.assertRaises(BlockingIOError): self.apply(plan)

    def test_interruption_leaves_two_recoverable_points_and_replans(self):
        before = self.snapshot()
        plan = self.plan()
        calls = 0
        def interrupted(_):
            nonlocal calls
            calls += 1
            if calls == 3: raise RuntimeError("simulated interruption after first unlink")
        with self.assertRaisesRegex(RuntimeError, "interruption"): self.apply(plan, open_guard=interrupted)
        self.assertEqual(len(before) - len(self.snapshot()), 1)
        self.assertEqual(r.verify_recovery(self.root, plan["keep"]), 8)
        with self.assertRaisesRegex(RuntimeError, "plan changed"): self.apply(plan)
        self.assertEqual(self.apply()["deleted_files"], 5)
        self.assertEqual(self.apply()["mode"], "no_changes")

    def test_command_line_plan_is_read_only(self):
        bin_dir = self.root.parent / "bin"
        bin_dir.mkdir()
        state_path = self.root.parent / "synthetic-containers.json"
        state_path.write_text(json.dumps(self.state))
        fake_docker = bin_dir / "docker"
        fake_docker.write_text(f"#!{sys.executable}\nimport json,sys\n"
                               f"state=json.load(open({str(state_path)!r}))\n"
                               "if sys.argv[1:]==['ps','-aq']: print('\\n'.join(c['Id'] for c in state))\n"
                               "elif sys.argv[1]=='inspect': print(json.dumps(state))\n"
                               "else: raise SystemExit('unexpected docker mutation')\n")
        fake_docker.chmod(0o700)
        env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ.get("PATH", "")}
        command = [sys.executable, str(Path(r.__file__)), "--releases-dir", str(self.root)]
        before = self.snapshot()
        out = subprocess.run(command, capture_output=True, text=True, env=env, timeout=30)
        self.assertEqual(out.returncode, 0, out.stderr)
        result = json.loads(out.stdout)
        self.assertEqual(result["mode"], "plan_only")
        self.assertFalse((self.root / ".deployment.lock").exists())
        self.assertFalse((self.root / ".retention-audit").exists())
        self.assertEqual(before, self.snapshot())
        if PROC_READABLE:
            out = subprocess.run(command + ["--apply-plan-sha256", result["plan_sha256"]],
                                 capture_output=True, text=True, env=env, timeout=30)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertEqual(json.loads(out.stdout)["deleted_files"], 6)

    def test_abrupt_process_exit_keeps_recovery_and_rejects_stale_plan(self):
        plan = self.plan()
        before = self.snapshot()
        state_path = self.root.parent / "crash-state.json"
        state_path.write_text(json.dumps(self.state))
        program = """
import importlib.util,json,os,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('retention',sys.argv[1])
r=importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
state=json.loads(Path(sys.argv[4]).read_text())
calls=0
def abrupt_exit(files):
    global calls
    calls+=1
    if calls==3: os._exit(73)
r.apply_plan(Path(sys.argv[2]),sys.argv[3],lambda:state,abrupt_exit)
"""
        out = subprocess.run([sys.executable,"-B","-c",program,str(Path(r.__file__)),str(self.root),
                              r.digest(plan),str(state_path)],capture_output=True,text=True,timeout=30)
        self.assertEqual(out.returncode,73,out.stderr)
        self.assertEqual(len(before)-len(self.snapshot()),1)
        self.assertEqual(r.verify_recovery(self.root,plan["keep"]),8)
        receipts=list((self.root/".retention-audit").glob("*.jsonl"))
        self.assertEqual(len(receipts),1)
        rows=[json.loads(line) for line in receipts[0].read_text().splitlines()]
        self.assertEqual(sum("intent" in row for row in rows),1)
        self.assertEqual(sum("deleted" in row for row in rows),1)
        with self.assertRaisesRegex(RuntimeError,"plan changed"): self.apply(plan)
        self.assertEqual(self.apply()["deleted_files"],5)

    def test_future_acceptance_and_missing_recovery_block(self):
        p = self.root / self.names[-1] / "acceptance.json"
        original = p.read_text()
        rows = json.loads(original)
        rows[-1]["sample"]["checked_at"] = time.time() + 3600
        p.write_text(json.dumps(rows))
        with self.assertRaisesRegex(RuntimeError, "fresh acceptance"): self.plan()
        p.write_text(original)
        db = self.root / self.names[-2] / "cold-backup" / r.DATABASES[0]
        db.unlink()
        before = self.snapshot()
        with self.assertRaises(FileNotFoundError): self.plan()
        self.assertEqual(before, self.snapshot())

    def test_candidate_corruption_rejected_before_any_deletion(self):
        plan = self.plan()
        p = Path(plan["files"][-1]["path"])
        p.write_bytes(b"bad")
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "plan changed"): self.apply(plan)
        with self.assertRaisesRegex(RuntimeError, "candidate differs"): self.apply()
        self.assertEqual(before, self.snapshot())

    def test_missing_manifest_and_broad_root_fail_closed(self):
        (self.root / self.names[0] / "backup-checks.json").unlink()
        with self.assertRaises(FileNotFoundError): self.plan()
        with self.assertRaisesRegex(RuntimeError, "explicit releases"):
            r.apply_plan(self.root.parent, "x", lambda: self.state, lambda _: None)
        self.assertFalse((self.root.parent / ".deployment.lock").exists())

    def test_proc_permission_error_fails_closed(self):
        def entries(path):
            if path == Path("/proc"):
                return iter([Path("/proc/999")])
            raise PermissionError("cannot inspect process descriptors")
        with patch.object(Path, "is_dir", return_value=True), patch.object(Path, "iterdir", entries):
            with self.assertRaises(PermissionError): r.no_open_files([])

    @unittest.skipUnless(PROC_READABLE, "real /proc guard requires an inspectable Linux process namespace; covered in isolated Docker")
    def test_real_open_fd_is_detected(self):
        plan = self.plan()
        with Path(plan["files"][0]["path"]).open("rb"):
            with self.assertRaisesRegex(RuntimeError, "candidate is open"): r.no_open_files(plan["files"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import monitor_release_guard as guard
import test_release_backup_retention as fixtures


class ReleasePipelineRehearsal(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReleaseBackupRehearsal()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.name = self.fixture.names[-1]
        self.state_path = self.root.parent / "container-state.json"
        self.state_path.write_text(json.dumps(self.fixture.state))
        self.control = self.root.parent / "control.json"
        self.control.write_text("{}")
        self.events = self.root.parent / "events.jsonl"
        self.pid_file = self.root.parent / "child.pid"
        self.release_hold = self.root.parent / "release-child"
        self.script = self.root.parent / "deploy_synthetic.py"
        self.script.write_text(f"""
import datetime,json,os,sys,time
from pathlib import Path
phase=sys.argv[1]
control=json.loads(Path({str(self.control)!r}).read_text())
with Path({str(self.events)!r}).open('a') as f:
    f.write(json.dumps({{'phase':phase,'args':sys.argv[2:]}})+'\\n'); f.flush(); os.fsync(f.fileno())
if control.get('hold_phase')==phase:
    Path({str(self.pid_file)!r}).write_text(str(os.getpid()))
    deadline=time.monotonic()+10
    while not Path({str(self.release_hold)!r}).exists() and time.monotonic()<deadline: time.sleep(.02)
if control.get('fail_phase')==phase: raise SystemExit(42)
p=Path({str(self.state_path)!r}); states=json.loads(p.read_text())
if phase=='switch':
    states[0]['Config']['Image']={self.fixture.images[self.name]!r}
    states[0]['State']['StartedAt']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    p.write_text(json.dumps(states))
if phase=='rollback-image':
    states[0]['Config']['Image']={self.fixture.images[self.fixture.names[-2]]!r}
    p.write_text(json.dumps(states))
if phase=='accept' and not control.get('stale_accept'):
    path=Path({str(self.root / self.name / 'acceptance.json')!r})
    rows=json.loads(path.read_text())
    for row in rows:
        row['sample']['checked_at']=time.time()
        row['sample']['image']=states[0]['Config']['Image']
    path.write_text(json.dumps(rows))
""")

    def state(self):
        return json.loads(self.state_path.read_text())

    def phases(self):
        if not self.events.exists(): return []
        return [json.loads(line)["phase"] for line in self.events.read_text().splitlines()]

    def run_phase(self, phase, **kwargs):
        return guard.run_phase(self.root, self.script, self.name, phase,
                               baseline_verified=True, reports_verified=True,
                               state_provider=self.state, open_guard=lambda _: None, **kwargs)

    def cli(self, phase, *extra):
        return [sys.executable, "-B", str(Path(guard.__file__)), "--releases-dir", str(self.root),
                "--script", str(self.script), "--release-name", self.name,
                "--baseline-reports-verified", "--authenticated-reports-verified", *extra, phase]

    def fake_docker_env(self):
        folder = self.root.parent / "bin"
        folder.mkdir(exist_ok=True)
        p = folder / "docker"
        p.write_text(f"#!{sys.executable}\nimport sys,json\n"
                     f"states=json.load(open({str(self.state_path)!r}))\n"
                     "if sys.argv[1:]==['ps','-aq']: print('\\n'.join(c['Id'] for c in states))\n"
                     "elif sys.argv[1]=='inspect': print(json.dumps(states))\n"
                     "else: raise SystemExit('forbidden docker mutation')\n")
        p.chmod(0o700)
        return {**os.environ, "PATH": str(folder)+os.pathsep+os.environ.get("PATH", "")}

    def test_prepare_switch_accept_plan_default_never_deletes(self):
        before = self.fixture.snapshot()
        other_before = self.state()[1]
        for phase in ("prepare", "switch", "observe"):
            result = self.run_phase(phase)
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(result["cleanup"]["status"], "not_run")
        result = self.run_phase("accept")
        self.assertEqual(result["cleanup"]["status"], "plan_only")
        self.assertEqual(len(result["cleanup"]["plan"]["files"]), 6)
        after = self.fixture.snapshot()
        self.assertEqual({k:v for k,v in before.items() if not k.endswith("acceptance.json")},
                         {k:v for k,v in after.items() if not k.endswith("acceptance.json")})
        self.assertEqual(other_before, self.state()[1])
        self.assertEqual(self.phases(), ["prepare", "switch", "observe", "accept"])

    def test_explicit_accept_apply_and_restore(self):
        self.run_phase("prepare")
        self.run_phase("switch")
        result = self.run_phase("accept", cleanup="apply")
        self.assertEqual(result["cleanup"]["deleted_files"], 6)
        plan = guard.retention.make_plan(self.root, self.state())
        self.assertEqual(guard.retention.verify_recovery(self.root, plan["keep"]), 8)
        self.assertEqual(self.phases(), ["prepare", "switch", "accept"])
        self.assertEqual(self.run_phase("accept", cleanup="apply")["cleanup"]["mode"], "no_changes")

    def test_failure_of_any_phase_never_starts_cleanup_or_rollback(self):
        for phase in guard.PHASES:
            with self.subTest(phase=phase):
                self.control.write_text(json.dumps({"fail_phase":phase}))
                before = self.fixture.snapshot()
                result = self.run_phase(phase, cleanup="apply" if phase=="accept" else "plan")
                self.assertEqual(result["status"], "phase_failed")
                self.assertEqual(result["child_exit_code"], 42)
                self.assertEqual(result["cleanup"]["status"], "not_run")
                self.assertEqual(before, self.fixture.snapshot())
        self.assertEqual(self.phases(), list(guard.PHASES))

    def test_stale_success_receipt_is_not_acceptance(self):
        self.control.write_text('{"stale_accept":true}')
        before = self.fixture.snapshot()
        result = self.run_phase("accept", cleanup="apply")
        self.assertEqual(result["status"], "acceptance_unconfirmed")
        self.assertEqual(result["cleanup"]["status"], "not_run")
        self.assertEqual(before, self.fixture.snapshot())

    def test_cleanup_failure_does_not_fail_deployment_or_rollback(self):
        self.run_phase("switch")
        p = self.root/self.fixture.names[-2]/"cold-backup"/guard.retention.DATABASES[0]
        p.write_bytes(b"corrupted synthetic keeper")
        before = self.state()
        result = self.run_phase("accept", cleanup="apply")
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["cleanup"]["status"], "maintenance_warning")
        self.assertEqual(self.state(), before)
        self.assertEqual(self.phases(), ["switch", "accept"])
        for name in self.fixture.names[:2]:
            for rel in guard.retention.DATABASES: self.assertTrue((self.root/name/"cold-backup"/rel).exists())

    def test_rollback_only_uses_existing_image_rollback_and_no_cleanup(self):
        before = self.fixture.snapshot()
        result = self.run_phase("rollback-image")
        self.assertEqual(result["cleanup"]["status"], "not_run")
        self.assertEqual(before, self.fixture.snapshot())
        self.assertEqual(self.phases(), ["rollback-image"])

    def test_version_child_runs_shared_compose_rollback_under_release_lock(self):
        # The host phase is exercised through its real CLI/child process. Only
        # Docker commands and the clock are synthetic; no real service is touched.
        self.script.write_text(f"""
import fcntl, json, sys
from pathlib import Path
sys.path.insert(0, {str(Path(guard.__file__).resolve().parent)!r})
from monitor_release_rollback import rollback_compose_monitor
assert sys.argv[1:] == ["rollback-image"]
with open({str(self.root / ".deployment.lock")!r}, "r") as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        pass
    else:
        raise AssertionError("release lock not held by the parent and inherited child")
old = "registry/example@sha256:" + "a" * 64
new = "registry/example@sha256:" + "b" * 64
now, active, running = 0, new, True
events = []
def inspect(timeout):
    return {{"Name": "/monitor-test", "Config": {{"Image": active,
            "Labels": {{"com.docker.compose.project": "monitor-test", "com.docker.compose.service": "monitor"}}}},
            "State": {{"Running": running}}}}
def command(args, name, *, timeout):
    global now, active, running
    assert args[-1] == "monitor" and args[:4] == ["docker", "compose", "-p", "monitor-test"]
    if "stop" in args:
        running = False
        events.append("stop")
        now += 5
    else:
        assert not running and timeout <= 68 and "--no-deps" in args and "--pull" in args
        running, active = True, old
        events.append("old")
        now += 5
def verify(timeout):
    assert not running
    events.append("single_writer")
def warm(**kwargs):
    global now
    assert kwargs["deadline"] == 300 and not running
    events.append("warm")
    now += 25
    return {{"status": "interrupted", "reason": "watchdog_timeout"}}
def probe(deadline):
    assert deadline == 300 and active == old and running
    return {{"store": True, "facts_store": True, "monitor_http": True, "usage_http": True}}
def advance(delay):
    global now
    now += delay
result = rollback_compose_monitor(
    active_compose=["docker", "compose", "-p", "monitor-test", "-f", "new.yml"],
    old_compose=["docker", "compose", "-p", "monitor-test", "-f", "old.yml"],
    project="monitor-test", container_name="monitor-test", active_digest=new, old_digest=old,
    inspect_container=inspect, assert_no_other_writer=verify, checked=command, probe_ready=probe,
    data_root="/own/test/data", backup_root="/own/test/backup",
    store_paths=("/data/nexus_monitor.db", "/data/usage-facts.db"),
    warmer=warm, clock=lambda: now, sleep=advance)
Path({str(self.events)!r}).write_text(json.dumps({{"events": events, "result": result}}))
""")
        before = self.fixture.snapshot()
        result = self.run_phase("rollback-image")
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["cleanup"]["status"], "not_run")
        self.assertEqual(self.fixture.snapshot(), before)
        proof = json.loads(self.events.read_text())
        self.assertEqual(proof["events"], ["stop", "single_writer", "warm", "old"])
        self.assertEqual(proof["result"]["elapsed_ms"], 59000)
        self.assertEqual(proof["result"]["read_ahead"]["reason"], "watchdog_timeout")

    def test_all_phases_block_while_cleanup_holds_lock(self):
        checked = False
        def during_cleanup(files):
            nonlocal checked
            if checked: return
            checked = True
            for phase in guard.PHASES:
                with self.assertRaises(BlockingIOError): self.run_phase(phase)
        plan = guard.retention.make_plan(self.root,self.state())
        result = guard.retention.apply_plan(self.root,guard.retention.digest(plan),self.state,during_cleanup)
        self.assertEqual(result["deleted_files"],6)
        self.assertEqual(self.phases(),[])
        self.assertTrue(checked)

    def test_deployment_blocks_cleanup(self):
        plan = guard.retention.make_plan(self.root,self.state())
        def child(command, fd):
            self.assertGreaterEqual(fd,0)
            with self.assertRaises(BlockingIOError):
                guard.retention.apply_plan(self.root,guard.retention.digest(plan),self.state,lambda _:None)
            return 0
        self.assertEqual(self.run_phase("prepare",runner=child)["status"],"succeeded")

    def test_missing_auth_or_wrong_phase_apply_never_launches_script(self):
        for phase, kwargs in (("switch",{}),("accept",{}),("prepare",{"cleanup":"apply"})):
            with self.subTest(phase=phase):
                with self.assertRaises(RuntimeError):
                    guard.run_phase(self.root,self.script,self.name,phase,**kwargs)
        self.assertEqual(self.phases(),[])

    def test_new_deployment_between_acceptance_and_cleanup_is_warning(self):
        actual = guard.retention.make_plan
        def changed(root, states):
            states=copy.deepcopy(states)
            states[0]["Config"]["Image"]=self.fixture.images[self.fixture.names[-2]]
            return actual(root,states)
        with patch.object(guard.retention,"make_plan",side_effect=changed):
            result=self.run_phase("accept",cleanup="apply")
        self.assertEqual(result["status"],"succeeded")
        self.assertEqual(result["cleanup"]["status"],"maintenance_warning")
        self.assertEqual(self.phases(),["accept"])

    def test_cli_lock_conflict_has_distinct_exit_code(self):
        with guard.retention.deployment_lock(self.root):
            out=subprocess.run(self.cli("prepare"),capture_output=True,text=True,timeout=10)
        self.assertEqual(out.returncode,75,out.stderr)
        self.assertEqual(json.loads(out.stdout)["status"],"blocked")
        self.assertEqual(self.phases(),[])

    def test_cli_chain_and_cleanup_failure_exit_zero(self):
        env=self.fake_docker_env()
        for phase in ("prepare","switch","accept"):
            out=subprocess.run(self.cli(phase),env=env,capture_output=True,text=True,timeout=10)
            self.assertEqual(out.returncode,0,out.stderr)
        self.assertEqual(json.loads(out.stdout)["cleanup"]["status"],"plan_only")
        if fixtures.PROC_READABLE:
            p=self.root/self.fixture.names[-2]/"cold-backup"/guard.retention.DATABASES[0]
            p.write_bytes(b"broken")
            out=subprocess.run(self.cli("accept","--cleanup","apply"),env=env,capture_output=True,text=True,timeout=10)
            self.assertEqual(out.returncode,0,out.stderr)
            self.assertEqual(json.loads(out.stdout)["cleanup"]["status"],"maintenance_warning")
            self.assertNotIn("rollback-image",self.phases())

    def test_killed_wrapper_does_not_unlock_running_child(self):
        self.control.write_text('{"hold_phase":"prepare"}')
        proc=subprocess.Popen(self.cli("prepare"),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            deadline=time.monotonic()+5
            while not self.pid_file.exists() and time.monotonic()<deadline: time.sleep(.02)
            self.assertTrue(self.pid_file.exists(),"synthetic child did not start")
            proc.kill(); proc.wait(timeout=5)
            with self.assertRaises(BlockingIOError):
                with guard.retention.deployment_lock(self.root): pass
        finally:
            self.release_hold.touch()
            if proc.poll() is None: proc.kill(); proc.wait(timeout=5)
        deadline=time.monotonic()+5
        while True:
            try:
                with guard.retention.deployment_lock(self.root): break
            except BlockingIOError:
                if time.monotonic()>=deadline: self.fail("child failed to release lock")
                time.sleep(.02)
        self.assertEqual(self.phases(),["prepare"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

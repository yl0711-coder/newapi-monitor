"""Post-acceptance cold-backup retention. Default: read-only plan, no writes.

Never install as a broad age-based cron. Deployment, rollback and this tool
must share releases/.deployment.lock before enabling automatic invocation.
Only three named database files in accepted, superseded releases are eligible.
"""
import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import time


DATABASES = ("data/nexus_monitor.db", "data/usage-facts.db", "evidence/nginx-evidence.db")
RELEASE_NAME = re.compile(r"monitor-v\d+\.\d+\.\d+(?:[-.][a-zA-Z0-9]+)*-\d{8}(?:T\d{6}Z)?\Z")
IMAGE = re.compile(r"[^\s]+@sha256:[a-f0-9]{64}\Z")
MAX_DELETE_BYTES = 4 * 1024**3
MAX_CONTROL_BYTES = 8 * 1024**2


def fail(message):
    raise RuntimeError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def ordinary(path, directory=False):
    if path.resolve(strict=True) != path:
        fail("symlink path refused: " + str(path))
    s = path.lstat()
    if directory:
        if not stat.S_ISDIR(s.st_mode):
            fail("not a directory: " + str(path))
    elif not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
        fail("not a single-link regular file: " + str(path))
    return s


def identity(path):
    s = ordinary(path)
    return {"path": str(path), "bytes": s.st_size, "device": s.st_dev,
            "inode": s.st_ino, "mtime_ns": s.st_mtime_ns, "ctime_ns": s.st_ctime_ns}


def file_hash(path):
    before = identity(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    if identity(path) != before:
        fail("file changed during hashing: " + str(path))
    return h.hexdigest()


def read_control(path):
    s = ordinary(path)
    if not 0 < s.st_size <= MAX_CONTROL_BYTES:
        fail("control file size invalid: " + str(path))
    return json.loads(path.read_text())


def inside(path, parent):
    return path == parent or parent in path.parents


def mounted(path, containers):
    # Includes stopped containers; protect both mounted ancestors and children.
    for c in containers:
        for m in c.get("Mounts", []):
            if not m.get("Source") and m.get("Type") == "tmpfs":
                continue
            if not m.get("Source") or not Path(m["Source"]).is_absolute():
                fail("cannot establish host mount source")
            source = Path(m["Source"]).resolve()
            if inside(path, source) or inside(source, path):
                return True
    return False


def docker_state():
    ids = subprocess.check_output(["docker", "ps", "-aq"], text=True, timeout=20).split()
    if not ids:
        fail("no containers: cannot establish production boundary")
    return json.loads(subprocess.check_output(["docker", "inspect", *ids], timeout=30))


def state_signature(containers):
    return sorted([{"id": c["Id"], "name": c["Name"], "image": c["Config"]["Image"],
                    "started": c["State"]["StartedAt"], "status": c["State"]["Status"],
                    "health": c["State"].get("Health", {}).get("Status"),
                    "oom": c["State"].get("OOMKilled"), "restarts": c["RestartCount"],
                    "mounts": sorted((m["Source"], m["Destination"]) for m in c.get("Mounts", []))}
                   for c in containers], key=lambda c: c["id"])


def accepted(release):
    p = release / "acceptance.json"
    if not p.exists():
        return None
    rows = read_control(p)
    if not isinstance(rows, list) or len(rows) < 2:
        return None
    row = rows[-1]
    sample = row.get("sample", {})
    timestamp = sample.get("checked_at")
    if (row.get("decision", {}).get("action") != "READY" or
            row.get("same_mounts") is not True or row.get("other_services_unchanged") is not True or
            sample.get("reports_ok") is not True or sample.get("health") != "healthy" or
            sample.get("state") != "running" or sample.get("oom") is not False or sample.get("restarts") != 0 or
            sample.get("ready", {}).get("status") != "ready"):
        return None
    if type(timestamp) not in (float, int) or not math.isfinite(timestamp) or timestamp <= 0:
        fail("invalid acceptance time: " + release.name)
    if not IMAGE.fullmatch(sample.get("image", "")):
        fail("acceptance must use a pinned image: " + release.name)
    return {"release": release.name, "checked_at": timestamp, "image": sample["image"],
            "revision": sample.get("revision"), "acceptance_sha256": file_hash(p)}


def backup_checks(release):
    checks = read_control(release / "backup-checks.json")
    for relative in DATABASES:
        area, name = relative.split("/", 1)
        record = checks.get("/" + area, {})
        expected = record.get("source_hashes", {}).get(name, "")
        if not re.fullmatch(r"[a-f0-9]{64}", expected) or name not in record.get("sqlite_checked_as_uid_1000", []):
            fail("required recovery member missing from checks: " + release.name + "/" + relative)
    return checks


def validate_root(root):
    ordinary(root, directory=True)
    if root.name != "releases" or root == Path("/releases"):
        fail("expected an explicit releases directory, not a broad data root")


def make_plan(root, containers):
    validate_root(root)
    monitors = [c for c in containers if c["Name"] == "/nexusapi-monitor"]
    if len(monitors) != 1:
        fail("exactly one Monitor container required")
    monitor = monitors[0]
    state = monitor["State"]
    if state["Status"] != "running" or state.get("Health", {}).get("Status") != "healthy" or state.get("OOMKilled"):
        fail("Monitor is not healthy")
    records, protected = [], []
    for release in sorted(root.iterdir()):
        if not RELEASE_NAME.fullmatch(release.name):
            continue
        ordinary(release, directory=True)
        record = accepted(release)
        if record is None:
            protected.append({"release": release.name, "reason": "not_accepted"})
        else:
            records.append(record)
    records.sort(key=lambda r: (r["checked_at"], r["release"]), reverse=True)
    if len(records) < 2:
        fail("fewer than two accepted recovery points; no pruning")
    newest = records[0]
    started = dt.datetime.fromisoformat(state["StartedAt"].replace("Z", "+00:00")).timestamp()
    revision = monitor["Config"].get("Labels", {}).get("org.opencontainers.image.revision")
    if (newest["image"] != monitor["Config"]["Image"] or not revision or newest["revision"] != revision or
            newest["checked_at"] < started or newest["checked_at"] > time.time() + 60):
        fail("active container differs from latest accepted deployment; rollback/restart requires fresh acceptance")
    keep = records[:2]
    for record in keep:
        release = root / record["release"]
        backup_checks(release)
        for name in DATABASES:
            ordinary(release / "cold-backup" / name)
        record["checks_sha256"] = file_hash(release / "backup-checks.json")
    files = []
    for record in records[2:]:
        release = root / record["release"]
        if (release / "KEEP_BACKUP").exists() or (release / "KEEP_BACKUP").is_symlink():
            protected.append({"release": release.name, "reason": "pinned"})
            continue
        if mounted(release, containers):
            protected.append({"release": release.name, "reason": "container_mount"})
            continue
        checks = backup_checks(release)
        record["checks_sha256"] = file_hash(release / "backup-checks.json")
        for relative in DATABASES:
            p = release / "cold-backup" / relative
            # Previously retired files are not errors and are never recreated.
            if not p.exists() and not p.is_symlink():
                continue
            for suffix in ("-wal", "-shm", "-journal"):
                if (Path(str(p) + suffix)).exists() or (Path(str(p) + suffix)).is_symlink():
                    fail("SQLite sidecar found; manual review required: " + str(p))
            area, name = relative.split("/", 1)
            expected = checks["/" + area]["source_hashes"][name]
            files.append({**identity(p), "sha256": expected})
            if sum(f["bytes"] for f in files) > MAX_DELETE_BYTES or len(files) > 24:
                fail("cleanup exceeds per-run limit (4GiB / 24 files); review backlog separately")
    plan = {"version": 1, "root": str(root), "keep": keep, "accepted": records,
            "protected": protected, "containers": state_signature(containers), "files": files,
            "delete_bytes": sum(f["bytes"] for f in files)}
    return plan


def verify_recovery(root, records):
    checked = 0
    for record in records:
        release = root / record["release"]
        checks = backup_checks(release)
        if file_hash(release / "backup-checks.json") != record["checks_sha256"]:
            fail("recovery manifest changed")
        for area, values in checks.items():
            if area not in ("/data", "/evidence", "/gift-handoff"):
                fail("unknown backup area requires review")
            for relative, expected in values["source_hashes"].items():
                rel = Path(relative)
                if rel.is_absolute() or ".." in rel.parts or not rel.parts:
                    fail("unsafe recovery member path")
                p = release / "cold-backup" / area[1:] / rel
                if file_hash(p) != expected:
                    fail("retained recovery checksum failed: " + str(p))
                if p.suffix == ".db":
                    for suffix in ("-wal", "-shm", "-journal"):
                        if Path(str(p) + suffix).exists():
                            fail("retained recovery has SQLite sidecar")
                    deadline = time.monotonic() + 60
                    with contextlib.closing(sqlite3.connect(p.as_uri() + "?mode=ro&immutable=1", uri=True)) as conn:
                        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                        if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                            fail("retained SQLite quick_check failed: " + str(p))
                    checked += 1
    return checked


def no_open_files(files):
    if not Path("/proc/self/fd").is_dir():
        fail("open-file guard requires Linux /proc")
    names = {f["path"] for f in files}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            # Unlike glob, iterdir does not silently swallow permission errors.
            fds = list((proc / "fd").iterdir())
        except (FileNotFoundError, ProcessLookupError):
            continue
        for fd in fds:
            try:
                target = os.readlink(fd)
            except (FileNotFoundError, ProcessLookupError):
                continue
            if target in names:
                fail("candidate is open: " + target)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def deployment_lock(root):
    validate_root(root)
    path = root / ".deployment.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        ordinary(path)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A deployment child inherits this descriptor so killing its wrapper
        # cannot unlock the critical section while the child is still running.
        yield fd
    finally:
        os.close(fd)


def apply_plan(root, expected_sha, state_provider=docker_state, open_guard=no_open_files):
    with deployment_lock(root):
        plan = make_plan(root, state_provider())
        if digest(plan) != expected_sha:
            fail("plan changed; generate and review a new plan")
        if not plan["files"]:
            return {"mode": "no_changes", "deleted_files": 0}
        # Validate every candidate before any deletion, so a corrupt later
        # member cannot leave an avoidable partially applied plan.
        for entry in plan["files"]:
            if file_hash(Path(entry["path"])) != entry["sha256"]:
                fail("candidate differs from cold-backup checksum: " + entry["path"])
        verified = verify_recovery(root, plan["keep"])
        if make_plan(root, state_provider()) != plan:
            fail("files, mounts, acceptance or running container changed during verification")
        open_guard(plan["files"])
        audit = root / ".retention-audit"
        audit.mkdir(mode=0o700, exist_ok=True)
        ordinary(audit, directory=True)
        sync_directory(root)
        receipt = audit / (str(time.time_ns()) + ".jsonl")
        fd = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as log:
            sync_directory(audit)
            def record(value):
                log.write(json.dumps(value, sort_keys=True) + "\n")
                log.flush()
                os.fsync(log.fileno())
            record({"plan_sha256": expected_sha, "plan": plan, "verified_databases": verified})
            for entry in plan["files"]:
                # Deployment must honor the shared lock; recheck all containers
                # as a second fence against unexpected manual operations.
                if state_signature(state_provider()) != plan["containers"]:
                    fail("container state changed; stopping cleanup")
                p = Path(entry["path"])
                if identity(p) != {k: v for k, v in entry.items() if k != "sha256"}:
                    fail("candidate identity changed")
                if file_hash(p) != entry["sha256"]:
                    fail("candidate checksum changed")
                open_guard([entry])
                record({"intent": entry})
                p.unlink()  # explicit validated file only; never rmtree / glob deletion
                sync_directory(p.parent)
                record({"deleted": entry["path"], "bytes": entry["bytes"]})
            if state_signature(state_provider()) != plan["containers"]:
                fail("post-cleanup container state changed; inspect audit receipt")
            result = {"mode": "applied", "deleted_files": len(plan["files"]),
                      "deleted_bytes": plan["delete_bytes"], "verified_databases": verified,
                      "retained_releases": [r["release"] for r in plan["keep"]], "receipt": str(receipt)}
            record({"result": result})
            return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--releases-dir", type=Path, required=True)
    parser.add_argument("--apply-plan-sha256", help="explicitly execute this exact reviewed plan")
    args = parser.parse_args()
    root = args.releases_dir.absolute()
    if args.apply_plan_sha256:
        result = apply_plan(root, args.apply_plan_sha256)
    else:
        plan = make_plan(root, docker_state())
        result = {"mode": "plan_only", "plan_sha256": digest(plan), "plan": plan}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

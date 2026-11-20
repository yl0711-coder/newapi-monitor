"""Shared image-only rollback phase for the version-specific release script.

The caller owns the deployment lock and the verified Compose project, images,
and mounts. This phase never restores a backup or changes a database.
"""
import argparse
from dataclasses import dataclass
import hashlib
import json
from math import isfinite
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time
from typing import Callable, Mapping, Optional

from monitor_release_readahead import read_ahead_sqlite_pair, run_read_ahead_watchdog


_SNAPSHOT_LEAF = re.compile(r"pre-migrate-[A-Za-z0-9._-]+")
_MAX_METADATA = 64 * 1024


def _metadata(path):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as f:
        info = os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= _MAX_METADATA:
            raise ValueError("invalid migration metadata file")
        contents = f.read(_MAX_METADATA + 1)
        if len(contents) != info.st_size:
            raise ValueError("migration metadata size changed")
        return contents


def pinned_rollback_paths(*, backup_root, data_root, store_paths, data_target="/data"):
    """Resolve the exact Go migration-plan reference for the two verified mounts."""
    backups, data = Path(backup_root), Path(data_root)
    for root in (backups, data):
        if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
            raise ValueError("noncanonical rollback mount")
    if len(store_paths) != 2:
        raise ValueError("rollback requires two stores")
    target = PurePosixPath(data_target)
    names = []
    for item in store_paths:
        path = PurePosixPath(item)
        if not target.is_absolute() or str(path) != item or path.parent != target or path.name in (".", ".."):
            raise ValueError("store is outside the verified mount")
        names.append(path.name)
    if names[0] == names[1]:
        raise ValueError("live stores are not independent")
    # Keep this key in sync with monitor.preMigrationReferenceName. The key
    # uses container paths, not host mount paths or newest-directory guessing.
    key = hashlib.sha256(("main-facts\0" + "\0".join(store_paths)).encode()).hexdigest()[:32]
    reference = json.loads(_metadata(backups / (".pre-migration-plan-" + key + ".json")))
    leaf = reference["snapshot_dir"]
    if reference["format_version"] != 1 or not _SNAPSHOT_LEAF.fullmatch(leaf):
        raise ValueError("invalid pinned migration reference")
    root = backups / leaf
    if root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("noncanonical pinned rollback directory")
    raw = _metadata(root / "manifest.json")
    actual_hash = hashlib.sha256(raw).hexdigest()
    if (reference["manifest_sha256"] != actual_hash
            or _metadata(root / "READY").strip() != ("sha256:" + actual_hash).encode()):
        raise ValueError("migration reference, manifest and READY do not agree")
    manifest = json.loads(raw)
    expected = dict(zip(("main", "facts"), zip(names, ("monitor.db", "usage-facts.db"))))
    members = manifest["stores"]
    if (manifest["format_version"] != 1 or manifest["migration_plan"] != reference["migration_plan"]
            or not isinstance(members, list) or len(members) != 2
            or any(not isinstance(member, dict) or member.get("present") is not True for member in members)
            or {m["role"]: (m["source_file"], m["snapshot_file"]) for m in members} != expected):
        raise ValueError("unexpected paired migration manifest")
    return (root / "monitor.db", root / "usage-facts.db"), tuple(data / name for name in names)


def _read_ahead_stopped_mounts(*, backup_root, data_root, store_paths, seconds,
                              read_ahead=read_ahead_sqlite_pair):
    try:
        pinned, live = pinned_rollback_paths(backup_root=backup_root, data_root=data_root, store_paths=store_paths)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError):
        return {"status": "skipped", "reason": "pinned_reference_unavailable"}
    return read_ahead(pinned=pinned, live=live, seconds=seconds)


def warm_stopped_mounts(*, backup_root, data_root, store_paths, deadline, seconds=30,
                        watchdog=run_read_ahead_watchdog):
    if len(store_paths) != 2:
        raise ValueError("rollback requires two stores")
    # Resolving the reference is disk I/O too. Keep it out of the release
    # process so a stuck volume cannot hold the optional preparation open.
    command = [sys.executable, "-B", os.path.abspath(__file__), "--backup-root", os.fspath(backup_root),
               "--data-root", os.fspath(data_root), "--main-path", store_paths[0],
               "--facts-path", store_paths[1], "--seconds", str(seconds)]
    return watchdog(command, rollback_deadline=deadline, seconds=seconds)


@dataclass(frozen=True)
class ImageRollback:
    # stop, verify and start receive a command timeout in seconds. The probe
    # receives the shared absolute deadline so it can bound multiple checks.
    stop_active_writer: Callable[[float], None]
    verify_stopped_and_exclusive: Callable[[float], None]
    prepare_read_ahead: Callable[[float], Mapping]
    start_previous: Callable[[float], None]
    probe_previous: Callable[[float], Optional[Mapping]]


def rollback_image(steps, *, seconds=300, stable_seconds=24, polling_seconds=2,
                   clock=time.monotonic, sleep=time.sleep):
    """Use one deadline from before stopping the writer through stable readiness."""
    if (not all(isfinite(x) for x in (seconds, stable_seconds, polling_seconds))
            or not 0 < stable_seconds < seconds or polling_seconds <= 0):
        raise ValueError("invalid rollback window")
    started = clock()
    deadline = started + seconds

    def remaining(phase, cap):
        left = deadline - clock()
        if left <= 0:
            raise TimeoutError(f"rollback deadline reached before {phase}")
        return min(left, cap)

    steps.stop_active_writer(remaining("stop", 58))
    steps.verify_stopped_and_exclusive(remaining("stopped writer verification", 15))
    remaining("read-ahead", seconds)
    try:
        warming = steps.prepare_read_ahead(deadline)
    except Exception as exc:
        # Optional I/O acceleration may fail; stopping proof and old readiness
        # are mandatory. Do not rerun the helper or widen the deadline.
        warming = {"status": "interrupted", "reason": type(exc).__name__}
    steps.start_previous(remaining("old image start", 68))
    healthy_since = None
    while True:
        remaining("old image readiness", polling_seconds)
        proof = steps.probe_previous(deadline)
        observed = clock()
        if observed >= deadline:
            raise TimeoutError("rollback deadline reached before readiness result")
        if proof is not None:
            if not isinstance(proof, Mapping):
                raise TypeError("old readiness probe must return a proof or None")
            if healthy_since is None:
                healthy_since = observed
            if observed - healthy_since >= stable_seconds:
                return {"elapsed_ms": round((observed - started) * 1000), "read_ahead": warming,
                        "readiness": proof}
        else:
            healthy_since = None
        sleep(remaining("next readiness sample", polling_seconds))


def rollback_compose_monitor(*, active_compose, old_compose, project, container_name,
                             active_digest, old_digest, inspect_container, assert_no_other_writer,
                             checked, probe_ready, data_root, backup_root, store_paths,
                             seconds=300, stable_seconds=24, polling_seconds=2,
                             clock=time.monotonic, sleep=time.sleep, warmer=warm_stopped_mounts):
    """Concrete service adapter. Both Compose commands must already be audited.

    Use under the release lock. inspect_container must return None only if the
    named container is absent, not on Docker errors. assert_no_other_writer
    verifies that the already-audited mount set still has no other live writer.
    probe_ready verifies both listeners/stores using the absolute deadline.
    """
    for digest in (active_digest, old_digest):
        if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", digest):
            raise ValueError("rollback images must be pinned by digest")
    if active_digest == old_digest:
        raise ValueError("rollback requires distinct active and previous images")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise ValueError("invalid Compose project name")
    for command in (active_compose, old_compose):
        if (list(command[:4]) != ["docker", "compose", "-p", project]
                or any(arg in ("-p", "--project-name") or arg.startswith("--project-name=")
                       for arg in command[4:])):
            raise ValueError("both rollback commands must stick to the audited Compose project")

    def identity(container, digest):
        config = container.get("Config", {})
        labels = config.get("Labels") or {}
        if (container.get("Name") != "/" + container_name or config.get("Image") != digest
                or labels.get("com.docker.compose.project") != project
                or labels.get("com.docker.compose.service") != "monitor"):
            raise RuntimeError("rollback container identity does not match the audited service")

    def command_budget(deadline):
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError("rollback command time exhausted")
        return remaining

    def stop(timeout):
        deadline = clock() + timeout
        current = inspect_container(min(8, command_budget(deadline)))
        if current is not None:
            # Automatic rollback may run before the candidate ever started.
            # A stopped old container is safe; a running old one is not a
            # candidate and must not be stopped by this path.
            if not current["State"]["Running"] and current["Config"]["Image"] == old_digest:
                identity(current, old_digest)
            else:
                identity(current, active_digest)
                if current["State"]["Running"]:
                    checked([*active_compose, "stop", "monitor"], "stop candidate monitor",
                            timeout=command_budget(deadline))

    def verify(timeout):
        deadline = clock() + timeout
        current = inspect_container(min(8, command_budget(deadline)))
        if current is not None:
            digest = current["Config"]["Image"]
            if digest not in (active_digest, old_digest):
                raise RuntimeError("unexpected image owns the rollback mounts")
            identity(current, digest)
            if current["State"]["Running"]:
                raise RuntimeError("refuse rollback while Monitor can still write")
        assert_no_other_writer(command_budget(deadline))

    def start(timeout):
        checked([*old_compose, "up", "-d", "--no-deps", "--pull", "never", "monitor"],
                "start previous monitor", timeout=timeout)

    def probe(deadline):
        current = inspect_container(min(8, command_budget(deadline)))
        if current is None:
            return None
        identity(current, old_digest)
        state = current["State"]
        if state.get("OOMKilled") or current.get("RestartCount", 0):
            raise RuntimeError("previous Monitor encountered OOM or restarted")
        if not state["Running"]:
            raise RuntimeError("previous Monitor exited before readiness")
        command_budget(deadline)
        return probe_ready(deadline)

    def warm(deadline):
        return warmer(backup_root=backup_root, data_root=data_root,
                      store_paths=store_paths, deadline=deadline)

    return rollback_image(ImageRollback(stop, verify, warm, start, probe), seconds=seconds,
                          stable_seconds=stable_seconds, polling_seconds=polling_seconds,
                          clock=clock, sleep=sleep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read ahead only; does not manage a service")
    for key in ("backup-root", "data-root", "main-path", "facts-path"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(_read_ahead_stopped_mounts(
        backup_root=args.backup_root, data_root=args.data_root,
        store_paths=(args.main_path, args.facts_path), seconds=args.seconds)))

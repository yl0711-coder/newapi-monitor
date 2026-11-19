"""Best-effort read-ahead for a stopped Monitor on a Linux Docker host.

The deployment owner selects the current live pair and the pinned pair. This
helper never marks a store ready, starts a service, changes SQLite, or restores
a backup. Use one bounded attempt, with the original readiness checks intact.
"""
import argparse
import contextlib
import json
from math import isfinite
import os
from pathlib import Path
import stat
import subprocess
import sys
import threading
import time


MIB = 1024 ** 2
SIDECARS = ("-wal", "-journal")


def linux_available_memory():
    with open("/proc/meminfo", encoding="ascii") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                _, kib, unit = line.split()
                if unit == "kB":
                    return int(kib) * 1024
    raise ValueError("MemAvailable not available")


def fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def path_fingerprint(path):
    try:
        return fingerprint(path.lstat())
    except FileNotFoundError:
        return None


def read_ahead_sqlite_pair(*, pinned, live, max_bytes=3 * 1024 * MIB,
                           reserve_bytes=640 * MIB, seconds=30,
                           memory_available=linux_available_memory, clock=time.monotonic):
    """Return an observation, not a gate: the original launch always proceeds."""
    start = clock()
    read = 0
    scope = "none"

    def result(status, reason):
        return {"status": status, "reason": reason, "bytes_read": read,
                "scope": scope, "elapsed_ms": round((clock() - start) * 1000)}

    if len(pinned) != 2 or len(live) != 2 or not (0 < max_bytes <= 8 * 1024 * MIB
            and reserve_bytes >= 512 * MIB and 0 < seconds <= 60):
        return result("skipped", "invalid_plan")
    try:
        # Do not treat missing files, aliases, symlinks or non-regular files as
        # a valid precondition. The legacy integrity/restore path still decides.
        paths = {}
        initial_paths = {}
        for store, is_live in [(path, False) for path in pinned] + [(path, True) for path in live]:
            path = Path(store)
            if not path.is_absolute() or path.resolve(strict=True) != path:
                return result("skipped", "noncanonical_path")
            if path in paths:
                return result("skipped", "duplicate_store")
            paths[path] = is_live
            for suffix in SIDECARS:
                sidecar = path.with_name(path.name + suffix)
                side = path_fingerprint(sidecar)
                initial_paths[sidecar] = side
                if side is not None:
                    if sidecar in paths:
                        return result("skipped", "duplicate_store")
                    if not is_live and side[2] > 0:
                        return result("skipped", "sealed_sidecar_has_data")
                    paths[sidecar] = is_live
        with contextlib.ExitStack() as resources:
            inputs = []
            identities = set()
            for path, is_live in paths.items():
                initial = path.lstat()
                if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1:
                    return result("skipped", "not_an_independent_regular_file")
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
                resources.callback(os.close, fd)
                actual = os.fstat(fd)
                if fingerprint(actual) != fingerprint(initial):
                    return result("skipped", "file_changed")
                if fingerprint(actual)[:2] in identities:
                    return result("skipped", "duplicate_store")
                identities.add(fingerprint(actual)[:2])
                mark = fingerprint(actual)
                if path in initial_paths and initial_paths[path] != mark:
                    return result("skipped", "file_changed")
                initial_paths[path] = mark
                inputs.append((fd, actual.st_size, is_live))
            total = sum(size for _, size, _ in inputs)
            live_bytes = sum(size for _, size, is_live in inputs if is_live)
            if live_bytes > max_bytes:
                return result("skipped", "byte_budget")
            available = memory_available()
            if live_bytes + reserve_bytes > available:
                return result("skipped", "host_memory_budget")
            scope = "live_and_pinned" if total <= max_bytes and total + reserve_bytes <= available else "live_only"
            for fd, remaining, is_live in inputs:
                if scope == "live_only" and not is_live:
                    continue
                if hasattr(os, "posix_fadvise"):
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_SEQUENTIAL)
                while remaining:
                    if clock() - start >= seconds:
                        return result("interrupted", "time_budget")
                    if memory_available() < reserve_bytes:
                        return result("interrupted", "host_memory_budget")
                    chunk = os.read(fd, min(MIB, remaining))
                    if not chunk:
                        return result("interrupted", "file_changed")
                    read += len(chunk)
                    remaining -= len(chunk)
            if any(path_fingerprint(path) != initial for path, initial in initial_paths.items()):
                return result("interrupted", "file_changed")
            if memory_available() < reserve_bytes:
                return result("interrupted", "host_memory_budget")
            if clock() - start >= seconds:
                return result("interrupted", "time_budget")
            return result("completed", "read_ahead_only")
    except FileNotFoundError:
        return result("interrupted" if read else "skipped", "file_missing")
    except (OSError, ValueError, RuntimeError):
        return result("interrupted" if read else "skipped", "io_or_platform_unavailable")


def run_bounded_read_ahead(*, pinned, live, rollback_deadline, startup_reserve_seconds=60,
                           seconds=30, max_mib=3072, reserve_mib=640, script=None):
    """Host-side watchdog; use the same monotonic deadline for the real rollback.

    The caller has the deployment lock and has already stopped the writer.
    It must proceed to the original startup/integrity checks on every outcome.
    """
    if len(pinned) != 2 or len(live) != 2:
        return {"status": "skipped", "reason": "invalid_plan", "bytes_read": None,
                "scope": "unknown", "elapsed_ms": 0}
    args = [sys.executable, "-B", str(Path(__file__).resolve() if script is None else script)]
    for key, path in zip(("pinned-main", "pinned-facts", "live-main", "live-facts"), (*pinned, *live)):
        args.extend(("--" + key, os.fspath(path)))
    args += ["--seconds", str(seconds), "--max-mib", str(max_mib), "--reserve-mib", str(reserve_mib)]
    return run_read_ahead_watchdog(args, rollback_deadline=rollback_deadline,
                                   startup_reserve_seconds=startup_reserve_seconds, seconds=seconds)


def run_read_ahead_watchdog(args, *, rollback_deadline, startup_reserve_seconds=60, seconds=30):
    """Bound the entire optional subprocess, including its metadata reads."""
    start = time.monotonic()

    def outcome(status, reason):
        return {"status": status, "reason": reason, "bytes_read": None, "scope": "unknown",
                "elapsed_ms": round((time.monotonic() - start) * 1000)}

    if (not isfinite(rollback_deadline)
            or not isfinite(startup_reserve_seconds) or startup_reserve_seconds < 0
            or type(seconds) is not int or not 1 <= seconds <= 60):
        return outcome("skipped", "invalid_plan")
    edge = min(start + seconds + 2, rollback_deadline - startup_reserve_seconds)
    if edge <= start:
        return outcome("skipped", "rollback_time_reserved")
    try:
        proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL)
    except OSError:
        return outcome("skipped", "helper_start_failed")
    try:
        stdout, _ = proc.communicate(timeout=max(0, edge - time.monotonic()))
    except (subprocess.TimeoutExpired, OSError) as exc:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        proc.stdout.close()
        try:
            proc.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            # A kernel I/O wait need not end on SIGKILL immediately. Do not
            # block rollback on the optional read-only child.
            threading.Thread(target=proc.wait, daemon=True).start()
        return outcome("interrupted", "watchdog_timeout" if isinstance(exc, subprocess.TimeoutExpired)
                       else "helper_io_failed")
    try:
        result = json.loads(stdout)
        if proc.returncode != 0 or result.get("status") not in ("completed", "skipped", "interrupted"):
            raise ValueError("helper failed")
        if result["status"] == "completed" and (
                result.get("scope") not in ("live_only", "live_and_pinned")
                or type(result.get("bytes_read")) is not int or result["bytes_read"] <= 0):
            raise ValueError("incomplete helper result")
    except (ValueError, AttributeError):
        return outcome("interrupted", "helper_failed")
    result["elapsed_ms"] = round((time.monotonic() - start) * 1000)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("pinned-main", "pinned-facts", "live-main", "live-facts"):
        parser.add_argument("--" + key, required=True, type=Path)
    parser.add_argument("--max-mib", type=int, default=3072)
    parser.add_argument("--reserve-mib", type=int, default=640)
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    report = read_ahead_sqlite_pair(
        pinned=(args.pinned_main, args.pinned_facts), live=(args.live_main, args.live_facts),
        max_bytes=args.max_mib * MIB, reserve_bytes=args.reserve_mib * MIB,
        seconds=args.seconds)
    print(json.dumps(report))


if __name__ == "__main__":
    main()

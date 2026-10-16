"""Serialize existing Monitor deployment phases; no new deployment semantics.

The version-specific deployment script remains responsible for backups,
switching, readiness and image-only rollback. Cleanup defaults to plan-only
after successful fresh acceptance, never after switch or rollback.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

import release_backup_retention as retention


PHASES = ("prepare", "switch", "observe", "accept", "rollback-image")


def run_child(command, lock_fd):
    # No shell, automatic retry, automatic rollback or implicit timeout.
    # The child owns the same flock until its process exits.
    return subprocess.run(command, pass_fds=(lock_fd,)).returncode


def run_phase(root, script, release_name, phase, *, baseline_verified=False,
              reports_verified=False, cleanup="plan", runner=run_child,
              state_provider=retention.docker_state, open_guard=retention.no_open_files):
    retention.validate_root(root)
    retention.ordinary(script)
    if not retention.inside(script, root.parent) or script.suffix != ".py":
        raise RuntimeError("deployment script must be a reviewed Python file beneath the tools root")
    if not retention.RELEASE_NAME.fullmatch(release_name):
        raise RuntimeError("invalid release artifact name")
    if phase not in PHASES or cleanup not in ("off", "plan", "apply"):
        raise RuntimeError("invalid phase or cleanup mode")
    if phase != "accept" and cleanup == "apply":
        raise RuntimeError("cleanup apply is only permitted after acceptance")
    if phase == "switch" and not baseline_verified:
        raise RuntimeError("authenticated baseline verification required before switch")
    if phase == "accept" and not reports_verified:
        raise RuntimeError("authenticated post-release reports must be verified first")
    command = [sys.executable, str(script), phase]
    if phase == "switch":
        command.append("--baseline-reports-verified")
    if phase == "accept":
        command.append("--authenticated-reports-verified")
    with retention.deployment_lock(root) as lock_fd:
        began = time.time()
        code = runner(command, lock_fd)
        if code != 0:
            return {"phase": phase, "status": "phase_failed", "child_exit_code": code,
                    "cleanup": {"status": "not_run"}}
        if phase == "accept":
            # Exit code 0 alone must not authorize cleanup using a stale
            # acceptance from a previous invocation or a different release.
            try:
                record = retention.accepted(root / release_name)
                if record is None or not began <= record["checked_at"] <= time.time() + 60:
                    raise RuntimeError("no fresh matching acceptance receipt")
            except Exception as exc:
                return {"phase": phase, "status": "acceptance_unconfirmed", "reason": str(exc),
                        "cleanup": {"status": "not_run"}}
    result = {"phase": phase, "status": "succeeded", "cleanup": {"status": "not_run"}}
    if phase != "accept" or cleanup == "off":
        return result
    # Release the deployment lock, then reacquire for a fresh plan. Another
    # operation in this gap invalidates cleanup; never deadlock by nesting locks.
    try:
        with retention.deployment_lock(root):
            plan = retention.make_plan(root, state_provider())
            if plan["keep"][0]["release"] != release_name:
                raise RuntimeError("a different release was accepted before cleanup")
        plan_sha = retention.digest(plan)
        if cleanup == "plan":
            result["cleanup"] = {"status": "plan_only", "plan_sha256": plan_sha, "plan": plan}
        else:
            result["cleanup"] = retention.apply_plan(root, plan_sha, state_provider, open_guard)
    except Exception as exc:
        # Maintenance failure is NOT a failed deployment. In particular never
        # feed this exception into an automatic service/database rollback path.
        result["cleanup"] = {"status": "maintenance_warning", "reason": str(exc)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--releases-dir", type=Path, required=True)
    parser.add_argument("--script", type=Path, required=True)
    parser.add_argument("--release-name", required=True)
    parser.add_argument("--cleanup", choices=("off", "plan", "apply"), default="plan")
    parser.add_argument("--baseline-reports-verified", action="store_true")
    parser.add_argument("--authenticated-reports-verified", action="store_true")
    parser.add_argument("phase", choices=PHASES)
    args = parser.parse_args()
    try:
        result = run_phase(args.releases_dir.absolute(), args.script.absolute(), args.release_name, args.phase,
                           baseline_verified=args.baseline_reports_verified,
                           reports_verified=args.authenticated_reports_verified, cleanup=args.cleanup)
    except BlockingIOError:
        print(json.dumps({"status": "blocked", "reason": "deployment_or_cleanup_in_progress"}))
        return 75
    except Exception as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

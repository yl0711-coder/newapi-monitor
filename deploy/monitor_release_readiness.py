"""Policy for a Monitor release; does not start/stop a container or run SQL."""

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Optional


class Action(str, Enum):
    WAIT_ENTRY = "wait_entry"
    OBSERVE_DATA = "observe_data"
    ACCEPT = "accept"
    HOLD = "hold"
    ROLLBACK = "rollback"


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str


@dataclass(frozen=True)
class Probe:
    # Readiness of the two listeners and the actual local store checks are
    # separate from progress of background workers on a healthy process.
    monitor_http: bool = False
    usage_http: bool = False
    stores_verified: bool = False
    workers_healthy: bool = False
    source_through: Optional[int] = None
    rejection_through: Optional[int] = None
    fatal_entry_failure: bool = False  # e.g. process exit, OOM, or integrity failure
    data_hard_failure: bool = False


class ReleaseReadiness:
    def __init__(self, *, started, entry_seconds, catchup_seconds,
                 stall_seconds, stable_seconds, lost_entry_seconds,
                 source_target, rejection_target):
        values = (started, entry_seconds, catchup_seconds, stall_seconds,
                  stable_seconds, lost_entry_seconds)
        if not all(isfinite(v) for v in values) or min(values[1:]) <= 0:
            raise ValueError("release timing must be bounded and positive")
        if stall_seconds > catchup_seconds or stable_seconds >= catchup_seconds:
            raise ValueError("catch-up window must allow stall and stable checks")
        if any(type(t) is not int or t < 0 for t in (source_target, rejection_target)):
            raise ValueError("invalid source baseline cursor")
        self.started = self.last_observation = started
        self.entry_seconds = entry_seconds
        self.catchup_seconds = catchup_seconds
        self.stall_seconds = stall_seconds
        self.stable_seconds = stable_seconds
        self.lost_entry_seconds = lost_entry_seconds
        self.target = (source_target, rejection_target)
        self.cursor = [None, None]
        self.previous_sample = (None, None)
        self.progress_at = [None, None]
        self.entry_at = self.lost_entry_at = self.stable_at = None
        self.terminal = None

    def finish(self, action, reason):
        self.terminal = Decision(action, reason)
        return self.terminal

    def observe(self, now, probe):
        if not isfinite(now) or now < self.last_observation:
            raise ValueError("use a monotonic observation clock")
        self.last_observation = now
        # An already requested rollback is final. HOLD/ACCEPT finish the
        # data decision only; an infrastructure failure can still supersede.
        if self.terminal is not None and self.terminal.action is Action.ROLLBACK:
            return self.terminal
        if probe.fatal_entry_failure:
            return self.finish(Action.ROLLBACK, "fatal_entry_failure")

        available = probe.monitor_http and probe.usage_http and probe.stores_verified
        if self.entry_at is None:
            if now - self.started >= self.entry_seconds:
                return self.finish(Action.ROLLBACK, "entry_deadline")
            if not available:
                return Decision(Action.WAIT_ENTRY, "waiting_for_http_and_stores")
            self.entry_at = now
            self.progress_at = [now, now]
        if not available:
            self.stable_at = None
            if self.lost_entry_at is None:
                self.lost_entry_at = now
            if now - self.lost_entry_at >= self.lost_entry_seconds:
                return self.finish(Action.ROLLBACK, "entry_lost_after_start")
            if self.terminal is not None:
                return Decision(Action.HOLD, "entry_temporarily_unreachable")
            return Decision(Action.OBSERVE_DATA, "entry_temporarily_unreachable")
        self.lost_entry_at = None
        if self.terminal is not None:
            return self.terminal

        if probe.data_hard_failure:
            return self.finish(Action.HOLD, "data_worker_failed_with_entry_available")
        if now - self.entry_at >= self.catchup_seconds:
            return self.finish(Action.HOLD, "data_acceptance_deadline")

        current = (probe.source_through, probe.rejection_through)
        if any(type(old) is int and type(new) is int and new < old
               for old, new in zip(self.previous_sample, current)):
            self.stable_at = None
        self.previous_sample = current
        for i, value in enumerate(current):
            if type(value) is int and value >= 0 and (self.cursor[i] is None or value > self.cursor[i]):
                self.cursor[i] = value
                self.progress_at[i] = now
        settled = [type(v) is int and v >= t for v, t in zip(current, self.target)]
        if not probe.workers_healthy or not all(settled):
            self.stable_at = None
            if any(not reached and now - self.progress_at[i] >= self.stall_seconds
                   for i, reached in enumerate(settled)):
                return self.finish(Action.HOLD, "data_cursor_stalled")
            return Decision(Action.OBSERVE_DATA, "waiting_for_data")
        if self.stable_at is None:
            self.stable_at = now
        if now - self.stable_at >= self.stable_seconds:
            return self.finish(Action.ACCEPT, "entry_and_data_stable")
        return Decision(Action.OBSERVE_DATA, "stability_window")

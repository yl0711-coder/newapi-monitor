import unittest

from monitor_release_readiness import Action, Probe, ReleaseReadiness


def available(source=None, rejection=None, **kwargs):
    return Probe(monitor_http=True, usage_http=True, stores_verified=True,
                 source_through=source, rejection_through=rejection, **kwargs)


class ReleaseReadinessTest(unittest.TestCase):
    def setUp(self):
        self.gate = ReleaseReadiness(started=0, entry_seconds=5, catchup_seconds=30,
                                     stall_seconds=8, stable_seconds=3, lost_entry_seconds=2,
                                     source_target=100, rejection_target=80)

    def test_second_listener_and_both_stores_required(self):
        self.assertEqual(self.gate.observe(1, Probe(monitor_http=True, stores_verified=True)).action,
                         Action.WAIT_ENTRY)
        self.assertEqual(self.gate.observe(2, Probe(monitor_http=True, usage_http=True)).action,
                         Action.WAIT_ENTRY)
        d = self.gate.observe(5, available(100, 80))
        self.assertEqual((d.action, d.reason), (Action.ROLLBACK, "entry_deadline"))

    def test_data_has_its_own_clock_after_last_second_entry(self):
        self.assertEqual(self.gate.observe(4, available(50, 40)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(7, available(60, 50)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(12, available(90, 70)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(17, available(100, 80, workers_healthy=True)).reason,
                         "stability_window")
        self.assertEqual(self.gate.observe(20, available(101, 81, workers_healthy=True)).action, Action.ACCEPT)

    def test_one_stalled_cursor_cannot_hide_behind_other_progress(self):
        self.gate.observe(1, available(30, 20))
        self.gate.observe(5, available(50, 20))
        d = self.gate.observe(9, available(80, 20))
        self.assertEqual((d.action, d.reason), (Action.HOLD, "data_cursor_stalled"))

    def test_healthy_entry_is_held_not_restarted_on_data_error(self):
        d = self.gate.observe(1, available(30, 20, data_hard_failure=True))
        self.assertEqual((d.action, d.reason), (Action.HOLD, "data_worker_failed_with_entry_available"))
        self.assertEqual(self.gate.observe(3, available(100, 80, workers_healthy=True)), d)

    def test_fatal_entry_failure_supersedes_held_data(self):
        self.gate.observe(1, available(30, 20, data_hard_failure=True))
        self.assertEqual(self.gate.observe(3, Probe(fatal_entry_failure=True)).reason, "fatal_entry_failure")

    def test_sustained_entry_loss_supersedes_hold_but_brief_loss_does_not(self):
        held = self.gate.observe(1, available(data_hard_failure=True))
        self.assertEqual(self.gate.observe(2, Probe()).reason, "entry_temporarily_unreachable")
        self.assertEqual(self.gate.observe(3, available()), held)
        self.gate.observe(4, Probe())
        decision = self.gate.observe(6, Probe())
        self.assertEqual((decision.action, decision.reason), (Action.ROLLBACK, "entry_lost_after_start"))
        self.assertEqual(self.gate.observe(7, available()), decision)

    def test_entry_loss_is_not_reported_accepted_after_data_acceptance(self):
        both = available(100, 80, workers_healthy=True)
        self.gate.observe(1, both)
        self.assertEqual(self.gate.observe(4, both).action, Action.ACCEPT)
        self.assertEqual(self.gate.observe(5, Probe()).action, Action.HOLD)
        self.assertEqual(self.gate.observe(7, Probe()).action, Action.ROLLBACK)

    def test_temporary_loss_resets_stability_but_sustained_loss_rolls_back(self):
        self.gate.observe(1, available(100, 80, workers_healthy=True))
        self.assertEqual(self.gate.observe(2, Probe()).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(3, available(100, 80, workers_healthy=True)).action,
                         Action.OBSERVE_DATA)
        self.gate.observe(4, Probe())
        self.assertEqual(self.gate.observe(6, Probe()).reason, "entry_lost_after_start")

    def test_fatal_store_or_process_error_rolls_back_without_delay(self):
        d = self.gate.observe(1, Probe(fatal_entry_failure=True))
        self.assertEqual((d.action, d.reason), (Action.ROLLBACK, "fatal_entry_failure"))

    def test_cursor_regression_cannot_accept_cached_high_watermark(self):
        self.gate.observe(1, available(100, 80, workers_healthy=True))
        self.assertEqual(self.gate.observe(4, available(100, 70, workers_healthy=True)).action,
                         Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(6, available(100, 80, workers_healthy=True)).action,
                         Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(9, available(100, 80, workers_healthy=True)).action, Action.ACCEPT)

    def test_regression_while_still_above_target_restarts_stability_window(self):
        self.gate.observe(1, available(130, 120, workers_healthy=True))
        self.assertEqual(self.gate.observe(4, available(125, 120, workers_healthy=True)).reason,
                         "stability_window")
        self.assertEqual(self.gate.observe(5, available(124, 120, workers_healthy=True)).reason,
                         "stability_window")
        self.assertEqual(self.gate.observe(8, available(125, 120, workers_healthy=True)).action, Action.ACCEPT)

    def test_progress_cannot_extend_absolute_deadline_forever(self):
        for now in [1, 7, 13, 19, 25]:
            self.assertEqual(self.gate.observe(now, available(now, now)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(31, available(31, 31)).reason, "data_acceptance_deadline")

    def test_missing_or_noninteger_cursors_and_unhealthy_workers_do_not_accept(self):
        self.assertEqual(self.gate.observe(1, available("100", 80, workers_healthy=True)).action,
                         Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(6, available(100, 80)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(27, available(100, 80)).action, Action.OBSERVE_DATA)
        self.assertEqual(self.gate.observe(31, available(100, 80)).action, Action.HOLD)

    def test_rejects_backwards_clock(self):
        self.gate.observe(2, Probe())
        with self.assertRaisesRegex(ValueError, "monotonic"):
            self.gate.observe(1, Probe())


if __name__ == "__main__":
    unittest.main()

import unittest

from ecs_acceptance_verify import final_boundary_mismatches


class FinalBoundaryGateTest(unittest.TestCase):
    def test_requires_one_verified_boundary_for_every_source(self):
        source = {"node": "fixture", "lane": "access"}
        good = {**source, "status": 200, "final_boundary_verified": 1}
        for proofs in [[], [{**good, "status": 425}], [{**good, "final_boundary_verified": 0}],
                       [good, good], [good, {**good, "node": "unexpected"}]]:
            with self.subTest(proofs=proofs):
                self.assertTrue(final_boundary_mismatches({"sources": [source], "final_boundaries": proofs}))
        self.assertEqual(final_boundary_mismatches({"sources": [source], "final_boundaries": [good]}), [])

    def test_empty_or_legacy_report_cannot_pass_new_protocol_gate(self):
        self.assertTrue(final_boundary_mismatches({"sources": []}))
        self.assertTrue(final_boundary_mismatches({"sources": [{"node": "legacy", "lane": "reject"}]}))


if __name__ == "__main__":
    unittest.main()

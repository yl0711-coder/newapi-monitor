import unittest
from ecs_acceptance_logs import stream_events


class LogPaginationTest(unittest.TestCase):
    def reader(self, pages):
        pages = iter(pages)
        return lambda *_: next(pages)

    def test_empty_page_does_not_mean_empty_stream(self):
        reader = self.reader([{"events": [], "nextForwardToken": "a"},
                              {"events": [{"message": "final oracle"}], "nextForwardToken": "b"},
                              {"events": [], "nextForwardToken": "b"}])
        self.assertEqual(stream_events(reader, "test", "synthetic"), [{"message": "final oracle"}])

    def test_pagination_cycle_fails_closed(self):
        reader = self.reader([{"events": [], "nextForwardToken": t} for t in ["a", "b", "a"]])
        with self.assertRaisesRegex(RuntimeError, "cycle"):
            stream_events(reader, "test", "synthetic")

    def test_token_missing_is_not_complete(self):
        with self.assertRaisesRegex(RuntimeError, "token missing"):
            stream_events(self.reader([{"events": []}]), "test", "synthetic")

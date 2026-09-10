import json
import tempfile
import unittest
from pathlib import Path

from ecs_split_smoke import final_oracle, verify_files


class SplitOracleTest(unittest.TestCase):
    def test_final_and_retained_files_match(self):
        row = {"schema": "split-v1", "kind": "nginx", "instance": "a" * 16, "final": True,
               "files": {"nexusapi_access.jsonl": 2, "error.log": 2}}
        oracle = final_oracle("nginx", "SPLIT_SYNTHETIC_ORACLE " + json.dumps(row))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in row["files"]:
                (root / name).write_bytes(b"fixture\nfixture\n")
            verify_files(root, oracle)
            (root / "error.log").write_bytes(b"fixture\n")
            with self.assertRaisesRegex(RuntimeError, "line count"):
                verify_files(root, oracle)

    def test_missing_duplicate_or_wrong_final_rejected(self):
        row = {"schema": "split-v1", "kind": "nginx", "instance": "a" * 16, "final": True,
               "files": {"nexusapi_access.jsonl": 2, "error.log": 2}}
        line = "SPLIT_SYNTHETIC_ORACLE " + json.dumps(row)
        for output in ("", "SYNTHETIC_ORACLE {}", line + "\n" + line):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                final_oracle("nginx", output)
        with self.assertRaisesRegex(RuntimeError, "identity"):
            final_oracle("new-api", line)

    def test_missing_tail_created_file_rejected(self):
        row = {"schema": "split-v1", "kind": "new-api", "instance": "a" * 16, "final": True,
               "files": {"oneapi-20000101.log": 2, "oneapi-20000102.log": 2}}
        with self.assertRaisesRegex(RuntimeError, "file set"):
            final_oracle("new-api", "SPLIT_SYNTHETIC_ORACLE " + json.dumps(row))


if __name__ == "__main__":
    unittest.main()

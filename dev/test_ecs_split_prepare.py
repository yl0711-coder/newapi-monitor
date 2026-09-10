import datetime as dt
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ecs_split_prepare as prepare


class SplitPrepareTest(unittest.TestCase):
    def test_exported_config_hash_not_docker_index_id(self):
        config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        digest = hashlib.sha256(config).hexdigest()
        name = "blobs/sha256/" + digest
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            with tarfile.open(path, "w") as archive:
                for member, data in (("manifest.json", json.dumps([{"Config": name}]).encode()), (name, config)):
                    info = tarfile.TarInfo(member)
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
            self.assertEqual(prepare.exported_config(path), "sha256:" + digest)

    def test_export_cannot_follow_tar_link(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            with tarfile.open(path, "w") as archive:
                info = tarfile.TarInfo("manifest.json")
                info.type = tarfile.SYMTYPE
                info.linkname = "/unapproved"
                archive.addfile(info)
            with self.assertRaisesRegex(RuntimeError, "regular"):
                prepare.exported_config(path)

    def test_remote_docker_rejected_before_reading_images(self):
        with patch.object(prepare, "docker", return_value="tcp://remote:2376") as docker:
            with self.assertRaisesRegex(RuntimeError, "local OrbStack"):
                prepare.images()
            self.assertEqual(docker.call_count, 1)

    def test_collector_retag_detected(self):
        image = {"Architecture": "amd64", "Os": "linux", "Id": "sha256:" + "a" * 64,
                 "Config": {"User": "collector", "Entrypoint": []}}
        with patch.object(prepare, "docker", side_effect=["unix:///test/.orbstack/run/docker.sock", json.dumps([image])]):
            with self.assertRaisesRegex(RuntimeError, "image changed"):
                prepare.images()

    def test_manifest_binds_registry_digest_to_local_config(self):
        local = "sha256:" + "a" * 64
        manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": {"digest": local}, "layers": [{"digest": "sha256:" + "b" * 64}]}
        raw = json.dumps(manifest).encode()
        digest = prepare.verify_registry_manifest(raw, local)
        self.assertNotEqual(digest, local)
        self.assertEqual(digest, prepare.verify_registry_manifest(raw, local))
        with self.assertRaisesRegex(RuntimeError, "differs"):
            prepare.verify_registry_manifest(raw, "sha256:" + "c" * 64)
        manifest["mediaType"] = "application/vnd.oci.image.index.v1+json"
        with self.assertRaisesRegex(RuntimeError, "single-platform"):
            prepare.verify_registry_manifest(json.dumps(manifest).encode(), local)

    def test_malformed_manifest(self):
        for raw in (b'{}', b'null', b'[]', b'{"a":1,"a":2}', b'x' * (256 * 1024 + 1)):
            with self.subTest(raw=raw[:50]), self.assertRaises(RuntimeError):
                prepare.verify_registry_manifest(raw, "sha256:" + "a" * 64)

    def test_bootstrap_cannot_enable_tasks(self):
        now = dt.datetime(2026, 9, 10, 0, 0, tzinfo=dt.timezone.utc)
        values = {x["ParameterKey"]: x["ParameterValue"] for x in prepare.runtime_parameters(
            "https://fixture.trycloudflare.com", "2026-09-10T01:00:00", now)}
        self.assertEqual(values["EnableTestDefinition"], "no")
        self.assertEqual(values["SyntheticImageDigest"], "UNSET")

    def test_expired_excessive_or_invalid_cutoff_rejected(self):
        now = dt.datetime(2026, 9, 10, 0, 0, tzinfo=dt.timezone.utc)
        for cutoff in ("2026-09-10T00:00:00", "2026-09-10T02:00:00", "2026-02-30T00:00:00"):
            with self.subTest(cutoff=cutoff), self.assertRaises(RuntimeError):
                prepare.runtime_parameters("https://fixture.trycloudflare.com", cutoff, now)

    def test_partial_digest_set_and_production_receiver_rejected(self):
        now = dt.datetime(2026, 9, 10, 0, 0, tzinfo=dt.timezone.utc)
        with self.assertRaisesRegex(RuntimeError, "three verified"):
            prepare.runtime_parameters("https://fixture.trycloudflare.com", "2026-09-10T01:00:00", now, {"nginx": "sha256:" + "a" * 64})
        with self.assertRaisesRegex(RuntimeError, "dedicated"):
            prepare.runtime_parameters("https://monitor.nexusapi.link", "2026-09-10T01:00:00", now)


if __name__ == "__main__":
    unittest.main()

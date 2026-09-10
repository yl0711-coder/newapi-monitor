"""Local-only review bundle. No AWS credentials, API calls, upload or deploy.

Reads local OrbStack image metadata; writes only a fresh private temporary
directory. Registry manifest pinning is a separate offline verification step.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import tarfile
import tempfile
from pathlib import Path

from ecs_image_acceptance import docker, require
from ecs_split_template import DIGEST_PARAMETERS, split_template

TAGS = {"nginx": "monitor-ecs-nginx-local:ownership-20260910",
        "reject": "monitor-ecs-reject-local:ownership-20260910",
        "synthetic": "monitor-ecs-synthetic-local:split-20260910"}
REVIEWED_COLLECTORS = {
    "nginx": "sha256:e7e45bfbf0cc953c0d8e39a2351b35999ebc01e9147a5b66e4112cd9387f0d33",
    "reject": "sha256:65002308bacce76893e729f12bc4831f5c605dd04a39adfd422b3576509ea902",
}
PARAMETER_BY_KIND = dict(zip(TAGS, DIGEST_PARAMETERS))


def images():
    endpoint = docker("context", "inspect", "orbstack", "--format", "{{.Endpoints.docker.Host}}")
    require(endpoint.startswith("unix:///") and endpoint.endswith("/.orbstack/run/docker.sock"), "local OrbStack required")
    result = {}
    for kind, tag in TAGS.items():
        image = json.loads(docker("image", "inspect", tag))[0]
        require(image["Architecture"] == "amd64" and image["Os"] == "linux", "linux/amd64 image required")
        require(image["Config"]["User"] == "collector", "nonroot candidate required")
        require(re.fullmatch(r"sha256:[a-f0-9]{64}", image["Id"]), "invalid local image ID")
        if kind in REVIEWED_COLLECTORS:
            require(image["Id"] == REVIEWED_COLLECTORS[kind], "reviewed collector image changed")
        result[kind] = {"tag": tag, "local_image_id": image["Id"], "platform": "linux/amd64",
                        "entrypoint": image["Config"]["Entrypoint"], "registry_digest": None}
    return result


def verify_registry_manifest(raw, config_digest):
    """Verify raw ECR single-platform manifest against the reviewed config ID.

    Caller must obtain the exact bytes from the isolated repository AFTER push;
    this function neither retrieves nor uploads anything. An index/list must be
    resolved to its linux/amd64 image manifest first; no implicit platform pick.
    """
    require(isinstance(raw, bytes) and 0 < len(raw) <= 256 * 1024, "bounded manifest bytes required")
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", config_digest), "valid reviewed config digest required")
    def unique(pairs):
        obj = {}
        for key, value in pairs:
            require(key not in obj, "duplicate manifest key")
            obj[key] = value
        return obj
    try:
        manifest = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise RuntimeError("invalid registry manifest JSON") from exc
    require(isinstance(manifest, dict) and type(manifest.get("schemaVersion")) is int and
            manifest["schemaVersion"] == 2, "image manifest schema 2 required")
    require(manifest.get("mediaType") in ("application/vnd.oci.image.manifest.v1+json",
                                         "application/vnd.docker.distribution.manifest.v2+json"), "single-platform image manifest required")
    config = manifest.get("config")
    require(isinstance(config, dict) and config.get("digest") == config_digest, "registry content differs from reviewed local image")
    require(isinstance(manifest.get("layers"), list) and bool(manifest["layers"]), "image layers required")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def exported_config(path):
    """Read only bounded regular JSON members; never extract tar paths."""
    with tarfile.open(path, "r") as archive:
        def read_json(name):
            members = [m for m in archive.getmembers() if m.name == name]
            require(len(members) == 1 and members[0].isfile() and 0 < members[0].size <= 256 * 1024,
                    "one bounded regular image metadata member required")
            with archive.extractfile(members[0]) as stream:
                data = stream.read(256 * 1024 + 1)
            return data, json.loads(data)
        _, manifest = read_json("manifest.json")
        require(isinstance(manifest, list) and len(manifest) == 1, "one exported platform image required")
        config_name = manifest[0].get("Config", "")
        require(re.fullmatch(r"blobs/sha256/[a-f0-9]{64}|[a-f0-9]{64}\.json", config_name), "unexpected config path")
        raw, config = read_json(config_name)
        digest = hashlib.sha256(raw).hexdigest()
        require(digest in config_name, "exported config content hash mismatch")
        require(config.get("architecture") == "amd64" and config.get("os") == "linux", "exported platform mismatch")
        return "sha256:" + digest


def lock_exported_configs(root, image_lock):
    for kind, image in image_lock.items():
        path = root / (kind + "-image.tar")
        require(not path.exists(), "do not overwrite an image export")
        docker("image", "save", "--platform", "linux/amd64", "--output", str(path), image["local_image_id"], timeout=90)
        path.chmod(0o600)
        image["config_digest"] = exported_config(path)
        platform = json.loads(docker("image", "inspect", "--platform", "linux/amd64", image["local_image_id"]))[0]
        image["platform_manifest_digest"] = platform.get("Descriptor", {}).get("digest")
        require(isinstance(image["platform_manifest_digest"], str) and
                re.fullmatch(r"sha256:[a-f0-9]{64}", image["platform_manifest_digest"]), "platform manifest digest required")
        with tarfile.open(path, "r") as archive:
            member = archive.getmember("blobs/sha256/" + image["platform_manifest_digest"].split(":")[1])
            require(member.isfile() and 0 < member.size <= 256 * 1024, "bounded platform manifest required")
            with archive.extractfile(member) as stream:
                manifest = stream.read(256 * 1024 + 1)
        require(verify_registry_manifest(manifest, image["config_digest"]) == image["platform_manifest_digest"],
                "exported manifest differs from inspected platform")
        image["export_manifest_verified"] = True
        # A registry can preserve or convert a manifest media type on push.
        # Config binding remains mandatory; record both rather than confuse
        # the Docker index ID, platform manifest digest and config digest.
        image["export_sha256"] = file_digest(path)


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def runtime_parameters(origin, cutoff, now, pinned=None):
    """Check activation inputs immediately before a separately approved AWS step.

    A pinned map must come from verify_registry_manifest, not Docker IDs.
    This is local syntax/time checking, not proof of repository ownership.
    """
    require(re.fullmatch(r"https://[a-z0-9-]+\.trycloudflare\.com", origin), "dedicated temporary receiver origin required")
    require(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", cutoff), "explicit UTC cutoff required")
    try:
        expires = dt.datetime.strptime(cutoff, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    except ValueError as exc:
        raise RuntimeError("invalid cutoff date") from exc
    require(now.tzinfo is not None, "timezone-aware clock required")
    require(dt.timedelta(minutes=10) <= expires - now <= dt.timedelta(minutes=110), "cutoff must be 10..110 minutes ahead; do not extend an existing run")
    values = {"ReceiverOrigin": origin, "StopAtUTC": cutoff, "EnableTestDefinition": "no"}
    values.update({key: "UNSET" for key in DIGEST_PARAMETERS})
    if pinned is not None:
        require(isinstance(pinned, dict) and set(pinned) == set(TAGS), "three verified image digests required")
        for kind, digest in pinned.items():
            require(isinstance(digest, str) and re.fullmatch(r"sha256:[a-f0-9]{64}", digest), "invalid registry digest")
            values[PARAMETER_BY_KIND[kind]] = digest
        values["EnableTestDefinition"] = "yes"
    return [{"ParameterKey": key, "ParameterValue": value} for key, value in values.items()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    spec = split_template(args.name)  # Reject unscoped names BEFORE Docker reads.
    image_lock = images()
    root = Path(tempfile.mkdtemp(prefix="ecs-split-review-", dir="/private/tmp"))
    lock_exported_configs(root, image_lock)
    artifacts = {"template.json": spec, "local-images.json": image_lock,
                 "review.json": {"name": args.name, "scope": "isolated", "local_checks_passed": True,
                                 "production_ready": False, "aws_resources_created": False,
                                 "max_requested_tasks": 2, "task_vcpu": 0.5, "task_memory_gib": 1,
                                 "max_run_minutes": 110, "initial_desired_count": 0,
                                 "receiver_ownership_enabled": True,
                                 "receiver_containers": {"nginx": ["access", "error", "evidence"], "new-api": ["reject"]},
                                 "remaining": ["approve AWS bootstrap and budget", "set and freeze receiver/cutoff",
                                               "push only isolated ECR; verify three manifests", "activate at desired count zero",
                                               "use split-v1 oracle, not legacy equal-file oracle", "separately authorize task start"]}}
    for name, value in artifacts.items():
        with open(root / name, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
    print(root)


if __name__ == "__main__":
    main()

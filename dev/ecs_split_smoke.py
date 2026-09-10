"""Local network-none smoke for the exact split-producer image and init command.

Reuses the image harness's label-checked cleanup. No AWS emulation or collectors:
this checks the newly added TEST producer, not a replacement end-to-end result.
"""
import json
import re
import tempfile
import time
from pathlib import Path

from ecs_image_acceptance import LocalRun, docker, require
from ecs_split_prepare import images
from ecs_split_template import split_template


def final_oracle(kind, output):
    prefix = "SPLIT_SYNTHETIC_ORACLE "
    entries = [json.loads(line[len(prefix):]) for line in output.splitlines() if line.startswith(prefix)]
    require(bool(entries) and sum(row.get("final") is True for row in entries) == 1 and entries[-1].get("final") is True,
            "one final producer oracle required")
    row = entries[-1]
    require(row.get("schema") == "split-v1" and row.get("kind") == kind and
            re.fullmatch(r"[a-f0-9]{16}", row.get("instance", "")), "invalid oracle identity")
    expected = {"nexusapi_access.jsonl", "error.log"} if kind == "nginx" else {
        "oneapi-20000101.log", "oneapi-20000102.log", "oneapi-20000103.log"}
    require(set(row.get("files", {})) == expected, "wrong final file set")
    require(all(type(count) is int and 0 < count <= 4202 for count in row["files"].values()), "invalid final file counts")
    if kind == "new-api":
        require(row["files"]["oneapi-20000103.log"] == 1, "tail-created file must contain one record")
    return row


def verify_files(root, oracle):
    require({p.name for p in root.iterdir()} == set(oracle["files"]), "retained file inventory differs from oracle")
    for name, count in oracle["files"].items():
        path = root / name
        require(not path.is_symlink() and path.is_file(), "regular log files required")
        data = path.read_bytes()
        require(data.endswith(b"\n") and data.count(b"\n") == count, "retained line count differs from oracle")


def main():
    lock = images()  # local context and reviewed collector IDs checked first
    image = lock["synthetic"]["local_image_id"]
    spec = split_template("monitor-ecs-acceptance-split-smoke")
    containers = {c["Name"]: c for c in spec["Resources"]["TaskDefinition"]["Properties"]["ContainerDefinitions"]}
    root = Path(tempfile.mkdtemp(prefix="ecs-split-smoke-", dir="/private/tmp"))
    print("Local split producer evidence: " + str(root), flush=True)
    run = LocalRun(root, "producers", {"fixture": "not-used"})
    try:
        volumes = {name: run.volume(name) for name in ("NginxLogs", "RejectLogs", "NginxState", "RejectState")}
        init = containers["nginxcollector-init"]
        options = ["--network", "none", "--user", "0:0", "--entrypoint", "/bin/sh",
                   "--cap-add", "CHOWN", "--cap-add", "FOWNER", "--cap-add", "DAC_OVERRIDE"]
        for m in init["MountPoints"]:
            options += ["--mount", f"type=volume,src={volumes[m['SourceVolume']]},dst={m['ContainerPath']}"]
        cid = run.start("init", image, options, ["-ec", *init["Command"]])
        require(docker("wait", cid, timeout=20) == "0", "init failed")
        for kind in ("new-api", "nginx"):
            c = containers[kind]
            m = c["MountPoints"][0]
            options = ["--network", "none", "--user", "100:101",
                       "--mount", f"type=volume,src={volumes[m['SourceVolume']]},dst={m['ContainerPath']}"]
            for env in c["Environment"]:
                options += ["-e", env["Name"] + "=" + env["Value"]]
            cid = run.start(kind, image, options)
            ready = False
            for _ in range(40):
                try:
                    docker("exec", cid, "/usr/local/bin/synthetic", "--health", timeout=5)
                    ready = True
                    break
                except RuntimeError:
                    time.sleep(0.25)
            require(ready, "producer health check failed")
        oracles = {}
        for kind in ("nginx", "new-api"):
            run.stop(kind, expected=True)
            cid = run.containers[kind]
            oracles[kind] = final_oracle(kind, docker("logs", cid))
            target = run.root / kind
            target.mkdir(mode=0o700)
            path = containers[kind]["MountPoints"][0]["ContainerPath"]
            docker("cp", cid + ":" + path + "/.", str(target))
            verify_files(target, oracles[kind])
    finally:
        run.cleanup()
    result = {"passed": True, "production_ready": False, "aws_verified": False,
              "image_id": image, "oracles": oracles, "local_resources_removed": True}
    (root / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

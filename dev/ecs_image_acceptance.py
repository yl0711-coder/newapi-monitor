"""Run unchanged local candidate images without any external container network.

All AWS endpoints are fake TLS servers in a network-none namespace. No Docker
socket is mounted; no host credential/dataset directory is mounted. This does
not test AWS IAM or certify production deployment dependencies.
"""
import argparse
import json
import re
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

LABEL = "monitor.ecs.image-acceptance"
IMAGES = {kind: f"monitor-ecs-{kind}-local:ownership-20260910" for kind in ("nginx", "reject")}
FIXTURE = "monitor-ecs-fixture-local:shutdown-20260910"
HOSTS = ["monitor.fixture.invalid", "fixture.execute-api.us-west-2.amazonaws.com",
         "sts.us-west-2.amazonaws.com", "fixture-image-archive.s3.us-west-2.amazonaws.com"]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def docker(*args, timeout=60):
    result = subprocess.run(["docker", "--context", "orbstack", *args],
                            capture_output=True, text=True, timeout=timeout)
    require(result.returncode == 0, f"local docker {args[0]} failed: {result.stderr[-2000:]}")
    return (result.stdout + result.stderr if args[0] == "logs" else result.stdout).strip()


def local_images():
    endpoint = docker("context", "inspect", "orbstack", "--format", "{{.Endpoints.docker.Host}}")
    require(endpoint.startswith("unix:///") and endpoint.endswith("/.orbstack/run/docker.sock"), "local OrbStack Unix socket required")
    images = {}
    for name, tag in {**IMAGES, "fixture": FIXTURE}.items():
        data = json.loads(docker("image", "inspect", tag))[0]
        require(data["Architecture"] == "amd64" and data["Os"] == "linux", "linux/amd64 candidate required")
        if name != "fixture":
            require(data["Config"]["User"] == "collector", "unchanged non-root collector image required")
        images[name] = data["Id"]
    return images


class LocalRun:
    def __init__(self, root, scenario, images):
        self.root, self.scenario, self.images = root / scenario, scenario, images
        self.root.mkdir(mode=0o700)
        self.name = "monitor-image-test-" + uuid.uuid4().hex[:12]
        self.containers, self.volumes = {}, []
        self.receiver = None

    def volume(self, name):
        value = self.name + "-" + name
        docker("volume", "create", "--label", f"{LABEL}={self.name}", value)
        self.volumes.append(value)
        return value

    def start(self, role, image, options, command=()):
        if image == self.images["fixture"]:
            # The Monitor base declares three data volumes. Override them for
            # test-only helper containers so Docker cannot leave anonymous data.
            options = ["--tmpfs", "/data:rw,size=16m", "--tmpfs", "/backup:rw,size=16m",
                       "--tmpfs", "/evidence:rw,size=16m", *options]
        cid = docker("create", "--pull", "never", "--name", self.name + "-" + role,
                     "--label", f"{LABEL}={self.name}", "--platform", "linux/amd64", "--read-only",
                     "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
                     "--memory", "512m", "--cpus", "1", "--tmpfs", "/tmp:rw,nosuid,nodev,size=192m",
                     *options, image, *command)
        require(re.fullmatch(r"[a-f0-9]{64}", cid), "unexpected local container identity")
        self.containers[role] = cid
        (self.root / "containers.json").write_text(json.dumps(self.containers, indent=2) + "\n")
        record = json.loads(docker("inspect", cid))[0]
        require(all(m["Type"] == "tmpfs" or m["Type"] == "volume" and m["Name"] in self.volumes
                    for m in record["Mounts"]), "unexpected host bind or anonymous volume in test container")
        docker("start", cid)
        return cid

    def control(self, path, body=None):
        args = ["exec", self.receiver, "wget", "-q", "-O", "-", "-T", "20"]
        if body is not None:
            args += ["--post-data=" + json.dumps(body)]
        return json.loads(docker(*args, "http://127.0.0.1:8080" + path, timeout=25))

    def wait_status(self, predicate, seconds=45):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                if predicate(self.control("/status")):
                    return
            except (RuntimeError, json.JSONDecodeError):
                pass
            time.sleep(0.5)
        raise RuntimeError("local fixture readiness/initial collection timed out")

    def stop(self, role, expected):
        cid = self.containers[role]
        docker("kill", "--signal", "TERM", cid)
        code = int(docker("wait", cid, timeout=50))
        require((code == 0) == expected, f"{role} unexpected exit code {code}")
        return code

    def producer(self, role, volume):
        cid = self.start(role, self.images["fixture"], ["--network", "none", "--user", "100:101",
            "--mount", f"type=volume,src={volume},dst=/logs",
            "-e", "MONITOR_TEST_ECS_IMAGE_FIXTURE=" + role],
            ["-test.run=^TestECSLogImageProducerFixture$", "-test.v"])
        self.control("/producer", {"Name": role, "DockerId": cid, "KnownStatus": "RUNNING"})

    def collector(self, kind, state, logs, shared):
        env = {
            "ECSLOG_SCOPE": "isolated", "ECSLOG_KIND": kind,
            "ECSLOG_SERVICE_ARN": "arn:aws:ecs:us-west-2:123456789012:service/fixture/worker",
            "ECSLOG_PRODUCER_CONTAINER": "nginx" if kind == "nginx" else "new-api",
            "ECSLOG_MONITOR_URL": "https://monitor.fixture.invalid",
            "ECSLOG_REGISTER_URL": "https://fixture.execute-api.us-west-2.amazonaws.com/isolated/register",
            "ECSLOG_AUDIENCE": "fixture-local-monitor", "ECSLOG_STATE_ROOT": "/data/ecs",
            "ECSLOG_ARCHIVE_ENABLED": "true", "ECSLOG_ARCHIVE_BUCKET": "fixture-image-archive",
            "ECSLOG_ARCHIVE_PREFIX": "isolated/", "ECSLOG_DEFERRED_ARCHIVE_ACK": "true",
            "ECSLOG_ARCHIVE_CLOSURE": "true", "ECSLOG_FINAL_LOG_ROOT": "/logs",
            "ECSLOG_FINAL_FILE_CONTRACT": "newapi-files-v1",
            "ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4/fixture-image",
            "SSL_CERT_FILE": "/trust/ca.pem", "AWS_REGION": "us-west-2",
            "AWS_ACCESS_KEY_ID": "fixture-id", "AWS_SECRET_ACCESS_KEY": "fixture-secret",
            "AWS_SESSION_TOKEN": "fixture-session", "AWS_EC2_METADATA_DISABLED": "true",
            "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY": "k" * 32, "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID": "key-1",
            "NGINXCOLLECTOR_INTERVAL_SECONDS": "3600", "NGINXCOLLECTOR_ERROR_TIMEZONE": "UTC",
            "COLLECTOR_FLUSH_SECONDS": "3600", "COLLECTOR_LOG_TIMEZONE": "UTC",
        }
        options = ["--network", "container:" + self.receiver,
                   "--mount", f"type=volume,src={state},dst=/data/ecs",
                   "--mount", f"type=volume,src={logs},dst=/logs,readonly",
                   "--mount", f"type=volume,src={shared},dst=/trust,readonly"]
        for name, value in env.items():
            options += ["-e", name + "=" + value]
        self.start(kind + "-collector", self.images[kind], options)

    def run(self):
        shared = self.volume("shared")
        logs = {k: self.volume(k + "-logs") for k in IMAGES}
        state = {k: self.volume(k + "-state") for k in IMAGES}
        options = ["--network", "none", "--user", "0:0", "--cap-add", "NET_ADMIN",
                   "--cap-add", "NET_BIND_SERVICE", "--cap-add", "CHOWN", "--cap-add", "DAC_OVERRIDE",
                   "--entrypoint", "/bin/sh", "-e", "MONITOR_TEST_ECS_IMAGE_FIXTURE=receiver",
                   "--mount", f"type=volume,src={shared},dst=/fixture-shared"]
        for host in HOSTS:
            options += ["--add-host", host + ":169.254.170.2"]
        for k in IMAGES:
            options += ["--mount", f"type=volume,src={logs[k]},dst=/logs-{k}",
                        "--mount", f"type=volume,src={state[k]},dst=/state-{k}"]
        self.receiver = self.start("receiver", self.images["fixture"], options,
            ["-ec", "ip addr add 169.254.170.2/32 dev lo; exec /fixture.test -test.run=^TestECSLogImageReceiverFixture$ -test.v"])
        isolated = json.loads(docker("inspect", self.receiver))[0]
        require(isolated["HostConfig"]["NetworkMode"] == "none" and not isolated["HostConfig"]["PortBindings"], "fixture must not expose or join a network")
        self.wait_status(lambda _: True, seconds=20)
        # START order: collectors precede their own producer; registration waits.
        for k in IMAGES:
            self.collector(k, state[k], logs[k], shared)
        self.producer("new-api", logs["reject"])
        self.producer("nginx", logs["nginx"])
        self.wait_status(lambda s: s["lanes"] == 4)
        if self.scenario == "wrong-stop-order":
            for k in IMAGES:
                self.stop(k + "-collector", expected=False)
        for role in ["nginx", "new-api"]:
            code = self.stop(role, expected=True)
            oracle = docker("logs", self.containers[role])
            require("final=true" in oracle, "producer final append oracle missing")
            self.control("/producer", {"Name": role, "DockerId": self.containers[role], "KnownStatus": "STOPPED", "ExitCode": code})
        if self.scenario == "normal":
            for k in IMAGES:
                self.stop(k + "-collector", expected=True)
        # Remove actual task containers before independent archive verification.
        for role in list(self.containers):
            if role != "receiver":
                self.remove_container(role)
        result = self.control("/verify?expect=" + ("complete" if self.scenario == "normal" else "gap"), {})
        require(result["passed"] is True and result["production_ready"] is False, "unexpected acceptance result")
        result["candidate_images"] = self.images
        (self.root / "report.json").write_text(json.dumps(result, indent=2) + "\n")
        return result

    def remove_container(self, role):
        cid = self.containers[role]
        record = json.loads(docker("inspect", cid))[0]
        require(record["Config"]["Labels"].get(LABEL) == self.name, "refuse unrelated container cleanup")
        (self.root / (role + ".log")).write_text(docker("logs", cid))
        docker("rm", "-f", "-v", cid)
        del self.containers[role]

    def cleanup(self):
        failures = []
        for role in list(self.containers):
            try:
                self.remove_container(role)
            except Exception as exc:
                failures.append(str(exc))
        for volume in self.volumes:
            try:
                record = json.loads(docker("volume", "inspect", volume))[0]
                require(record["Labels"].get(LABEL) == self.name, "refuse unrelated volume cleanup")
                docker("volume", "rm", volume)
            except Exception as exc:
                failures.append(str(exc))
        require(not failures, "local test cleanup incomplete: " + "; ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=["normal", "wrong-stop-order", "all"], default="all")
    args = parser.parse_args()
    images = local_images()
    root = Path(tempfile.mkdtemp(prefix="ecs-image-acceptance-", dir="/private/tmp"))
    print("Local image acceptance evidence: " + str(root), flush=True)
    scenarios = ["normal", "wrong-stop-order"] if args.scenario == "all" else [args.scenario]
    for scenario in scenarios:
        run = LocalRun(root, scenario, images)
        try:
            result = run.run()
        finally:
            run.cleanup()
        result["local_resources_removed"] = True
        (run.root / "report.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

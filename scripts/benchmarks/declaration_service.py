"""The real in-memory Schema Service, built and owned only on a GitHub runner."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from urllib.request import urlopen

from node import file_hash, save


def build_service(output):
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("The real service build belongs in CI")
    root = Path(__file__).resolve().parents[2]
    target = Path(os.environ["RUNNER_TEMP"]) / "declaration-service-build"
    if target.exists():
        raise RuntimeError("The service target is not a fresh owned path")
    command = ["cargo", "build", "--locked", "--profile", "dev-release",
               "-p", "schema_service_server_http", "--bin", "schema_service"]
    with (output / "service-build.log").open("w") as log:
        subprocess.run(command, cwd=root, env=dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="3"),
                       stdout=log, stderr=subprocess.STDOUT, timeout=2400, check=True)
    binary = Path(os.environ["RUNNER_TEMP"]) / "cold-bins" / "schema_service"
    shutil.copy2(target / "dev-release" / "schema_service", binary)
    save(output / "service-build.json", {"revision": os.environ["DRIVER_SHA"], "sha256": file_hash(binary),
                                        "command": command, "default_embedder": "disabled"})
    shutil.rmtree(target)
    return binary


class Service:
    def __init__(self, binary, output):
        self.binary, self.output, self.process, self.log = binary, output, None, None
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"

    def health(self):
        with urlopen(self.url + "/v1/health", timeout=5) as response:
            result = json.load(response)
            if result.get("status") != "healthy":
                raise RuntimeError("The real service health is not healthy")
            return result

    def start(self):
        # No inherited credentials, catalog addresses, app roots, or mutation settings.
        environment = {k: os.environ[k] for k in ("PATH", "HOME", "LANG") if k in os.environ}
        environment.update(OBS_FILE_PATH=str(self.output / "service-observability.jsonl"),
                           RUST_LOG="warn", RUST_MIN_STACK="16777216")
        self.log = (self.output / "service.log").open("w")
        self.process = subprocess.Popen([str(self.binary), "--port", str(self.port)], env=environment,
                                        stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned real service exited before health; inspect service.log")
            try:
                self.health()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("The real service missed its health deadline")

    def close(self):
        if self.process is None:
            return
        forced = False
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                forced = True
                self.process.kill()
                self.process.wait(timeout=5)
        code = self.process.returncode
        self.process = None
        self.log.close()
        save(self.output / "service-shutdown.json", {"exit_code": code, "forced": forced})
        if forced or code not in (0, -15):
            raise RuntimeError("The owned real service did not stop normally")

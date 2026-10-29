#!/usr/bin/env python3
"""Run the real S3 adapter against one owned, disposable MinIO process in CI."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minio", type=Path, required=True)
    parser.add_argument("--test-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("This check runs only in GitHub Actions")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    if args.output.exists() or not args.output.parent.resolve(strict=True).is_relative_to(runner):
        raise RuntimeError("A fresh output directory under RUNNER_TEMP is required")
    args.output.mkdir(mode=0o700)
    data = args.output / "minio-data"
    data.mkdir(mode=0o700)
    data_inode = data.stat().st_ino
    environment = dict(os.environ)
    environment.update(
        MINIO_ROOT_USER="schema-ci-user", MINIO_ROOT_PASSWORD="disposable-schema-ci-password",
        MINIO_BROWSER="off", MINIO_UPDATE="off", AWS_ACCESS_KEY_ID="schema-ci-user",
        AWS_SECRET_ACCESS_KEY="disposable-schema-ci-password", AWS_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true", SCHEMA_S3_TEST_ENDPOINT="http://127.0.0.1:9000",
    )
    for name in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "SCHEMA_S3_TEST_MODE", "SCHEMA_S3_TEST_BUCKET"):
        environment.pop(name, None)
    result = {"source": os.environ["GITHUB_SHA"], "passed": False,
              "scope": "Loopback S3 protocol and fault injection; not AWS or Lambda evidence"}
    with (args.output / "minio.log").open("w") as log:
        process = subprocess.Popen(
            [str(args.minio), "server", str(data),
             "--address", "127.0.0.1:9000", "--console-address", "127.0.0.1:9101"],
            env=environment, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("The owned MinIO process exited before readiness")
                try:
                    with urllib.request.urlopen("http://127.0.0.1:9000/minio/health/ready", timeout=2) as reply:
                        if reply.status == 200:
                            break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.2)
            else:
                raise RuntimeError("The owned MinIO process missed its readiness deadline")
            listed = subprocess.run([str(args.test_binary), "--list"], capture_output=True,
                                    text=True, timeout=10, check=True)
            names = [line.split(": ", 1)[0] for line in listed.stdout.splitlines()
                     if "protocol_" in line and line.endswith(": test")]
            if len(names) != 6:
                raise RuntimeError("The explicit target must expose all six protocol cases")
            result["discovered_cases"] = names
            completed = subprocess.run(
                [str(args.test_binary), "protocol_", "--test-threads=1", "--nocapture"],
                env=environment, timeout=600, check=False,
            )
            result["exit_code"] = completed.returncode
            result["passed"] = completed.returncode == 0
            return completed.returncode
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            result["owned_minio_stopped"] = process.poll() is not None
            if (data.is_symlink() or data.resolve().parent != args.output.resolve()
                    or data.stat().st_uid != os.getuid() or data.stat().st_ino != data_inode):
                raise RuntimeError("The owned MinIO data identity changed; cleanup refused")
            shutil.rmtree(data)
            result["owned_minio_data_removed"] = True
            (args.output / "protocol-result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    sys.exit(main())

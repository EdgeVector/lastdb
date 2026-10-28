#!/usr/bin/env python3
"""Run only the exact CI executable pair with bounded private output."""

import hashlib
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time

from support import CHUNK, OperationError, decode_json, require, stable


def execute(command, environment, input_bytes=None, timeout=120):
    process = subprocess.Popen(command, env=environment, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    output = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": 4 * CHUNK, "stderr": 65536}
    deadline = time.monotonic() + timeout
    try:
        # Only a bounded recovery phrase or API key enters stdin, never argv.
        require(input_bytes is None or len(input_bytes) <= 4096, "stdin_size")
        if input_bytes:
            process.stdin.write(input_bytes)
        process.stdin.close()
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                require(time.monotonic() < deadline, "cli_deadline")
                for key, _mask in selector.select(min(0.25, max(0, deadline - time.monotonic()))):
                    block = os.read(key.fileobj.fileno(), 65536)
                    if not block:
                        selector.unregister(key.fileobj)
                    else:
                        target = output[key.data]
                        require(len(target) + len(block) <= limits[key.data], "cli_output_size")
                        target.extend(block)
        process.wait(timeout=max(0.1, deadline - time.monotonic()))
        require(process.returncode == 0, "product_cli_refused")
        return bytes(output["stdout"])
    except subprocess.TimeoutExpired as error:
        raise OperationError("cli_deadline") from error
    finally:
        if process.poll() is None:
            # This group belongs to this CLI invocation, never a daemon or primary.
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()
        if not process.stdin.closed:
            process.stdin.close()


def executable(path):
    require(not path.is_symlink(), "binary_symlink")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    value = hashlib.sha256()
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_mode & 0o111
                and 0 < before.st_size <= 512 * CHUNK, "binary_file")
        for block in iter(lambda: stream.read(CHUNK), b""):
            value.update(block)
        require(stable(before, os.fstat(stream.fileno())), "binary_changed")
    return {"sha256": value.hexdigest(), "bytes": before.st_size}


class Programs:
    def __init__(self, directory, version, environment):
        require(directory.is_absolute() and directory.resolve() == directory
                and directory.is_dir(), "binary_directory")
        self.directory, self.version, self.environment = directory, version, environment
        self.identities = {}
        for name in ("lastdb", "lastdbd"):
            path = directory / name
            self.identities[name] = executable(path)
            raw = execute([str(path), "--version"], environment, timeout=20)
            require(raw.decode("utf-8").strip() == f"{name} {version}", "binary_version")

    def verify(self):
        for name, identity in self.identities.items():
            require(executable(self.directory / name) == identity, "binary_pair_changed")

    def command(self, home, arguments, input_bytes=None):
        self.verify()
        environment = dict(self.environment, LASTDB_BYTE_BACKUP_V2_ENABLED="true")
        command = [str(self.directory / "lastdb"), "--data-dir", str(home), *arguments]
        return decode_json(execute(command, environment, input_bytes))

    def restore(self, home, destination, baseline, phrase):
        self.verify()
        environment = dict(self.environment, LASTDB_BYTE_BACKUP_V2_ENABLED="true")
        command = [str(self.directory / "lastdb"), "--data-dir", str(home), "cloud",
                   "backup-v2", "restore", "--recovery-phrase-stdin", "--stream",
                   baseline["stream_id"], "--generation", baseline["account_generation"],
                   "--commit", baseline["commit_id"], "--commit-sha256", baseline["commit_sha256"],
                   "--into", str(destination), "--json"]
        return decode_json(execute(command, environment, phrase, timeout=1200))

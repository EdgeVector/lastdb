"""Run only the exact CI executable pair and own one Mini daemon."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import (  # noqa: E402
    UnixConnection as Connection,
    classify_socket_refusal,
    poll_until,
    stop_process,
    verify_shutdown_receipt,
)
from support import ACCOUNT, CHUNK, CREDENTIALS_FILE, OperationError, decode_json, read_json, require, stable  # noqa: E402


def command_words(command):
    """The subcommand words of a CLI call, without the binary, flags or values."""
    words, arguments = [], list(command[1:])
    while arguments and arguments[0] == "--data-dir":
        arguments = arguments[2:]
    for argument in arguments[:3]:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", argument):
            break
        words.append(argument)
    return " ".join(words)


def error_head(message):
    """Leading static words of an error chain without ids, paths or numbers."""
    head = re.match(rb"[A-Za-z _:'-]{0,160}", message)[0].rstrip(b" :")
    return head.decode("ascii") or None


def cli_refusal(stderr, exit_code, command=None):
    """Project only closed source labels; never return a captured byte or path."""
    result = {"exit_code": exit_code, "reason": "unrecognized"}
    if command:
        result["command"] = command_words(command)
    lines = bytes(stderr).splitlines()
    if exit_code == -signal.SIGABRT:
        if b"fatal runtime error: stack overflow, aborting" in lines:
            return {**result, "reason": "rust_stack_overflow"}
        if b"LastDB panic" in lines:
            return {**result, "reason": "lastdb_panic"}
    errors = [line[len(b"error: "):] for line in lines if line.startswith(b"error: ")]
    if len(errors) != 1:
        return result
    message = errors[0]
    result["error_head"] = error_head(message)
    routed = re.match(rb"(/api/[a-z0-9/-]{1,64}) returned HTTP/1\.1 ([0-9]{3})[^:]*: (.*)", message)
    if routed:
        result["route"] = routed[1].decode("ascii")
        result["http_status"] = int(routed[2])
        try:
            body = decode_json(routed[3])
        except OperationError:
            body = None
        detail = body.get("error") if isinstance(body, dict) else None
        text = detail.encode() if isinstance(detail, str) else routed[3] if body is None else b""
        result["error_head"] = error_head(text)
    return result


def execute(command, environment, input_bytes=None, timeout=120):
    process = subprocess.Popen(command, env=environment, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    output = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": 4 * CHUNK, "stderr": 65536}
    deadline = time.monotonic() + timeout
    try:
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
        if process.returncode != 0:
            raise OperationError("product_cli_refused",
                                 cli_refusal=cli_refusal(output["stderr"], process.returncode, command))
        return bytes(output["stdout"])
    except subprocess.TimeoutExpired as error:
        raise OperationError("cli_deadline") from error
    finally:
        if process.poll() is None:
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

    def command(self, home, arguments, input_bytes=None, extra=None, timeout=120):
        self.verify()
        environment = dict(self.environment)
        environment.update(extra or {})
        command = [str(self.directory / "lastdb")]
        if home is not None:
            command.extend(("--data-dir", str(home)))
        command.extend(arguments)
        return decode_json(execute(command, environment, input_bytes, timeout))

    def connect_existing(self, node, api_key, code="dev_connection", timeout=30):
        raw = api_key if isinstance(api_key, bytes) else api_key.encode()
        receipt = self.command(node.home, [
            "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
            "--expected-user-hash", node.account, "--json",
        ], input_bytes=raw, timeout=timeout)
        require(receipt.get("ok") is True and receipt.get("environment") == "dev"
                and receipt.get("credentials_saved") is True
                and not receipt.get("registration_attempted")
                and receipt.get("account_id") == node.account, code)
        return receipt


def socket_refusal(path, response):
    """Keep the status and closed labels; never include a response body or path."""
    return classify_socket_refusal(
        path, response, decode_json_fn=decode_json, ignored_decode_errors=(OperationError,)
    )


class OwnedNode:
    def __init__(self, home, programs, account=ACCOUNT, *, isolated_copy=False, keep_saved_credentials=False):
        self.home, self.programs = home, programs
        require(type(keep_saved_credentials) is bool, "keep_saved_credentials_flag")
        self.keep_saved_credentials = keep_saved_credentials
        require(isinstance(account, str) and len(account) == 32 and all(c in "0123456789abcdef" for c in account),
                "owned_account")
        self.account = account
        self.socket = home / "data/folddb.sock"
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops = 0
        self.isolated_copy = isolated_copy
        require(len(str(self.socket).encode()) + len(".tmp") < 104, "socket_path_size")

    def call(self, path, body=None):
        connection = Connection(self.socket, timeout=30)
        headers = {"X-LastDB-Client": "cloud-backup-ci", "Content-Type": "application/json"}
        content = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        try:
            connection.request("GET" if body is None else "POST", path, content, headers)
            response = connection.getresponse()
            if response.status != 200:
                raise OperationError("socket_http_status", socket_refusal=socket_refusal(path, response))
            raw = response.read(4 * CHUNK + 1)
            require(len(raw) <= 4 * CHUNK, "socket_json_size")
            return decode_json(raw)
        finally:
            connection.close()

    def offline(self):
        if not self.keep_saved_credentials:
            require(not os.path.lexists(self.home / CREDENTIALS_FILE), "restored_home_cloud_off")

    def disconnect(self, *, remove_saved=False):
        if remove_saved:
            (self.home / CREDENTIALS_FILE).unlink(missing_ok=True)

    def start(self):
        require(self.process is None and not self.socket.exists(), "owned_boot_state")
        self.offline()
        self.programs.verify()
        environment = dict(
            self.programs.environment,
            LASTDB_ISOLATED_COPY="1" if self.isolated_copy else "0",
            LASTDB_TTL_SWEEP="0",
        )
        self.process = subprocess.Popen(
            [str(self.programs.directory / "lastdbd"), "--data-dir", str(self.home)],
            env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )

        def check_boot():
            require(self.process.poll() is None, "daemon_start_exit")
            try:
                return self.health()
            except (OSError, http.client.HTTPException):
                return False

        poll_until(120, check_boot, interval=0.2, timeout_error=OperationError("daemon_start_deadline"))

    def health(self):
        paths = ("/health", "/api/status", "/api/system/boot-identity", "/api/db/native-status")
        with ThreadPoolExecutor(max_workers=len(paths)) as pool:
            health, status, boot, native = list(pool.map(self.call, paths))
        require(self.process is not None and self.process.poll() is None, "daemon_health_exit")
        instance = health["instance_id"]
        prefix = str(self.process.pid) + "-"
        require(health["status"] == "ok" and isinstance(instance, str) and instance.startswith(prefix)
                and instance[len(prefix):].isdigit() and int(instance[len(prefix):]) > 0
                and self.instance_id in (None, instance), "daemon_health_instance")
        require(status["user_hash"] == self.account and status["status"]["build"]["version"] == self.programs.version
                and boot["pid"] == self.process.pid and boot["build"] == self.programs.version, "daemon_identity")
        start = boot["process_start_ts"]
        require(type(start) is int and start > 0 and self.start_ts in (None, start)
                and status["status"]["process_start_ts"] == start, "daemon_start_identity")
        self.start_ts, self.instance_id = start, instance
        require(native["native_disk"]["writer_failed"] is False, "daemon_native_health")
        self.offline()
        return True

    def stop(self, *, remove_saved=False):
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            boot = self.call("/api/system/boot-identity")
            require(boot["pid"] == process.pid and boot["process_start_ts"] == self.start_ts, "daemon_stop_identity")
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=210)
            except subprocess.TimeoutExpired as error:
                raise OperationError("daemon_clean_stop_deadline") from error
        require(process.returncode == 0 and self.start_ts is not None, "daemon_clean_exit")
        receipt = read_json(self.home / ".shutdown_flush_ready", 4096)
        require(verify_shutdown_receipt(receipt, process.pid, self.start_ts) and receipt.get("version") == 1,
                "daemon_flush_receipt")
        require(not self.socket.exists() and not (self.home / "current-session.json").exists(),
                "daemon_shutdown_complete")
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops += 1
        self.disconnect(remove_saved=remove_saved)
        self.offline()

    def cleanup_stop(self):
        if self.process is None:
            return
        stop_process(self.process, timeout=210, kill_timeout=10)
        self.process = None
        self.disconnect()

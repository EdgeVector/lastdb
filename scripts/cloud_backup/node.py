#!/usr/bin/env python3
"""Read restored and shared records through one explicitly owned Mini daemon."""

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness.daemon import poll_until, stop_process, verify_shutdown_receipt  # noqa: E402
from harness.uds import SOCKET_OPERATIONS, SOCKET_REFUSALS, UnixConnection as Connection, classify_socket_refusal  # noqa: E402,F401
from support import ACCOUNT, CHUNK, CREDENTIALS_FILE, OperationError, decode_json, read_json, require  # noqa: E402


def socket_refusal(path, response):
    """Keep the status and closed labels; never include a response body or path."""
    return classify_socket_refusal(
        path,
        response,
        decode_json_fn=decode_json,
        ignored_decode_errors=(OperationError,),
    )


class OwnedNode:
    def __init__(self, home, programs, account=ACCOUNT, *, isolated_copy=False, keep_saved_credentials=False):
        self.home, self.programs = home, programs
        require(type(keep_saved_credentials) is bool, "keep_saved_credentials_flag")
        self.keep_saved_credentials = keep_saved_credentials
        require(
            isinstance(account, str) and len(account) == 32 and all(c in "0123456789abcdef" for c in account),
            "owned_account",
        )
        self.account = account
        self.socket = home / "data/folddb.sock"
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops, self.forced = 0, False
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

    def start(self, *, sign_in=True):
        require(self.process is None and not self.socket.exists(), "owned_boot_state")
        require(type(sign_in) is bool, "sign_in_flag")
        self.offline()
        self.programs.verify()
        environment = dict(
            self.programs.environment,
            LASTDB_ISOLATED_COPY="1" if self.isolated_copy else "0",
            LASTDB_TTL_SWEEP="0",
        )
        self.process = subprocess.Popen(
            [str(self.programs.directory / "lastdbd"), "--data-dir", str(self.home)],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
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
        require(
            health["status"] == "ok"
            and isinstance(instance, str)
            and instance.startswith(prefix)
            and instance[len(prefix) :].isdigit()
            and int(instance[len(prefix) :]) > 0
            and self.instance_id in (None, instance),
            "daemon_health_instance",
        )
        require(
            status["user_hash"] == self.account
            and status["status"]["build"]["version"] == self.programs.version
            and boot["pid"] == self.process.pid
            and boot["build"] == self.programs.version,
            "daemon_identity",
        )
        start = boot["process_start_ts"]
        require(
            type(start) is int
            and start > 0
            and self.start_ts in (None, start)
            and status["status"]["process_start_ts"] == start,
            "daemon_start_identity",
        )
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
        require(
            not self.socket.exists() and not (self.home / "current-session.json").exists(),
            "daemon_shutdown_complete",
        )
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops += 1
        self.disconnect(remove_saved=remove_saved)
        self.offline()

    def cleanup_stop(self):
        if self.process is None:
            return
        self.forced = stop_process(self.process, timeout=210, kill_timeout=10) or self.forced
        self.process = None
        self.disconnect()

    def exit_report(self):
        state = "stopped" if self.process is None else self.process.poll()
        try:
            reports = sorted((self.home / "crash-reports").glob("*.txt"))[:4]
            locations = [
                line.partition(":")[2].strip()
                for report in reports
                for line in report.read_text(errors="replace").splitlines()[:16]
                if line.startswith("location:")
            ]
        except OSError:
            locations = ["unreadable"]
        return {"home": self.home.name, "exit": state, "crash_locations": locations}

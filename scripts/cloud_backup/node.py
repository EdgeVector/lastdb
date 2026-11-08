#!/usr/bin/env python3
"""Read the restored records through one explicitly owned Mini daemon."""

import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
from concurrent.futures import ThreadPoolExecutor
import signal
import socket
import subprocess
import time

from support import (ACCOUNT, BACKUP_DIR, CHUNK, CREDENTIALS_FILE, OperationError, canonical_uuid, decode_json,
                     read_json, require)


SOCKET_OPERATIONS = {
    "/health": "health",
    "/api/status": "status",
    "/api/system/boot-identity": "boot_identity",
    "/api/db/native-status": "native_status",
    "/api/queries/batch": "query_batch",
    "/api/mutations/batch": "mutation_batch",
    "/api/db/fetch-blob": "fetch_file",
}
SOCKET_REFUSALS = {
    "cas_conflict", "compaction_behind", "field_value_too_large", "persist_queue_full",
    "read_resource_limit", "storage_full", "write_resource_limit",
}


def socket_refusal(path, response):
    """Keep the status and closed labels; never include a response body or path."""
    refusal = {"operation": SOCKET_OPERATIONS.get(path, "other"),
               "status": response.status, "reason": "unrecognized"}
    try:
        raw = response.read(4097)
    except (OSError, http.client.HTTPException):
        return refusal
    if len(raw) > 4096:
        return refusal
    if raw.startswith(b"node is busy: too many concurrent reads; retry after "):
        refusal["reason"] = "read_busy"
        return refusal
    # The owner body is this sentence alone. A cloud failure uses a longer
    # 503, so the status is not enough.
    if raw == b"sign in to fetch this file":
        refusal["reason"] = "sign_in_required"
        return refusal
    try:
        body = decode_json(raw)
    except OperationError:
        return refusal
    code = body.get("error") if isinstance(body, dict) else None
    if isinstance(code, str) and code in SOCKET_REFUSALS:
        refusal["reason"] = code
    return refusal


class Connection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=30)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class OwnedNode:
    def __init__(self, home, programs, backup_source=False, ci_stream_id=None, terminal_r2=False,
                 account=ACCOUNT, *, isolated_copy=False, cloud_credentials=None,
                 device_sign_in=False):
        self.home, self.programs = home, programs
        # A restored device fetches a record's file from B2 on its first read.
        # device_sign_in registers a new device on this home and saves that
        # device's own credentials. The copy path is the older workaround.
        # Backup stays off on a restored home either way.
        self.cloud_credentials = cloud_credentials
        require(type(device_sign_in) is bool, "device_sign_in_flag")
        require(not device_sign_in or cloud_credentials is not None, "device_sign_in_account")
        self.device_sign_in = device_sign_in
        require(isinstance(account, str) and len(account) == 32
                and all(char in "0123456789abcdef" for char in account), "owned_account")
        self.account = account
        self.socket = home / "data/folddb.sock"
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops, self.forced = 0, False
        self.backup_source = backup_source
        self.shared_sync = None
        self.isolated_copy = backup_source or isolated_copy
        require(type(terminal_r2) is bool and (not terminal_r2 or backup_source), "owned_ci_upload_mode")
        require(ci_stream_id is None or (backup_source and terminal_r2), "owned_ci_stream_mode")
        self.ci_stream_id = None if ci_stream_id is None else canonical_uuid(ci_stream_id)
        self.terminal_r2 = terminal_r2
        # A restore must prove the selected bytes before any TTL worker can
        # erase an expired key and conceal a missing journal deletion.
        self.ttl_sweep = False
        # The daemon binds `folddb.sock.tmp` first, then renames it.
        require(len(str(self.socket).encode()) + len(".tmp") < 104, "socket_path_size")

    def call(self, path, body=None, binary=False):
        connection = Connection(self.socket)
        headers = {"X-LastDB-Client": "cloud-backup-ci", "Content-Type": "application/json"}
        if binary:
            headers["Accept"] = "application/octet-stream"
        content = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        returned_stream = False
        try:
            connection.request("GET" if body is None else "POST", path, content, headers)
            response = connection.getresponse()
            if response.status != 200:
                raise OperationError("socket_http_status", socket_refusal=socket_refusal(path, response))
            if binary:
                returned_stream = True
                return response, connection
            raw = response.read(4 * CHUNK + 1)
            require(len(raw) <= 4 * CHUNK, "socket_json_size")
            return decode_json(raw)
        finally:
            if not returned_stream:
                connection.close()

    def offline(self):
        if self.backup_source:
            return
        if self.cloud_credentials is None:
            require(not os.path.lexists(self.home / CREDENTIALS_FILE), "restored_home_cloud_off")
        require(not os.path.lexists(self.home / "data/cloud-byte-backup-v2/active.json"),
                "restored_no_active_backup")
        require(not (self.home / "data/disk-run" / BACKUP_DIR).exists(), "restored_no_backup_job")

    def connect(self):
        """Sign this restored home in as its own device, or copy saved credentials."""
        if os.path.lexists(self.home / CREDENTIALS_FILE):
            return
        if self.device_sign_in:
            self.sign_in_restored_device()
            return
        if self.cloud_credentials is None:
            return
        target = self.home / CREDENTIALS_FILE
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.copyfile(self.cloud_credentials / CREDENTIALS_FILE, target)
        os.chmod(target, 0o600)

    def sign_in_restored_device(self):
        """Register a new device for this home. The report stays in memory:
        it names the home and the account."""
        report = self.programs.command(
            self.home, ["cloud", "backup-v2", "connect", "--dev", "--restored", "--json"])
        require(isinstance(report, dict)
                and report.get("ok") is True
                and report.get("registration_attempted") is True
                and report.get("restored_home") is True
                and report.get("credentials_saved") is True
                and report.get("automatic_backup_enabled") is False
                and report.get("cloud_sync_off") is True
                and report.get("api_key_output") is False
                and os.path.lexists(self.home / CREDENTIALS_FILE),
                "restored_device_sign_in")

    def disconnect(self):
        """Remove the saved credentials once the daemon is down, so a retained
        runner home never keeps an API key. The device id file stays, so the
        next boot registers the same restored device again."""
        if self.cloud_credentials is not None or self.device_sign_in:
            (self.home / CREDENTIALS_FILE).unlink(missing_ok=True)

    def start(self, *, shared_schema=None, storage_inventory=False, sign_in=True):
        require(self.process is None and not self.socket.exists(), "owned_boot_state")
        require(type(sign_in) is bool, "sign_in_flag")
        if sign_in:
            self.connect()
        elif self.device_sign_in:
            require(not os.path.lexists(self.home / CREDENTIALS_FILE), "unsigned_boot_has_credentials")
        self.offline()
        self.programs.verify()
        # Native timing requires isolated-copy admission. Only the retention
        # measurement boots request inventory on these owned generated homes.
        isolated = self.isolated_copy or storage_inventory
        environment = dict(self.programs.environment,
                     LASTDB_BYTE_BACKUP_V2_ENABLED="true" if self.backup_source else "false",
                     LASTDB_ISOLATED_COPY="1" if isolated else "0",
                     LASTDB_TTL_SWEEP="1" if self.ttl_sweep else "0")
        for name in ("LASTDB_CLOUD_CI_STREAM_ID", "LASTDB_CLOUD_CI_TERMINAL_R2_UPLOADS"):
            environment.pop(name, None)
        environment.update(self.ci_uploads())
        if shared_schema is not None:
            environment["LASTDB_NATIVE_SHARED_SCHEMAS"] = json.dumps([shared_schema])
        if storage_inventory:
            environment["LASTDB_SNAPSHOT_TIMING"] = "1"
        self.process = subprocess.Popen(
            [str(self.programs.directory / "lastdbd"), "--data-dir", str(self.home)], env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            require(self.process.poll() is None, "daemon_start_exit")
            try:
                ready = self.health(readiness=True)
            except (OSError, http.client.HTTPException):
                time.sleep(0.2)
                continue
            if ready:
                return
            time.sleep(0.2)
        raise OperationError("daemon_start_deadline")

    def ci_uploads(self):
        """The owned CI stream and upload mode for the daemon and a stopped backup."""
        environment = {}
        if self.ci_stream_id is not None:
            environment["LASTDB_CLOUD_CI_STREAM_ID"] = self.ci_stream_id
        if self.terminal_r2:
            environment["LASTDB_CLOUD_CI_TERMINAL_R2_UPLOADS"] = "1"
        return environment

    def health(self, readiness=False):
        paths = ("/health", "/api/status", "/api/system/boot-identity", "/api/db/native-status")
        # Independent observations settle before the caller uses any of them.
        with ThreadPoolExecutor(max_workers=len(paths) + (self.shared_sync is not None)) as pool:
            policy = None
            if self.shared_sync is not None:
                # General status is sampled. Check a just-changed policy at its live endpoint.
                policy = pool.submit(self.call, "/api/sync/byte-backup-v2", {"action": "status"})
            health, status, boot, native = list(pool.map(self.call, paths))
        require(self.process is not None and self.process.poll() is None, "daemon_health_exit")
        instance = health["instance_id"]
        prefix = str(self.process.pid) + "-"
        require(health["status"] == "ok" and isinstance(instance, str)
                and instance.startswith(prefix) and instance[len(prefix):].isdigit()
                and int(instance[len(prefix):]) > 0
                and self.instance_id in (None, instance), "daemon_health_instance")
        require(status["user_hash"] == self.account
                and status["status"]["build"]["version"] == self.programs.version
                and boot["pid"] == self.process.pid and boot["build"] == self.programs.version,
                "daemon_identity")
        start = boot["process_start_ts"]
        require(type(start) is int and start > 0 and self.start_ts in (None, start)
                and status["status"]["process_start_ts"] == start, "daemon_start_identity")
        self.start_ts, self.instance_id = start, instance
        measured = status["status"]
        require(native["native_disk"]["writer_failed"] is False, "daemon_native_health")
        durability = measured["durability"]
        if policy is not None:
            shared = policy.result()["shared_sync"]
            require(shared["enabled"] is self.shared_sync
                    and (self.shared_sync or shared["progress"]["running"] is False),
                    "daemon_shared_sync_policy")
        elif not self.backup_source:
            require(durability["automatic_enabled"] is False and durability["running"] is False,
                    "daemon_backup_off")
        self.offline()
        if readiness and durability["observed"] is False:
            return False
        require(durability["observed"] is True, "daemon_backup_observation")
        return True

    def stop(self):
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            boot = self.call("/api/system/boot-identity")
            require(boot["pid"] == process.pid and boot["process_start_ts"] == self.start_ts,
                    "daemon_stop_identity")
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=210)
            except subprocess.TimeoutExpired as error:
                raise OperationError("daemon_clean_stop_deadline") from error
        require(process.returncode == 0 and self.start_ts is not None, "daemon_clean_exit")
        receipt = read_json(self.home / ".shutdown_flush_ready", 4096)
        require(receipt == {"version": 1, "pid": process.pid, "start_ts": self.start_ts,
                            "flush_ok": True}, "daemon_flush_receipt")
        require(not self.socket.exists() and not (self.home / "current-session.json").exists(),
                "daemon_shutdown_complete")
        self.process = None
        self.start_ts, self.instance_id = None, None
        self.clean_stops += 1
        self.disconnect()
        self.offline()

    def cleanup_stop(self):
        """Stop this child only. A failed strict stop always fails the operation."""
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=210)
            except subprocess.TimeoutExpired:
                self.forced = True
                process.kill()
                process.wait(timeout=10)
        self.process = None
        self.disconnect()

    def exit_report(self):
        """Fixed fields only: the exit status (null while running) and crash report positions."""
        state = "stopped" if self.process is None else self.process.poll()
        try:
            reports = sorted((self.home / "crash-reports").glob("*.txt"))[:4]
            locations = [line.partition(":")[2].strip() for report in reports
                         for line in report.read_text(errors="replace").splitlines()[:16]
                         if line.startswith("location:")]
        except OSError:
            locations = ["unreadable"]
        return {"home": self.home.name, "exit": state, "crash_locations": locations}

    def verify_records(self, baseline, file_released=False):
        """Exact keyed reads. The first baseline record holds the one file;
        no other record has a file. With `file_released`, a later write replaced
        that file version and the release deleted its B2 object, so the read
        must answer "not in the cloud" (design-lastdb-blob-files-on-demand)."""
        file_record = baseline["records"][0]["id"]
        queries = [{"schema_name": baseline["schema"],
                    "filter": {"HashKey": record["id"]}, "limit": 2, "offset": 0}
                   for record in baseline["records"]]
        reply = self.call("/api/queries/batch", {"queries": queries})
        require(isinstance(reply.get("results"), list) and len(reply["results"]) == len(queries),
                "record_batch_count")
        for item, expected in zip(reply["results"], baseline["records"]):
            result = item["response"]
            if expected.get("deleted") is True:
                require(item["status"] == 200 and result["ok"] is True
                        and result["returned_count"] == 0 and result["results"] == []
                        and not result.get("has_more", False), "deleted_key_stays_absent")
                continue
            require(item["status"] == 200 and result["ok"] is True
                    and result["returned_count"] == 1 and len(result["results"]) == 1
                    and not result.get("has_more", False), "keyed_record_complete")
            row = result["results"][0]
            require(row["key"] == {"hash": expected["id"], "range": None}, "keyed_record_identity")
            require(row["fields"] == {"id": expected["id"], "note": expected["note"]}, "keyed_record_values")
            marker = (row.get("metadata") or {}).get("file")
            if expected["id"] == file_record:
                require(isinstance(marker, dict) and marker.get("size_bytes") == baseline["file_bytes"]
                        and marker.get("sha256") == baseline["file_sha256"], "record_file_marker")
            else:
                require(marker is None, "record_without_file")
            inline = (row.get("metadata") or {}).get("inline_file")
            if expected["id"] == baseline.get("inline_record"):
                require(isinstance(inline, dict) and inline.get("size_bytes") == baseline["inline_bytes"]
                        and inline.get("sha256") == baseline["inline_sha256"]
                        and "bytes_b64" not in inline, "record_inline_marker")
            else:
                require(inline is None, "record_without_inline_file")
        if file_released:
            self.verify_file_released(baseline)
        else:
            self.verify_file(baseline)
        summary = {"records": sum(not record.get("deleted", False) for record in baseline["records"]),
                   "deleted_records": sum(record.get("deleted", False) for record in baseline["records"]),
                   "file_bytes": baseline["file_bytes"],
                   "file_sha256": baseline["file_sha256"], "exact_values": True,
                   "released_file_absent_in_cloud": file_released}
        if baseline.get("inline_record") is not None:
            self.verify_file(baseline, placement="inline")
            summary.update(inline_bytes=baseline["inline_bytes"], inline_sha256=baseline["inline_sha256"])
        self.health()
        summary["health_checked"] = True
        return summary

    def verify_file_needs_sign_in(self, baseline):
        """A file read before sign-in. The body is the sign-in sentence."""
        target = {"schema": baseline["schema"], "key": {"hash": baseline["records"][0]["id"]},
                  "placement": "blob"}
        try:
            response, connection = self.call("/api/db/fetch-blob", target, binary=True)
        except OperationError as error:
            refusal = error.socket_refusal or {}
            require(str(error) == "socket_http_status" and refusal.get("status") == 503
                    and refusal.get("reason") == "sign_in_required",
                    "file_read_before_sign_in")
            return "sign_in_required"
        connection.close()
        raise OperationError("file_read_before_sign_in")

    def verify_file_released(self, baseline):
        """The record still holds its marker (checked above), so a 404 here is
        the daemon's "not in the cloud" answer for the released version."""
        target = {"schema": baseline["schema"], "key": {"hash": baseline["records"][0]["id"]},
                  "placement": "blob"}
        try:
            response, connection = self.call("/api/db/fetch-blob", target, binary=True)
        except OperationError as error:
            refusal = error.socket_refusal or {}
            require(str(error) == "socket_http_status" and refusal.get("status") == 404,
                    "released_file_not_in_cloud")
            return
        connection.close()
        raise OperationError("released_file_still_in_cloud")

    def verify_file(self, baseline, placement="blob"):
        """Stream the record's file and check its exact size and SHA-256. On a
        restored home the first blob read downloads the version from its B2
        path and installs it; the restore proof already showed the CAS home empty."""
        if placement == "inline":
            record, size, sha256 = baseline["inline_record"], baseline["inline_bytes"], baseline["inline_sha256"]
        else:
            record, size, sha256 = baseline["records"][0]["id"], baseline["file_bytes"], baseline["file_sha256"]
        target = {"schema": baseline["schema"], "key": {"hash": record}, "placement": placement}
        response, connection = self.call("/api/db/fetch-blob", target, binary=True)
        try:
            length = response.getheader("Content-Length")
            require(length is None or length == str(size), placement + "_response_size")
            total, digest = 0, hashlib.sha256()
            while True:
                block = response.read(min(CHUNK, size + 1 - total))
                if not block:
                    break
                total += len(block)
                require(total <= size, placement + "_response_overflow")
                digest.update(block)
            require(total == size and digest.hexdigest() == sha256, "restored_" + placement + "_hash")
        finally:
            connection.close()

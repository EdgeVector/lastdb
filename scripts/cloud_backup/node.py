#!/usr/bin/env python3
"""Read the restored records through one explicitly owned Mini daemon."""

import hashlib
import http.client
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import signal
import socket
import subprocess
import time

from support import ACCOUNT, CHUNK, OperationError, decode_json, read_json, require


class Connection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=30)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class OwnedNode:
    def __init__(self, home, programs, backup_source=False):
        self.home, self.programs = home, programs
        self.socket = home / "data/folddb.sock"
        self.process, self.start_ts, self.instance_id = None, None, None
        self.clean_stops, self.forced = 0, False
        self.backup_source, self.pointer = backup_source, None
        require(len(str(home / "data/folddb-full.sock").encode()) < 104, "socket_path_size")

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
            require(response.status == 200, "socket_http_status")
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
        for name in ("cloud_sync.json", "cloud_sync.json.paused", "native_cloud_backup",
                     "cloud-byte-backup-v2-manual.json"):
            require(not os.path.lexists(self.home / name), "restored_home_cloud_off")
        require(not os.path.lexists(self.home / "data/cloud-byte-backup-v2/active.json"),
                "restored_no_active_backup")
        for name in ("cloud-byte-backup-v2", "cloud-byte-source-v2"):
            require(not (self.home / "data/disk-run" / name).exists(), "restored_no_backup_job")

    def start(self):
        require(self.process is None and not self.socket.exists(), "owned_boot_state")
        self.offline()
        self.programs.verify()
        self.process = subprocess.Popen(
            [str(self.programs.directory / "lastdbd"), "--data-dir", str(self.home)],
            env=dict(self.programs.environment,
                     LASTDB_BYTE_BACKUP_V2_ENABLED="true" if self.backup_source else "false"),
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

    def health(self, readiness=False):
        paths = ("/health", "/api/status", "/api/system/boot-identity", "/api/db/native-status")
        # Independent observations settle before the caller uses any of them.
        with ThreadPoolExecutor(max_workers=4) as pool:
            health, status, boot, native = list(pool.map(self.call, paths))
        require(self.process is not None and self.process.poll() is None, "daemon_health_exit")
        instance = health["instance_id"]
        prefix = str(self.process.pid) + "-"
        require(health["status"] == "ok" and isinstance(instance, str)
                and instance.startswith(prefix) and instance[len(prefix):].isdigit()
                and int(instance[len(prefix):]) > 0
                and self.instance_id in (None, instance), "daemon_health_instance")
        require(status["user_hash"] == ACCOUNT
                and status["status"]["build"]["version"] == self.programs.version
                and boot["pid"] == self.process.pid and boot["build"] == self.programs.version,
                "daemon_identity")
        start = boot["process_start_ts"]
        require(type(start) is int and start > 0 and self.start_ts in (None, start)
                and status["status"]["process_start_ts"] == start, "daemon_start_identity")
        self.start_ts, self.instance_id = start, instance
        measured = status["status"]
        for key in ("persist_lane_failures", "deferred_persist_failed"):
            value = measured["resident"][key]
            require(type(value) is int and value == 0, "daemon_persist_failure")
        for key in ("unresolved_atom_skips", "unresolved_atom_distinct", "unresolved_atom_rows"):
            value = measured["integrity"][key]
            require(type(value) is int and value == 0, "daemon_read_integrity")
        require(measured["integrity"]["unresolved_distinct_capped"] is False
                and native["native_disk"]["writer_failed"] is False, "daemon_native_health")
        durability = measured["durability"]
        if not self.backup_source:
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

    def verify_records(self, baseline):
        queries = [{"schema_name": baseline["schema"], "fields": ["id", "note", "file"],
                    "filter": {"HashKey": record["id"]}, "limit": 2, "offset": 0}
                   for record in baseline["records"]]
        reply = self.call("/api/queries/batch", {"queries": queries})
        require(isinstance(reply.get("results"), list) and len(reply["results"]) == len(queries),
                "record_batch_count")
        pointer = None
        for item, expected in zip(reply["results"], baseline["records"]):
            result = item["response"]
            if expected.get("deleted") is True:
                require(item["status"] == 200 and result["ok"] is True
                        and result["returned_count"] == 0 and result["results"] == []
                        and not result.get("has_more", False)
                        and result.get("unresolved_rows", 0) == 0, "deleted_key_stays_absent")
                continue
            require(item["status"] == 200 and result["ok"] is True
                    and result["returned_count"] == 1 and len(result["results"]) == 1
                    and not result.get("has_more", False)
                    and result.get("unresolved_rows", 0) == 0, "keyed_record_complete")
            fields = result["results"][0]["fields"]
            require(result["results"][0]["key"] == {"hash": expected["id"], "range": None},
                    "keyed_record_identity")
            require(set(fields) == {"id", "note", "file"} and fields["id"] == expected["id"]
                    and fields["note"] == expected["note"], "keyed_record_values")
            require(isinstance(fields["file"], dict) and "$lastdb_file" in fields["file"],
                    "record_file_pointer")
            if pointer is None:
                pointer = fields["file"]
            require(fields["file"] == pointer, "shared_file_pointer")
        require(pointer is not None and pointer == baseline.get("file_pointer", pointer),
                "expected_file_pointer")
        self.pointer = pointer
        self.verify_file(pointer, baseline)
        self.health()
        return {"records": sum(not record.get("deleted", False) for record in baseline["records"]),
                "deleted_records": sum(record.get("deleted", False) for record in baseline["records"]),
                "file_bytes": baseline["file_bytes"],
                "file_sha256": baseline["file_sha256"], "exact_values": True,
                "health_checked": True}

    def verify_file(self, pointer, baseline):
        response, connection = self.call("/api/db/fetch-file-blob", {"pointer": pointer}, binary=True)
        try:
            length = response.getheader("Content-Length")
            require(length is None or length == str(baseline["file_bytes"]), "file_response_size")
            total, digest = 0, hashlib.sha256()
            while True:
                block = response.read(min(CHUNK, baseline["file_bytes"] + 1 - total))
                if not block:
                    break
                total += len(block)
                require(total <= baseline["file_bytes"], "file_response_overflow")
                digest.update(block)
            require(total == baseline["file_bytes"]
                    and digest.hexdigest() == baseline["file_sha256"], "restored_file_hash")
        finally:
            connection.close()

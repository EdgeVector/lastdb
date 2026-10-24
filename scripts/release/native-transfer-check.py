#!/usr/bin/env python3
"""Exercise the actual two-home native migration before an artifact can ship."""

import argparse
import http.client
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import tempfile
import time


class Connection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.host)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def call(sock, route, body=None):
    connection = Connection(str(sock), timeout=120)
    try:
        connection.request("GET" if body is None else "POST", route,
                           None if body is None else json.dumps(body),
                           {"Host": "localhost", "Content-Type": "application/json",
                            "X-LastDB-Client": "native-transfer-release"})
        response = connection.getresponse()
        raw = response.read(8 * 1024 * 1024 + 1)
        require(response.status == 200 and len(raw) <= 8 * 1024 * 1024,
                "Native release operation failed: " + route)
        value = json.loads(raw)
        require(value.get("ok") is not False, "Native release operation refused: " + route)
        return value.get("data", value)
    finally:
        connection.close()


def run(argv):
    result = subprocess.run(list(map(str, argv)), capture_output=True, timeout=180)
    require(result.returncode == 0, "Native release command failed: " + str(argv[0]))
    return result


class Node:
    def __init__(self, root, name):
        self.home = root / name
        self.home.mkdir(mode=0o700)
        self.sock = root / (name + ".sock")
        self.process = None
        self.log = (root / (name + ".log")).open("ab")

    def start(self, binary, expect_refusal=False):
        environment = {"PATH": os.environ["PATH"], "HOME": str(self.home),
                       "LASTDB_HOME": str(self.home), "FOLDDB_HOME": str(self.home),
                       "LASTDB_ISOLATED_COPY": "1", "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1",
                       "LASTDBD_STDIO_LOG_ROTATION": "0", "RUST_MIN_STACK": "16777216"}
        self.process = subprocess.Popen([str(binary), "--data-dir", str(self.home),
            "--socket-path", str(self.sock), "--full-socket-path", str(self.sock) + ".full"],
            env=environment, stdout=self.log, stderr=self.log, stdin=subprocess.DEVNULL)
        if expect_refusal:
            require(self.process.wait(timeout=120) != 0, "Wrong account key did not refuse startup")
            self.process = None
            require(b"content key proof" in Path(self.log.name).read_bytes(),
                    "Wrong-key startup did not refuse through the account proof")
            return
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            require(self.process.poll() is None, "Generated daemon exited before ready")
            try:
                if call(self.sock, "/health").get("status") == "ok":
                    return
            except (OSError, http.client.HTTPException, RuntimeError):
                pass
            time.sleep(0.2)
        raise RuntimeError("Generated daemon missed its ready deadline")

    def stop(self):
        if self.process is None:
            return
        process = self.process
        current = json.loads((self.home / "current-session.json").read_text())
        identity = {name: current[name] for name in ("pid", "start_ts")}
        process.send_signal(signal.SIGTERM)
        require(process.wait(timeout=120) == 0, "Generated daemon did not stop cleanly")
        self.process = None
        receipt = json.loads((self.home / ".shutdown_flush_ready").read_text())
        require(receipt == {"version": 1, **identity, "flush_ok": True},
                "Generated shutdown receipt differs")
        ledger = [json.loads(line) for line in (self.home / "sessions.jsonl").read_text().splitlines()]
        require(any(row.get("exit") == "clean" and all(row.get(k) == v for k, v in identity.items())
                    for row in ledger), "Generated clean ledger is absent")

    def close(self):
        try:
            self.stop()
        finally:
            if self.process is not None and self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=15)
            self.log.close()


def mutations(sock, rows):
    result = call(sock, "/api/mutations/batch", {"mutations": rows})
    require(result.get("durability") == "durable" and result.get("count") == len(rows),
            "Default native acknowledgement is not durable")


def mutation(key, value=None, kind="create"):
    return {"type": "mutation", "schema": "NativeTransferReceipt", "mutation_type": kind,
            "key_value": {"hash": key},
            "fields_and_values": {} if kind == "delete" else {"receipt_id": key, "receipt_value": value}}


def read(sock, expected):
    keys = ["retained", "deleted"]
    result = call(sock, "/api/query", {"schema_name": "NativeTransferReceipt",
        "fields": ["receipt_id", "receipt_value"],
        "filter": {"HashRangeKeys": [[key, ""] for key in keys]}, "limit": 2})
    found = {row["key"]["hash"]: row["fields"] for row in result.get("results", [])}
    require(not result.get("has_more") and not result.get("unresolved_rows") and found == expected,
            "Migrated exact-key values differ")


def transfer(args, physical, target):
    (target.home / "data").mkdir(mode=0o700)
    export = subprocess.Popen([str(args.source_transfer), "export", "--home", str(physical)],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        imported = subprocess.run([str(args.candidate / "lastdb-native-transfer"), "import",
            "--home", str(target.home / "data/disk-pack")], stdin=export.stdout,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=300)
        export.stdout.close()
        _, source_receipt = export.communicate(timeout=60)
        require(export.returncode == 0 and imported.returncode == 0, "Native record transfer failed")
    finally:
        if export.poll() is None:
            export.kill()
            export.wait(timeout=15)
    require(json.loads(source_receipt) == json.loads(imported.stderr), "Native receipts differ")
    return json.loads(source_receipt)


def digest(args, home):
    return json.loads(run([args.candidate / "lastdb-native-transfer", "verify", "--home",
                           home / "data/disk-pack"]).stderr)


def account_checks(args, root, source, backup, expected):
    restored, wrong = Node(root, "restored"), Node(root, "wrong")
    try:
        old_physical = root / "restored-old"
        run([args.source_recovery, "restore", "--home", old_physical, "--backup", backup])
        transfer(args, old_physical, restored)
        run([args.candidate / "lastdb", "--data-dir", restored.home, "db",
             "prepare-native-transfer", "--source-home", source.home])
        restored.start(args.candidate / "lastdbd")
        read(restored.sock, expected)
        restored.stop()
        wrong.start(args.candidate / "lastdbd")
        wrong.stop()
        before = digest(args, restored.home)
        refused = subprocess.run(list(map(str, [args.candidate / "lastdb", "--data-dir",
            restored.home, "db", "prepare-native-transfer", "--source-home", wrong.home])),
            capture_output=True, timeout=180)
        require(refused.returncode != 0 and b"content key proof" in refused.stderr,
                "Wrong-key restore did not refuse through the account proof")
        require(digest(args, restored.home) == before, "Wrong-key restore changed stored records")
        # Both homes are private generated DEV homes with stopped owned daemons.
        # Retain the independent identity; copy only the already stopped data.
        shutil.rmtree(wrong.home / "data")
        shutil.copytree(restored.home / "data", wrong.home / "data")
        before = digest(args, wrong.home)
        wrong.start(args.candidate / "lastdbd", expect_refusal=True)
        require(digest(args, wrong.home) == before, "Wrong-key startup changed stored records")
        restored.start(args.candidate / "lastdbd")
        read(restored.sock, expected)
        restored.stop()
        return {"pre_change_backup_restore": True, "restored_account_proof": True,
                "wrong_key_restore_refused": True, "wrong_key_startup_refused": True,
                "wrong_key_records_unchanged": True}
    finally:
        wrong.close()
        restored.close()


def exercise(args, root):
    source, target = Node(root, "source"), Node(root, "target")
    try:
        source.start(args.baseline / "lastdbd")
        call(source.sock, "/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": "NativeTransferReceipt", "schema_type": "Hash",
            "descriptive_name": "NativeTransferReceipt", "key": {"hash_field": "receipt_id"},
            "fields": ["receipt_id", "receipt_value"], "field_descriptions": {
                "receipt_id": "Native migration receipt identity",
                "receipt_value": "Native migration receipt value"}}})
        mutations(source.sock, [mutation("retained", "source"), mutation("deleted", "source")])
        mutations(source.sock, [mutation("deleted", kind="delete")])
        backup = root / "pre-change-backup"
        receipt = call(source.sock, "/api/db/native-backup", {"destination": str(backup)})
        require(receipt["native_backup"]["complete"], "Pre-change native backup is incomplete")
        source.stop()
        source_receipt = transfer(args, source.home / "data/disk-pack", target)
        run([args.candidate / "lastdb", "--data-dir", target.home, "db", "prepare-native-transfer",
             "--source-home", source.home])
        expected = {"retained": {"receipt_id": "retained", "receipt_value": "source"}}
        target.start(args.candidate / "lastdbd")
        read(target.sock, expected)
        mutations(target.sock, [mutation("retained", "target", "update")])
        expected["retained"]["receipt_value"] = "target"
        compact = call(target.sock, "/api/db/compact", {"dry_run": False, "grace_ms": 0})
        require(compact["compact"]["native_disk"].get("executed") is True, "Target compaction did not execute")
        target.stop()
        accounts = account_checks(args, root, source, backup,
            {"retained": {"receipt_id": "retained", "receipt_value": "source"}})
        target.start(args.candidate / "lastdbd")
        read(target.sock, expected)
        target.stop()
        # Recovery reads the original source, never the target's new format.
        source.start(args.baseline / "lastdbd")
        read(source.sock, {"retained": {"receipt_id": "retained", "receipt_value": "source"}})
        source.stop()
        return {"passed": True, "source_retained": True, "record_transfer": source_receipt, **accounts,
                "target_compaction": True, "target_reopen": True, "source_reopen": True}
    finally:
        target.close()
        source.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--source-transfer", type=Path, required=True)
    parser.add_argument("--source-recovery", type=Path, required=True)
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="native-transfer-", dir=os.environ.get("RUNNER_TEMP")))
    result = exercise(args, root)
    print(json.dumps(result, sort_keys=True))
    # A failure retains only generated diagnostics in the runner's temp area.
    shutil.rmtree(root)


if __name__ == "__main__":
    main()

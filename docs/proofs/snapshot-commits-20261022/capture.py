#!/usr/bin/env python3
"""Capture one bounded product run on a fresh, explicitly owned LastDB home."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import stat
import struct
import subprocess
import time
import uuid


SCHEMA = "SnapshotCommitProduct"
HASH = "measure"
FIELDS = ["bucket", "id", "title", "body"]


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def key(number):
    return f"key-{number:06d}"


def fields(number, revision):
    body = "".join(hashlib.sha256(f"snapshot:{number}:{revision}:{part}".encode()).hexdigest()
                   for part in range(16))
    return {"bucket": HASH, "id": key(number),
            "title": f"record-{number}-revision-{revision}", "body": body}


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


class Capture:
    def __init__(self, args):
        self.identity = uuid.uuid4().hex
        self.binary = args.binary.resolve(strict=True)
        self.root = args.scratch.absolute()
        forbidden = {".lastdb", ".folddb", ".primary-lastdb-archive-20261014"}
        if forbidden.intersection(self.root.parts) or self.root.is_symlink():
            raise ValueError("Scratch must be a fresh owned path outside primary and archived data")
        if self.root.exists():
            info = self.root.stat()
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
                raise ValueError("Existing scratch must be owned by this user with mode 0700")
            if next(self.root.iterdir(), None) is not None:
                raise ValueError("Scratch must be empty")
        else:
            self.root.mkdir(mode=0o700)
        self.root = self.root.resolve(strict=True)
        if forbidden.intersection(self.root.parts):
            raise ValueError("Resolved scratch names primary or archived data")
        self.output = args.output.absolute()
        if forbidden.intersection(self.output.parts) or self.output.is_symlink():
            raise ValueError("Output must stay outside primary and archived data")
        if forbidden.intersection(self.output.resolve().parts):
            raise ValueError("Resolved output names primary or archived data")
        self.output.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.home = self.root / "home"
        self.home.mkdir(mode=0o700)
        self.socket = self.root / "q.sock"
        self.full_socket = self.root / "f.sock"
        if len(str(self.full_socket).encode()) >= 100:
            raise ValueError("Use a shorter scratch path for the Unix sockets")
        self.process, self.log, self.sequence = None, None, 0
        self.expected = {}
        info = self.root.stat()
        ownership = {"version": 1, "run_id": self.identity, "owner_uid": os.getuid(),
                     "device": info.st_dev, "inode": info.st_ino, "creator_pid": os.getpid(),
                     "scratch": str(self.root), "home": str(self.home), "primary_data": False}
        save(self.root / ".snapshot-capture-owner.json", ownership)
        save(self.output / "ownership.json", ownership)
        save(self.output / "inputs.json", {"source_revision": args.source_revision,
             "binary": str(self.binary), "binary_sha256": file_hash(self.binary),
             "driver_sha256": file_hash(Path(__file__)), "records": 64, "body_bytes": 1024,
             "update_rounds": 5, "deleted": list(range(8)), "recreated": list(range(4)),
             "clock": "perf_counter_ns", "filesystem_cache": "uncontrolled",
             "claim_limit": "Product run and process interruption; no power-loss proof"})

    def remember(self, operation, **value):
        receipt = {"operation": operation, "at_unix_ns": time.time_ns(), **value}
        with (self.output / "receipts.jsonl").open("a") as stream:
            stream.write(json.dumps(receipt, sort_keys=True) + "\n")

    def call(self, path, value=None, cost=False):
        self.sequence += 1
        request_id = f"snapshot-{self.identity}-{self.sequence}"
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "snapshot-product-capture",
                   "X-LastDB-Request-Id": request_id}
        if cost:
            headers["x-lastdb-read-receipt"] = "true"
        connection = UnixConnection(self.socket)
        started = time.perf_counter_ns()
        try:
            body = None if value is None else json.dumps(value, separators=(",", ":"))
            connection.request("GET" if value is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read()
            result = json.loads(raw)
            receipt = response.getheader("x-lastdb-read-receipt")
            self.remember("request", request_id=request_id, path=path, request=value,
                          http_status=response.status, response=result,
                          read_receipt=json.loads(receipt) if receipt else None,
                          elapsed_nanos=time.perf_counter_ns() - started)
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}")
            return result.get("data", result)
        finally:
            connection.close()

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned node is already active")
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           RUST_MIN_STACK="16777216")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember("owned-node-start", pid=self.process.pid, home=str(self.home))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned node exited before health; inspect node.log")
            if self.socket.exists():
                try:
                    health = self.call("/health")
                    if health.get("ok") is True or health.get("status") == "ok":
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("The owned node missed its health deadline")

    def stop(self, force=False):
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            process.send_signal(signal.SIGKILL if force else signal.SIGTERM)
            try:
                code = process.wait(timeout=10 if force else 45)
            except subprocess.TimeoutExpired:
                process.kill()
                code = process.wait(timeout=10)
                self.remember("shutdown-timeout", pid=process.pid)
                force = True
        else:
            code = process.returncode
        proof_path = self.home / ".shutdown_flush_ready"
        proof, proof_error = None, None
        try:
            proof = json.loads(proof_path.read_text()) if proof_path.exists() else None
        except (OSError, ValueError) as failure:
            proof_error = str(failure)
        self.remember("owned-node-stop", pid=process.pid, exit_code=code,
                      forced=force, shutdown_receipt=proof, shutdown_receipt_error=proof_error)
        self.process = None
        self.log.close()
        self.log = None
        if not force and (code != 0 or not proof or proof.get("flush_ok") is not True):
            raise RuntimeError("The owned node lacks a successful graceful shutdown receipt")

    def install(self):
        self.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": FIELDS, "field_descriptions": {
                field: f"generated snapshot capture {field}" for field in FIELDS}}})

    def mutate(self, operation, numbers, revision, durability="queued"):
        numbers, mutations = list(numbers), []
        for number in numbers:
            value = {} if operation == "delete" else fields(number, revision)
            if operation == "update":
                value = {name: value[name] for name in ("title", "body")}
            mutations.append({"type": "mutation", "schema": SCHEMA,
                "mutation_type": operation, "durability": durability,
                "key_value": {"hash": HASH, "range": key(number)}, "fields_and_values": value})
        result = self.call("/api/mutations/batch", {"mutations": mutations})
        if result.get("count") != len(numbers) or result.get("durability") not in ("queued", "durable"):
            raise RuntimeError("The product batch lacks its complete acknowledgement")
        if durability == "durable" and result.get("durability") != "durable":
            raise RuntimeError("The durable request did not return a durable acknowledgement")
        for number in numbers:
            if operation == "delete":
                self.expected.pop(key(number), None)
            else:
                self.expected[key(number)] = fields(number, revision)

    def flush(self, phase):
        result = self.call("/api/db/flush", {})
        receipt = result.get("native_flush", {})
        if result.get("durable") is not True or receipt.get("completed_sequence", -1) < receipt.get("target_sequence", 0):
            raise RuntimeError("The explicit flush lacks complete durable coverage")
        native = self.call("/api/db/native-status")["native_disk"]
        if native.get("writer_failed") is not False:
            raise RuntimeError("Native status reports a failed writer")
        self.remember("durable-boundary", phase=phase, receipt=receipt, native=native)

    def rows(self, result):
        rows, seen = result.get("results", []), {}
        for record in rows:
            coordinate = record["key"]
            if coordinate.get("hash") != HASH or coordinate["range"] in seen:
                raise RuntimeError("The product returned an unknown hash or duplicate key")
            seen[coordinate["range"]] = record["fields"]
        if result.get("returned_count") != len(rows):
            raise RuntimeError("The returned count differs from the records")
        return seen

    def readback(self, phase):
        result = self.call("/api/query", {"schema_name": SCHEMA, "fields": FIELDS,
            "filter": {"HashRangeKeys": [[HASH, key(number)] for number in range(64)]},
            "limit": 64}, cost=True)
        if self.rows(result) != self.expected or result.get("has_more") is not False:
            raise RuntimeError("Exact-key values or delete membership differ")
        page, actual, ordered = None, {}, []
        for _ in range(16):
            request = {"schema_name": SCHEMA, "fields": FIELDS,
                "filter": {"HashRangeRange": {"hash": HASH, "start": key(0), "end": key(64)}},
                "limit": 7}
            if page is not None:
                request["cursor"] = page
            result = self.call("/api/query", request, cost=True)
            current = self.rows(result)
            if len(current) > 7 or set(current).intersection(actual):
                raise RuntimeError("A bounded range page exceeds its limit or repeats a key")
            ordered.extend(row["key"]["range"] for row in result["results"])
            actual.update(current)
            if not result.get("has_more"):
                break
            next_page = result.get("next_cursor")
            if not next_page or next_page == page:
                raise RuntimeError("The range continuation cannot advance")
            page = next_page
        else:
            raise RuntimeError("The bounded range exceeded sixteen pages")
        if actual != self.expected or ordered != sorted(self.expected):
            raise RuntimeError("Range values, membership, or order differ")
        self.remember("known-key-readback", phase=phase, live_records=len(actual),
                      expected_sha256=hashlib.sha256(json.dumps(self.expected, sort_keys=True).encode()).hexdigest())

    def inventory(self, phase):
        physical = self.home / "data" / "disk-pack"
        stack, count, logical, allocated, kinds, forbidden = [(physical, 0)], 0, 0, 0, {}, []
        examined, deadline = 0, time.monotonic() + 20
        while stack:
            parent, depth = stack.pop()
            if depth > 8:
                raise RuntimeError("The owned physical home exceeds the inventory depth bound")
            for path in parent.iterdir():
                examined += 1
                if examined > 40000 or time.monotonic() > deadline:
                    raise RuntimeError("The owned inventory exceeds its entry or time bound")
                if path.is_symlink():
                    raise RuntimeError("The owned physical inventory refuses symlinks")
                if path.name in ("journal", "JOURNAL_TAIL"):
                    forbidden.append(str(path.relative_to(physical)))
                if path.is_dir():
                    stack.append((path, depth + 1))
                elif path.is_file():
                    info = path.stat()
                    count, logical, allocated = count + 1, logical + info.st_size, allocated + info.st_blocks * 512
                    kind = path.relative_to(physical).parts[0]
                    entry = kinds.setdefault(kind, {"files": 0, "bytes": 0})
                    entry["files"], entry["bytes"] = entry["files"] + 1, entry["bytes"] + info.st_size
                    if count > 20000:
                        raise RuntimeError("The owned physical inventory exceeds twenty thousand files")
        current = (physical / "CURRENT").read_bytes()
        features = struct.unpack_from("<Q", current, 112)[0] if len(current) == 4096 else 0
        self.remember("physical-inventory", phase=phase, files=count, logical_bytes=logical,
                      allocated_bytes=allocated, kinds=kinds, forbidden_paths=forbidden,
                      current_sha256=hashlib.sha256(current).hexdigest(), required_features=features)
        if forbidden or not features & 256 or features & 2:
            raise RuntimeError("The physical home is not the journal-free snapshot format")


def run(capture):
    capture.start()
    capture.install()
    capture.mutate("create", range(64), 0)
    capture.readback("create-ack")
    capture.flush("create")
    for revision in range(1, 6):
        capture.mutate("update", range(64), revision)
        capture.flush(f"update-{revision}")
    capture.mutate("delete", range(8), 0)
    capture.flush("delete")
    capture.mutate("create", range(4), 6)
    capture.flush("recreate")
    capture.readback("sixty-live")
    capture.inventory("before-compact")
    capture.call("/api/db/compact", {"dry_run": False, "grace_ms": 0})
    capture.readback("after-compact")
    capture.inventory("after-compact")
    capture.stop()
    capture.start()
    capture.readback("graceful-reopen")
    capture.mutate("update", [63], 99, durability="durable")
    capture.stop(force=True)
    capture.start()
    capture.readback("durable-update-forced-reopen")
    capture.inventory("final")
    capture.flush("final")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", default="unrecorded")
    args = parser.parse_args()
    capture, complete, error = Capture(args), False, None
    try:
        run(capture)
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        try:
            capture.stop()
        except Exception as failure:
            complete, error = False, f"{error or ''}; shutdown: {failure}"
        save(capture.output / "summary.json", {"complete": complete, "error": error,
             "scratch": str(capture.root), "home": str(capture.home),
             "all_owned_processes_stopped": capture.process is None,
             "expected_live_records": len(capture.expected), "data_retained_for_review": True,
             "scope": "One bounded product run; no power-loss, sync, backup, or corruption claim"})
    print(json.dumps({"complete": complete, "error": error, "output": str(capture.output)}))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())

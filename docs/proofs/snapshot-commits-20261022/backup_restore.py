#!/usr/bin/env python3
"""Capture one local backup and restore on an existing owned generated home."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import struct
import subprocess
import time
import uuid

from capture import Capture, UnixConnection, fields, file_hash, key, save


def metadata(path):
    info = path.stat()
    return {"device": info.st_dev, "inode": info.st_ino, "bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns, "owner_uid": info.st_uid}


def files(root):
    stack, result, examined, deadline = [(root, 0)], [], 0, time.monotonic() + 20
    while stack:
        parent, depth = stack.pop()
        if depth > 8:
            raise RuntimeError("Owned inventory exceeds depth eight")
        for path in parent.iterdir():
            examined += 1
            if examined > 40000 or time.monotonic() > deadline:
                raise RuntimeError("Owned inventory exceeds its entry or time bound")
            if path.is_symlink():
                raise RuntimeError("Owned inventory refuses symlinks")
            if path.is_dir():
                stack.append((path, depth + 1))
            elif path.is_file():
                result.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size})
                if len(result) > 20000:
                    raise RuntimeError("Owned inventory exceeds twenty thousand files")
            else:
                raise RuntimeError("Owned inventory contains a non-file object")
    return sorted(result, key=lambda item: item["path"])


class BackupRestore(Capture):
    def __init__(self, args):
        prior = args.run.resolve(strict=True)
        ownership = json.loads((prior / "ownership.json").read_text())
        summary = json.loads((prior / "summary.json").read_text())
        inputs = json.loads((prior / "inputs.json").read_text())
        self.root = Path(ownership["scratch"]).resolve(strict=True)
        self.home = Path(ownership["home"]).resolve(strict=True)
        forbidden = {".lastdb", ".folddb", ".primary-lastdb-archive-20261014"}
        if forbidden.intersection(self.root.parts) or ownership.get("primary_data") is not False:
            raise ValueError("Refuse primary or archived data")
        if self.home != self.root / "home" or self.root.is_symlink():
            raise ValueError("Owned home differs from its exact scratch path")
        info = self.root.stat()
        if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
                or info.st_dev != ownership["device"] or info.st_ino != ownership["inode"]):
            raise ValueError("Scratch ownership metadata differs")
        if json.loads((self.root / ".snapshot-capture-owner.json").read_text()) != ownership:
            raise ValueError("Scratch ownership marker differs")
        if summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True:
            raise ValueError("The preceding owned run is not complete and stopped")
        self.socket, self.full_socket = self.root / "q.sock", self.root / "f.sock"
        for path in (self.socket, self.full_socket):
            probe = socket.socket(socket.AF_UNIX)
            probe.settimeout(0.25)
            try:
                probe.connect(str(path))
            except (FileNotFoundError, ConnectionRefusedError):
                pass
            else:
                raise ValueError("The owned home still has an active node socket")
            finally:
                probe.close()
        self.output = prior / "backup-restore"
        self.output.mkdir(mode=0o700, exist_ok=False)
        self.identity, self.sequence = uuid.uuid4().hex, 0
        self.binary = Path(inputs["binary"]).resolve(strict=True)
        self.recovery = args.recovery.resolve(strict=True)
        if file_hash(self.binary) != inputs["binary_sha256"]:
            raise ValueError("The node binary differs from the original owned run")
        self.process, self.log = None, None
        self.physical = self.home / "data" / "disk-pack"
        if self.physical.resolve(strict=True) != self.physical:
            raise ValueError("The owned physical root contains a symlink")
        self.bundle = self.root / "backup-bundle"
        self.source = self.root / "source-disk-pack-before-restore"
        if self.bundle.exists() or self.source.exists():
            raise ValueError("An owned backup destination already exists")
        self.expected = {key(number): fields(number, 6 if number < 4 else 99 if number == 63 else 5)
                         for number in range(64) if number < 4 or number >= 8}
        if len(self.expected) != 60:
            raise ValueError("The expected generated state differs")
        self.identity_before = metadata(self.home / "identity.key")
        save(self.output / "inputs.json", {"ownership": ownership, "node_binary": str(self.binary),
             "node_binary_sha256": file_hash(self.binary), "recovery_binary": str(self.recovery),
             "recovery_binary_sha256": file_hash(self.recovery), "driver_sha256": file_hash(Path(__file__)),
             "expected_records": 60, "expected_last_revision": 99,
             "identity_bytes_read_or_copied": False})

    def command(self, command):
        started = time.perf_counter_ns()
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                text=True, timeout=120, check=False)
        name = command[1]
        (self.output / f"{name}.stdout").write_text(result.stdout)
        (self.output / f"{name}.stderr").write_text(result.stderr)
        receipt = {"argv": command, "exit_code": result.returncode,
                   "elapsed_nanos": time.perf_counter_ns() - started}
        if result.returncode == 0:
            receipt["result"] = json.loads(result.stdout)
        save(self.output / f"{name}-command.json", receipt)
        if result.returncode != 0 or receipt["result"].get("complete") is not True:
            raise RuntimeError(f"Owned {name} did not complete")
        return receipt["result"]

    def bundle_inventory(self):
        manifest_path = self.bundle / "BACKUP.json"
        if manifest_path.stat().st_size > 32 * 1024 * 1024:
            raise RuntimeError("Backup manifest exceeds its finite bound")
        manifest = json.loads(manifest_path.read_text())
        inventory = files(self.bundle)
        allowed = {"CURRENT", "directory", "locator", "packs", "generations",
                   "laststore-pack-home-v1", "laststore-layout-v1", "CONVERSION_COMPLETE"}
        paths = [Path(item["path"]) for item in manifest["files"]]
        if manifest.get("format") != "lastdb-native-backup-2":
            raise RuntimeError("Backup does not use snapshot bundle format 2")
        if any(path.parts[0] not in allowed for path in paths):
            raise RuntimeError("Backup contains an unexpected namespace")
        if any(any(part.lower() in {"journal", "journal_tail", "identity.key", "keys", "cloud"}
                   for part in path.parts) for path in paths):
            raise RuntimeError("Backup includes a journal or credential file path")
        expected = {item["path"] for item in manifest["files"]} | {"BACKUP.json", "BACKUP.sha256"}
        if {item["path"] for item in inventory} != expected:
            raise RuntimeError("Backup inventory differs from its exact manifest files")
        if any(item["bytes"] != (self.bundle / item["path"]).stat().st_size for item in manifest["files"]):
            raise RuntimeError("Backup manifest file lengths differ")
        save(self.output / "bundle-inventory.json", {"manifest": manifest, "inventory": inventory,
             "format": manifest["format"], "node_identity_key_excluded": True,
             "credential_file_paths_absent": True, "journal_paths_absent": True,
             "identity_bytes_read_or_copied": False})
        return manifest

    def run_restore(self):
        original_current = file_hash(self.physical / "CURRENT")
        self.inventory("source-before-backup")
        backup = self.command([str(self.recovery), "backup", "--home", str(self.physical),
                               "--backup", str(self.bundle)])
        if backup.get("includes_identity") is not False:
            raise RuntimeError("Backup receipt does not exclude node identity")
        manifest = self.bundle_inventory()
        self.physical.rename(self.source)
        self.remember("owned-source-renamed", source=str(self.source), restore_path=str(self.physical),
                      original_path_absent=not self.physical.exists())
        restored = self.command([str(self.recovery), "restore", "--home", str(self.physical),
                                 "--backup", str(self.bundle)])
        if (restored["generation"], restored["frontier"]) != (manifest["generation"], manifest["frontier"]):
            raise RuntimeError("Restored root differs from the backup boundary")
        if file_hash(self.physical / "CURRENT") != original_current:
            raise RuntimeError("Restored CURRENT differs from the original complete snapshot")
        if metadata(self.home / "identity.key") != self.identity_before:
            raise RuntimeError("The node identity file metadata changed before restart")
        self.start()
        self.readback("bundle-restored-product-readback")
        self.inventory("bundle-restored-product")
        native = self.call("/api/db/native-status")["native_disk"]
        if native.get("writer_failed") is not False:
            raise RuntimeError("The restored node reports a failed snapshot writer")
        self.stop()
        if metadata(self.home / "identity.key") != self.identity_before:
            raise RuntimeError("The node identity file metadata changed after restart")
        save(self.output / "result.json", {"complete": True, "format": manifest["format"],
             "expected_live_records": 60, "exact_and_range_values_matched": True,
             "key63_revision99_matched": True, "same_home_identity_file_metadata": True,
             "identity_bytes_read_or_copied": False, "source_physical_root_renamed": str(self.source),
             "restored_physical_root": str(self.physical), "bundle": str(self.bundle),
             "generation": manifest["generation"], "frontier": manifest["frontier"],
             "node_identity_key_excluded": True, "credential_file_paths_absent": True,
             "journal_paths_absent": True, "scope": "One owned local backup/restore and product readback; no cloud or power-loss claim"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--recovery", type=Path, required=True)
    args = parser.parse_args()
    operation, complete, error = None, False, None
    try:
        operation = BackupRestore(args)
        operation.run_restore()
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        if operation is not None:
            operation.remember("failure", error=error)
    finally:
        if operation is not None:
            try:
                operation.stop()
            except Exception as failure:
                complete, error = False, f"{error or ''}; shutdown: {failure}"
            summary = {"complete": complete, "error": error,
                       "all_owned_processes_stopped": operation.process is None,
                       "scratch": str(operation.root), "home": str(operation.home),
                       "physical": str(operation.physical), "physical_exists": operation.physical.exists(),
                       "renamed_source": str(operation.source), "renamed_source_exists": operation.source.exists(),
                       "bundle": str(operation.bundle), "bundle_exists": operation.bundle.exists(),
                       "data_retained_for_root_cleanup": True, "identity_bytes_read_or_copied": False}
            save(operation.output / "summary.json", summary)
    print(json.dumps({"complete": complete, "error": error,
                      "output": str(operation.output) if operation else None}))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())

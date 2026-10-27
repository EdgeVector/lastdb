"""Bounded responses and file walks for the generated cloud workload."""

import http.client
import json
import os
from pathlib import Path
import stat
import time

from node import Node, UnixConnection


class RequestFailure(RuntimeError):
    def __init__(self, message, elapsed_ms):
        super().__init__(message)
        self.elapsed_ms = elapsed_ms


class SustainedNode(Node):
    def call(self, path, body=None, receipt=False, request_id=None, timeout=60):
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body, separators=(",", ":")).encode()
        connection = UnixConnection(self.socket)
        connection.timeout = timeout
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-v2-sustained"}
        started = time.perf_counter_ns()
        try:
            connection.request("GET" if body is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise RuntimeError("Generated response exceeded the 4 MiB measurement cap")
            elapsed = (time.perf_counter_ns() - started) / 1e6
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}: {raw[:600]!r}")
            result = json.loads(raw)
            if result.get("ok") is False:
                raise RuntimeError(f"{path}: HTTP 200 returned ok=false: {raw[:600]!r}")
            return result.get("data", result), elapsed, None
        except (OSError, http.client.HTTPException) as error:
            # Node.start retries these while an old socket waits for its new
            # daemon. Keep the type, and retain timing for load failures.
            error.elapsed_ms = (time.perf_counter_ns() - started) / 1e6
            raise
        except Exception as error:
            raise RequestFailure(str(error), (time.perf_counter_ns() - started) / 1e6) from error
        finally:
            connection.close()

    def native_status(self, inventory=False, include_search=False):
        result, _, _ = self.call("/api/db/native-status", {"inventory": inventory}, timeout=85)
        audit = result.get("storage_diagnostics")
        if not isinstance(audit, dict) or set(audit) != {"main", "cas"}:
            raise RuntimeError("The isolated native diagnostic checkpoint is unavailable")
        return (audit, result.get("search")) if include_search else audit

    def disk_usage(self):
        stack, entries, logical, allocated = [(self.home, 0)], 0, 0, 0
        seen, families = set(), {}
        unique_logical, unique_allocated, duplicate_entries = 0, 0, 0
        deadline = time.monotonic() + 15
        while stack:
            parent, depth = stack.pop()
            if depth > 12:
                raise RuntimeError("Generated-home depth exceeded its bound")
            try:
                with os.scandir(parent) as children:
                    for entry in children:
                        entries += 1
                        if entries > 100_000 or time.monotonic() > deadline:
                            raise RuntimeError("Generated-home size check exceeded its bound")
                        try:
                            info = entry.stat(follow_symlinks=False)
                        except FileNotFoundError:
                            continue
                        if stat.S_ISLNK(info.st_mode):
                            raise RuntimeError("Unexpected symlink in generated home")
                        if stat.S_ISDIR(info.st_mode):
                            stack.append((Path(entry.path), depth + 1))
                        else:
                            logical += info.st_size
                            allocated += info.st_blocks * 512
                            if self.native_timing:
                                parts = Path(entry.path).relative_to(self.home).parts
                                family = disk_family(parts)
                                row = families.setdefault(family, {"files": 0, "logical_bytes": 0,
                                                                   "allocated_bytes": 0})
                                row["files"] += 1
                                row["logical_bytes"] += info.st_size
                                row["allocated_bytes"] += info.st_blocks * 512
                                identity = (info.st_dev, info.st_ino)
                                if identity not in seen:
                                    seen.add(identity)
                                    unique_logical += info.st_size
                                    unique_allocated += info.st_blocks * 512
                                else:
                                    duplicate_entries += 1
            except FileNotFoundError:
                continue
        result = {"logical_bytes": logical, "allocated_bytes": allocated}
        if self.native_timing:
            result.update(families=families, unique_file_logical_bytes=unique_logical,
                          unique_file_allocated_bytes=unique_allocated,
                          hardlink_duplicate_entries=duplicate_entries)
        return result


def disk_family(parts):
    """Only fixed role labels leave the generated home; never file names."""
    if parts[:3] == ("apps", "search", "inbox"):
        return "search_inbox"
    for home in ("disk-run", "disk-cas"):
        if parts[:2] == ("data", home):
            return f"data_{home}"
        if parts[:1] == (home,):
            return f"root_{home}"
        if home in parts:
            return f"nested_{home}"
    if any(part in ("snapshot", "snapshots", "backup", "backups") for part in parts[:-1]):
        return "snapshot_or_backup"
    if any(part == "logs" for part in parts[:-1]):
        return "node_logs"
    return "other_data" if parts[:1] == ("data",) else "other_home"

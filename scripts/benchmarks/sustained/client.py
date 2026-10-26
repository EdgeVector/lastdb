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

    def native_status(self, inventory=False):
        result, _, _ = self.call("/api/db/native-status", {"inventory": inventory}, timeout=85)
        audit = result.get("storage_diagnostics")
        if not isinstance(audit, dict) or set(audit) != {"main", "cas"}:
            raise RuntimeError("The isolated native diagnostic checkpoint is unavailable")
        return audit

    def disk_usage(self):
        stack, entries, logical, allocated = [(self.home, 0)], 0, 0, 0
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
            except FileNotFoundError:
                continue
        return {"logical_bytes": logical, "allocated_bytes": allocated}

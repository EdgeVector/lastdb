#!/usr/bin/env python3
"""Check the last stdout line of signed_history_relay_e2e.

Usage: check_relay_e2e.py STDOUT_FILE [LABEL] [--expect-streams] [--expect-blob-file BYTES]
                          [--expect-cleanup]

--expect-streams also requires every log.streams step to be reported;
--expect-blob-file BYTES requires the large-file blob step for that size;
--expect-cleanup requires the run to have deleted every bundle blob it uploaded
(blobs_dropped == bundles, released_bytes == bytes_uploaded) and the read-back to
show them gone, and prints one `cleanup:` line.

Fails unless the bin printed a JSON object as its last line with ok=true,
grant_revoke_ok=true, nongrantee_http_status=403, and upload_seconds /
restore_seconds both under the limit. Prints a one-line verdict.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

LIMIT_SECONDS = 60.0
STREAMS_STEPS = [
    "streams_empty_writer",
    "streams_two_devices",
    "streams_granted_callers_list",
    "streams_head_advances",
    "streams_contiguous_head",
    "streams_nongrantee_refused",
    "streams_revoked_refused",
]


def last_json(path: Path) -> dict:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        raise SystemExit(f"{path}: no stdout from signed_history_relay_e2e")
    try:
        report = json.loads(lines[-1])
    except json.JSONDecodeError as err:
        raise SystemExit(f"{path}: last stdout line is not JSON: {err}") from err
    if not isinstance(report, dict):
        raise SystemExit(f"{path}: last stdout line is not a JSON object")
    return report


def cleanup_problems(report: dict) -> list[str]:
    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict):
        return [f"cleanup is {cleanup!r}, expected the cleanup report"]
    if cleanup.get("ok") is not True:
        return [f"cleanup failed: {cleanup.get('error')}"]
    found: list[str] = []
    if cleanup.get("blobs_dropped") != report.get("bundles"):
        found.append(
            f"cleanup dropped {cleanup.get('blobs_dropped')!r} blobs, the run uploaded {report.get('bundles')!r}"
        )
    if cleanup.get("released_bytes") != report.get("bytes_uploaded"):
        found.append(
            f"cleanup released {cleanup.get('released_bytes')!r} bytes, the run uploaded {report.get('bytes_uploaded')!r}"
        )
    for db in cleanup.get("databases") or []:
        back = db.get("readback") or {}
        listed = db.get("blobs_listed")
        if back.get("blob_list_after") != 0 or back.get("object_get_not_found") != listed:
            found.append(f"cleanup read-back of {db.get('database_id')} is {back!r} for {listed!r} blobs")
    return found


def cleanup_line(report: dict) -> str:
    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict):
        return "cleanup: not run"
    if cleanup.get("ok") is not True:
        return f"cleanup: FAILED: {cleanup.get('error')}"
    parts = []
    for db in cleanup.get("databases") or []:
        back = db.get("readback") or {}
        parts.append(
            f"database {db.get('database_id')}: dropped {db.get('blobs_dropped')} blobs "
            f"({db.get('released_bytes')} bytes) in {db.get('drop_calls')} blob/drop calls; "
            f"read-back: blob/list lists {back.get('blob_list_after')}, "
            f"blob/get 404 for {back.get('blob_get_not_found')}, "
            f"object GET 404 for {back.get('object_get_not_found')}; "
            f"publisher log entries kept: {db.get('publisher_log_entries_kept')}"
        )
    total = f"{cleanup.get('blobs_dropped')} blobs, {cleanup.get('released_bytes')} bytes"
    return f"cleanup: deleted {total}. " + " | ".join(parts)


def problems(report: dict, expect_streams: bool, blob_file_bytes: int | None) -> list[str]:
    found: list[str] = []
    if expect_streams and report.get("streams_steps") != STREAMS_STEPS:
        found.append(f"streams_steps is {report.get('streams_steps')!r}, expected {STREAMS_STEPS}")
    if blob_file_bytes is not None:
        blob = report.get("blob_file_stream")
        if not isinstance(blob, dict) or blob.get("bytes") != blob_file_bytes:
            found.append(f"blob_file_stream is {blob!r}, expected {blob_file_bytes} bytes")
        elif not isinstance(blob.get("sha256"), str) or len(blob["sha256"]) != 64:
            found.append("blob_file_stream has no sha256")
        elif blob.get("refused_uploads") != 3:
            found.append(
                f"blob_file_stream refused_uploads is {blob.get('refused_uploads')!r}, expected 3"
            )
    if report.get("ok") is not True:
        found.append(f"ok is {report.get('ok')!r}: {report.get('error')}")
    if report.get("grant_revoke_ok") is not True:
        found.append("grant_revoke_ok is not true")
    if report.get("nongrantee_http_status") != 403:
        found.append(
            f"nongrantee_http_status is {report.get('nongrantee_http_status')!r}, expected 403"
        )
    for key in ("upload_seconds", "restore_seconds"):
        value = report.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            found.append(f"{key} is missing or not a number")
        elif value >= LIMIT_SECONDS:
            found.append(f"{key}={value:.3f} is not under {LIMIT_SECONDS:.0f}s")
    return found


def main() -> int:
    argv = sys.argv[1:]
    expect_streams = "--expect-streams" in argv
    expect_cleanup = "--expect-cleanup" in argv
    argv = [a for a in argv if a not in ("--expect-streams", "--expect-cleanup")]
    blob_file_bytes = None
    if "--expect-blob-file" in argv:
        at = argv.index("--expect-blob-file")
        blob_file_bytes = int(argv[at + 1])
        del argv[at : at + 2]
    if not argv:
        raise SystemExit(__doc__)
    label = argv[1] if len(argv) > 1 else "relay"
    report = last_json(Path(argv[0]))
    found = problems(report, expect_streams, blob_file_bytes)
    if expect_cleanup:
        found += cleanup_problems(report)
    verdict = "FAIL" if found else "PASS"
    line = (
        f"{verdict} {label} upload={report.get('upload_seconds')} "
        f"restore={report.get('restore_seconds')} bytes={report.get('bytes_uploaded')} "
        f"streams={report.get('streams_discovered')} grant_revoke_ok={report.get('grant_revoke_ok')} "
        f"nongrantee_http_status={report.get('nongrantee_http_status')} "
        f"streams_steps={len(report.get('streams_steps') or [])} "
        f"blob_file_stream={report.get('blob_file_stream')}"
    )
    print(line)
    if expect_cleanup or report.get("cleanup") is not None:
        print(cleanup_line(report))
    for item in found:
        print(f"  - {item}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

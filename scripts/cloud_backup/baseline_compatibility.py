"""Diagnose an authenticated saved baseline's format before allocating a stream.

The receipt must bind the exact protected point and source. Missing historical
evidence leaves compatibility unknown and the existing native restore decides.
No historical code or executable is fetched or run.
"""

import ast
import base64
from concurrent.futures import ThreadPoolExecutor
import re
import time
import zipfile
import zlib

from ownership_history import History, MAX_RUNS
from restore_receipts import BACKUP_PROFILE, HOME_FORMAT, NODE_MARKER
from support import OperationError, require

FORMAT_PATH = "fold_db/crates/core/src/storage/format.rs"


def candidate(history, run):
    if run.get("status") != "completed":
        return None
    try:
        # A baseline may have completed its independent source-free restore and
        # uploaded this receipt before a later protected-input update failed.
        return history.candidate(run, finished=False, kind="current-baseline")
    except (OperationError, KeyError, TypeError, ValueError, OverflowError,
            RuntimeError, zipfile.BadZipFile, zlib.error):
        return None


def producer(history):
    seen = set()
    expected_total = None
    page_size = 50
    for page in range(1, MAX_RUNS // page_size + 1):
        value = history.api("actions/workflows/cloud-backup-recovery.yml/runs"
                            f"?branch=main&event=workflow_dispatch&per_page={page_size}&page={page}")
        rows, total = value.get("workflow_runs"), value.get("total_count")
        require(isinstance(rows, list) and len(rows) <= page_size
                and type(total) is int and total >= len(rows)
                and expected_total in (None, total)
                and len(rows) == min(page_size, max(0, total - len(seen))),
                "ci_baseline_producer_directory")
        expected_total = total
        identifiers = [run.get("id") for run in rows]
        require(all(type(identifier) is int and identifier > 0 and identifier not in seen
                    for identifier in identifiers) and len(set(identifiers)) == len(rows),
                "ci_baseline_producer_directory")
        seen.update(identifiers)
        with ThreadPoolExecutor(max_workers=4) as pool:
            records = list(pool.map(lambda run: candidate(history, run), rows))
        matches = [record for record in records if record is not None]
        require(len(matches) <= 1, "ci_baseline_producer_ambiguous")
        if matches:
            return matches[0]["ownership"]["owner"]["source"]
        if len(rows) < page_size or len(seen) >= total:
            return None
    return None


def constant(source, name):
    pattern = (r"\bconst\s+" + name
               + r'\s*:[^=]+?=\s*(b?"(?:\\.|[^"\\])*"|[0-9]+)\s*;')
    matches = re.findall(pattern, source)
    require(len(matches) == 1, "ci_baseline_producer_format_definition")
    return ast.literal_eval(matches[0])


def inspect(operation, token):
    observation = {"observed": False, "restore_authority": "native_client"}
    history = None
    try:
        history = History(operation.baseline, operation.programs.environment, token)
        history.deadline = min(history.deadline, time.monotonic() + 45)
        source = producer(history)
        if source is None:
            return {**observation, "reason": "producer_receipt_unavailable"}
        entry = history.api(f"contents/{FORMAT_PATH}?ref={source}")
        require(entry.get("type") == "file" and entry.get("encoding") == "base64"
                and type(entry.get("size")) is int and 0 < entry["size"] <= 65536,
                "ci_baseline_producer_format_definition")
        raw = base64.b64decode(entry["content"], validate=False)
        require(len(raw) == entry["size"], "ci_baseline_producer_format_definition")
        text = raw.decode("utf-8")
        home, profile, marker = (constant(text, name) for name in (
            "HOME_FORMAT", "RECORD_PROFILE", "RECORD_NODE_BYTES"))
        require(type(home) is int and home > 0 and type(profile) is int and profile > 0
                and isinstance(marker, bytes) and len(marker) <= 128
                and marker.startswith(f"format={home}\n".encode()),
                "ci_baseline_producer_format_definition")
        return {"observed": True, "producer_source": source,
                "producer_home_format": home, "producer_backup_profile": profile,
                "client_home_format": HOME_FORMAT, "client_backup_profile": BACKUP_PROFILE,
                "compatible": (home, profile, marker) == (HOME_FORMAT, BACKUP_PROFILE, NODE_MARKER),
                "restore_authority": "native_client"}
    except (OperationError, KeyError, ValueError, TypeError, UnicodeError):
        return {**observation, "reason": "producer_evidence_unavailable"}
    finally:
        if history is not None:
            history.environment.pop("GH_TOKEN", None)

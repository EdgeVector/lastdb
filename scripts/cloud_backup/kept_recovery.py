"""Prove the saved latest successful CI snapshot after physical retirement."""

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from ci_control import decimal, digest
from node import OwnedNode
from ownership import REPOSITORY, WORKFLOW
from recovery_recipe import ROLES, validate as validate_recipe
from restore_receipts import NODE_V7, restored
from support import OperationError, canonical_uuid, require

# The first accepted main source whose client writes and restores only home
# format 7 (#929). A kept stream from an older source holds a format 4 to 6
# snapshot, which this client refuses by design. The service still keeps that
# stream until a newer successful CI stream replaces it.
FORMAT_SEVEN_SOURCE = "2cdc5531c1f056891ff17e16fbe25b52a5c4a201"
MAIN = "refs/remotes/origin/main"


def ancestor(older, newer):
    # Commit ancestry only: the full-history checkout has every main commit.
    root = Path(__file__).resolve().parents[2]
    # No credential or product variable reaches git; a missing commit fails, never fetches.
    environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C",
                   "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1"}
    try:
        result = subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", older, newer],
                                env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=60, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise OperationError("ci_kept_source_history") from error
    require(result.returncode in (0, 1), "ci_kept_source_history")
    return result.returncode == 0


def legacy_format(kept):
    """True when the kept stream's exact main source precedes the format 7 client."""
    owner = kept.get("owner") if isinstance(kept, dict) else None
    source = owner.get("source") if isinstance(owner, dict) else None
    require(isinstance(source, str) and re.fullmatch(r"[0-9a-f]{40}", source), "ci_kept_exact_owner")
    require(ancestor(source, MAIN) and ancestor(FORMAT_SEVEN_SOURCE, MAIN), "ci_kept_source_on_main")
    return not ancestor(FORMAT_SEVEN_SOURCE, source)


def selected(kept, baseline):
    require(isinstance(kept, dict) and kept.get("receipt_schema") == "lastdb.cloud-ci-stream.v1",
            "ci_kept_receipt_schema")
    stream = canonical_uuid(kept.get("stream_id"))
    require(stream != baseline["stream_id"], "ci_kept_excludes_baseline")
    owner = kept.get("owner")
    require(isinstance(owner, dict) and set(owner) == {
        "repository", "workflow", "source", "run_id", "run_attempt"}
        and owner["repository"] == REPOSITORY and owner["workflow"] == WORKFLOW
        and isinstance(owner["source"], str) and re.fullmatch(r"[0-9a-f]{40}", owner["source"]),
        "ci_kept_exact_owner")
    for name in ("run_id", "run_attempt"):
        require(decimal(owner[name]) > 0, "ci_kept_owner_number")
    require(decimal(kept.get("artifact_id")) > 0
            and decimal(kept.get("recovery_completed_ms")) > 0, "ci_kept_completed")
    for name in ("artifact_sha256", "registration_sha256", "receipt_sha256"):
        digest(kept.get(name))
    points = kept.get("verified_recovery_points")
    require(isinstance(points, list) and 3 <= len(points) <= 8, "ci_kept_supported_recipe")
    ordinals = {}
    verified = []
    for point in points:
        require(isinstance(point, dict) and set(point) == {
            "stream_id", "commit_id", "commit_sha256", "ordinal"}
            and point["stream_id"] == stream, "ci_kept_point_scope")
        require(canonical_uuid(point["commit_id"]).startswith("a17dbac2-"), "ci_kept_commit_id")
        digest(point["commit_sha256"])
        require(point["commit_id"] not in ordinals, "ci_kept_distinct_point")
        ordinals[point["commit_id"]] = decimal(point["ordinal"])
        verified.append({name: point[name] for name in ("stream_id", "commit_id", "commit_sha256")})
    recipe = validate_recipe(kept.get("recipe"), verified, owner["source"])
    ordered = [ordinals[recipe[role]["commit_id"]] for role in ROLES]
    require(ordered[0] < ordered[1] < ordered[2], "ci_kept_point_order")
    result = copy.deepcopy(baseline)
    result.update(recipe["final_snapshot"])
    for number, record in enumerate(result["records"]):
        if number == len(result["records"]) - 1:
            result["records"][number] = {"id": record["id"], "deleted": True}
        else:
            record["note"] = "Automatic log update " + str(number)
    result.update(file_bytes=int(recipe["live_file_bytes"]), file_sha256=recipe["live_file_sha256"])
    return result


def recover(operation, kept, credentials, phrase):
    expected = selected(kept, operation.baseline)
    parent = operation.root / "kept-latest"
    parent.mkdir(mode=0o700)
    destination = parent / "restored"
    report = operation.programs.restore(credentials, destination, expected, phrase)
    require(type(report.get("native_bytes")) is int and 0 < report["native_bytes"] <= 24 * 1024**2,
            "ci_kept_native_size")
    proof = restored(destination, report, expected, node_markers=(NODE_V7,))
    inventory = json.dumps(proof.pop("files"), sort_keys=True, separators=(",", ":")).encode()
    proof["native_inventory_sha256"] = hashlib.sha256(inventory).hexdigest()
    operation.node = OwnedNode(destination, operation.programs)
    reads = []
    for _number in range(2):
        operation.node.start()
        reads.append(operation.node.verify_records(expected))
        operation.node.stop()
    require(operation.node.clean_stops == 2 and not operation.node.forced,
            "ci_kept_two_clean_stops")
    return {"stream_id": expected["stream_id"], "commit_id": expected["commit_id"],
            "commit_sha256": expected["commit_sha256"], "restore": proof,
            "restored_boots": 2, "strict_clean_stops": 2, "keyed_reads": reads}

"""Inspect a completed restore in a caller-owned fresh private CI home.

The product authenticates encrypted native metadata and the complete inventory.
This module binds its exact selection, completed receipt, and saved per-file
proofs, then independently hashes the final files before the first normal boot.
It never reads source homes, credentials, payload metadata, or encryption keys.
"""

import hashlib
import os
from pathlib import Path
import re
import stat

from support import dev_endpoint, require, read_json, hash_file
from restore_log import position, verified as verified_log_routes

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
HEX = re.compile(r"[0-9a-f]{64}")
NIL = "00000000-0000-0000-0000-000000000000"
# Every home of this release is format 7 (the record model), backup profile 5.
# The product restores no older snapshot, so a format 4 to 6 marker is refused
# here too. Kept recovery hashes the restored bytes in place; it does not
# upgrade the marker.
NODE_V7 = b"format=7\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\nalternate-keys=v2\nrecords=v1\n"
NODE_PROFILE = {NODE_V7: (5, 7)}
MAX_FILES = 4096


def directory(path):
    require(path.is_absolute() and path.resolve(strict=True) == path, "private_directory_path")
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "private_directory")


def owned(home, relative):
    directory(home)
    target = home / relative
    require(target.is_relative_to(home) and ".." not in target.parts, "owned_path")
    parent = home
    for component in target.relative_to(home).parts[:-1]:
        parent /= component
        directory(parent)
    return target


def names(path):
    directory(path)
    result = set()
    with os.scandir(path) as entries:
        for entry in entries:
            require(len(result) < MAX_FILES, "native_directory_bound")
            result.add(entry.name)
    return result


def digest(value):
    require(isinstance(value, str) and HEX.fullmatch(value), "native_digest")
    return value


def nonnegative(value):
    require(type(value) is int and 0 <= value <= 18446744073709551615, "native_integer")
    return value


def native_path(relative):
    require(isinstance(relative, str), "native_path_type")
    if relative == "laststore-pack-node-v1":
        return
    for name in ("disk-run", "disk-cas"):
        if relative in (name + "/CURRENT", name + "/laststore-run-home-v2"):
            return
        prefix = name + "/runs/"
        if relative.startswith(prefix) and relative.endswith(".ldrun"):
            identifier = relative[len(prefix):-6]
            require(UUID.fullmatch(identifier) and identifier != NIL, "native_run_path")
            return
    require(False, "native_path_allowlist")


def closure(home, files):
    require(names(owned(home, "data/laststore-pack-node-v1").parent)
            == {"laststore-pack-node-v1", "disk-run", "disk-cas"}, "restored_root_closure")
    paths = {"laststore-pack-node-v1"}
    for name in ("disk-run", "disk-cas"):
        require(names(owned(home, "data/" + name + "/CURRENT").parent)
                == {"CURRENT", "laststore-run-home-v2", "LOCK", "runs"}, "restored_home_closure")
        hash_file(owned(home, "data/" + name + "/LOCK"), 0)
        paths.update(name + "/" + control for control in ("CURRENT", "laststore-run-home-v2"))
        runs = owned(home, "data/" + name + "/runs/placeholder").parent
        for filename in sorted(names(runs)):
            require(filename.endswith(".ldrun") and UUID.fullmatch(filename[:-6])
                    and filename[:-6] != NIL and len(paths) < MAX_FILES, "restored_run_name")
            paths.add(name + "/runs/" + filename)
    require(paths == {item["path"] for item in files}, "restored_native_file_closure")


def selection(home, baseline):
    candidates = [name for name in names(home.parent) if name.startswith(".byte-cloud-restore-")]
    require(len(candidates) == 1, "restore_session_count")
    identifier = candidates[0][len(".byte-cloud-restore-"):]
    require(UUID.fullmatch(identifier) and identifier != NIL, "restore_session_name")
    root = home.parent / candidates[0]
    selected = read_json(owned(root, "selection.json"))
    require(selected["version"] == 2 and selected["session_id"] == identifier
            and selected["destination"] == str(home)
            and selected["endpoint"] == dev_endpoint() + "/api/cloud/backups/v2/", "restore_selection")
    accepted = selected["accepted"]
    begin = accepted["begin"]
    stream = begin["job"]["stream"]
    require(accepted["version"] == 2 and begin["version"] == 2 and begin["descriptor"] is None
            and begin["required_destinations"] == ["r2", "b2"]
            and stream["scope"]["account_id"] == baseline["account"]
            and stream["scope"]["account_generation"] == baseline["account_generation"]
            and stream["stream_id"] == baseline["stream_id"]
            and begin["job"]["commit_id"] == baseline["commit_id"]
            and digest(accepted["commit_sha256"]) == baseline["commit_sha256"], "accepted_restore_selection")
    return root, identifier


def node_marker(home):
    path = owned(home, "data/laststore-pack-node-v1")
    require(path.stat().st_size <= 128, "node_marker_bound")
    value = path.read_bytes()
    require(value in NODE_PROFILE, "unsupported_node_marker")
    return value


def matched_node_marker(size, expected, node_markers):
    matched = None
    for marker in node_markers:
        if size == len(marker) and expected == hashlib.sha256(marker).hexdigest():
            matched = marker
    require(matched in NODE_PROFILE, "format_seven_marker")
    return matched


def inventory(home, root, node_markers=(NODE_V7,)):
    entries = owned(root, "entries/placeholder").parent
    files, seen, matched = [], set(), None
    for name in sorted(names(entries)):
        digest(name)
        item = read_json(owned(root, "entries/" + name))
        require(isinstance(item, dict) and set(item) == {"path", "sha256", "bytes"}, "native_proof_shape")
        path, size, expected = item["path"], nonnegative(item["bytes"]), digest(item["sha256"])
        native_path(path)
        require(path not in seen and size > 0 and name == hashlib.sha256(path.encode()).hexdigest(),
                "native_proof_identity")
        require(hash_file(owned(home, "data/" + path), size) == expected, "restored_full_native_hash")
        if path == "laststore-pack-node-v1":
            matched = matched_node_marker(size, expected, node_markers)
        elif path.endswith("/laststore-run-home-v2"):
            require(size == 64, "restored_descriptor_length")
        seen.add(path)
        files.append({"path": path, "native_sha256": expected, "native_bytes": size})
    closure(home, files)
    return sorted(files, key=lambda item: item["path"]), matched


def verified_prefixes(root, replay, accepted, observe):
    read = lambda path: read_json(owned(root, path))
    chain_names = names(owned(root, "chain/plan.json").parent)
    routes = verified_log_routes(read, chain_names, replay, accepted, observe)
    if "prefixes" not in replay:
        return routes
    stages = ("created", "updated_and_deleted", "both_deleted")
    values = replay["prefixes"]
    require(isinstance(values, dict) and set(values) == set(stages), "three_restored_mutation_prefixes")
    ordered = [position(values[name]) for name in stages]
    require(ordered[0]["epoch"] == ordered[1]["epoch"] == ordered[2]["epoch"]
            and ordered[0]["sequence"] < ordered[1]["sequence"] < ordered[2]["sequence"]
            and ordered[-1] == replay["minimum"], "ordered_restored_mutation_prefixes")
    proofs = {}
    for name, minimum in zip(stages, ordered):
        proof = verified_log_routes(read, chain_names, {"base": replay["base"], "minimum": minimum},
                                    accepted, observe)
        require(proof == routes, "same_complete_selected_chain_for_every_prefix")
        proofs[name] = {"sequence": minimum["sequence"], "sha256": minimum["sha256"],
                        "written_prefix_verified": proof["written_prefix_verified"]}
    return {**routes, "written_prefixes": proofs}


def replayed_inventory(home, root, replay, accepted, observe):
    routes = verified_prefixes(root, replay, accepted, observe)
    paths = ["laststore-pack-node-v1"]
    for name in ("disk-run", "disk-cas"):
        paths.extend(name + "/" + control for control in ("CURRENT", "laststore-run-home-v2"))
        runs = owned(home, "data/" + name + "/runs/placeholder").parent
        paths.extend(name + "/runs/" + filename for filename in sorted(names(runs)))
    require(len(paths) <= MAX_FILES, "replayed_file_count")
    files = []
    for path in sorted(paths):
        native_path(path)
        file = owned(home, "data/" + path)
        size = nonnegative(file.lstat().st_size)
        files.append({"path": path, "native_bytes": size, "native_sha256": hash_file(file, size)})
    closure(home, files)
    return files, routes


def restored(home: Path, report: dict, baseline: dict, replay=None, observe=None,
             node_markers=(NODE_V7,)) -> dict:
    """Return safe native hash evidence after the CLI's completed offline restore.

    The caller validates baseline selectors before it invokes the CLI. The
    destination remains unbooted until this function returns. Session metadata
    stays in memory; only safe hashes, totals, and check results leave here.
    """
    root, identifier = selection(home, baseline)
    commit = digest(baseline["commit_sha256"])
    require(report["commit_sha256"] == commit and report["destination"] == str(home)
            and report["file_dependencies_verified"] is True
            and nonnegative(report["native_files"]) > 0
            and nonnegative(report["native_bytes"]) > 0
            and nonnegative(report["verified_rows"]) >= 0
            and nonnegative(report["verified_external_files"]) == 0, "restore_report_binding")
    receipt_path = owned(home, "byte-cloud-restore-receipt.json")
    receipt = read_json(receipt_path, max_bytes=65536)
    require(isinstance(receipt, dict) and set(receipt) == {"version", "session_id", "report"}
            and receipt["version"] == 2 and receipt["report"] == report
            and receipt["session_id"] == identifier
            and read_json(owned(root, "result.json")) == report, "restore_receipt")
    require(not os.path.lexists(owned(home, "native-cloud-restore-receipt.json"))
            and not os.path.lexists(owned(home, "current-session.json"))
            and not os.path.lexists(owned(home, "data/folddb.sock")), "restore_before_normal_boot")
    routes = None
    if replay is None:
        files, marker = inventory(home, root, node_markers)
    else:
        # Replay restores a fresh format 7 run.
        files, routes = replayed_inventory(home, root, replay,
            read_json(owned(root, "selection.json"))["accepted"], observe)
        marker = node_marker(home)
    require(len(files) == report["native_files"]
            and sum(item["native_bytes"] for item in files) == report["native_bytes"], "restore_native_totals")
    profile_version, node_format = NODE_PROFILE[marker]
    result = {"schema": "lastdb.run-v2-restored-files.v1", "accepted_commit_sha256": commit,
            "native_files": len(files), "native_bytes": report["native_bytes"], "files": files,
            "receipt_sha256": hash_file(receipt_path), "profile_version": profile_version,
            "node_format": node_format,
            "required_destinations": ["r2", "b2"], "product_inventory_closure_verified": True,
            "independent_native_file_hashes_verified": replay is None,
            "final_native_file_hashes_recorded": True, "selected_log_chain_verified": replay is not None,
            "native_file_closure_verified": True,
            "before_first_normal_boot": True}
    if routes is not None:
        result["mutation_log_routes"] = routes
    return result

#!/usr/bin/env python3
"""Require isolated compiled source provenance before writer capability checks."""

import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import time

from build_variant import ROOT, digest, frozen, save
import accept_builds


def load(name):
    return json.loads((ROOT / name).read_text())


def engine_dependency_files(repo, target):
    directory = target / "release/deps"
    paths = list(directory.iterdir())
    if len(paths) > 20000:
        raise ValueError("The explicit compiler dependency directory exceeds its finite entry bound")
    result = []
    for package in ("laststore", "fold_db"):
        selected = [path for path in paths if path.name.startswith(package + "-") and path.suffix == ".d"]
        if not selected or len(selected) > 16:
            raise ValueError("The local engine/core dependency evidence is absent or ambiguous")
        for path in selected:
            raw = path.read_bytes()
            if path.is_symlink() or len(raw) > 8 * 1024 * 1024:
                raise ValueError("A local compiler dependency file is linked or oversized")
            first = raw.decode().splitlines()[0]
            inputs = shlex.split(first.split(": ", 1)[1])
            local, generated = [], []
            for name in inputs:
                source = Path(name)
                source = (source if source.is_absolute() else repo / source).resolve(strict=True)
                if source.is_relative_to(repo):
                    local.append(str(source))
                elif source.is_relative_to(target):
                    generated.append(str(source))
                else:
                    raise ValueError("A compiled local library references a different source worktree")
            if not local:
                raise ValueError("A compiled library lacks its local source inputs")
            result.append({"package": package, "path": str(path), "sha256": digest(path),
                           "source_paths": sorted(set(local)), "generated_paths": sorted(set(generated)),
                           "relative_paths_resolved_against_recorded_compiler_cwd": str(repo)})
    return result


def symbols(binary):
    output = subprocess.check_output(["nm", "-j", str(binary)], text=True, stderr=subprocess.DEVNULL)
    if len(output) > 32 * 1024 * 1024:
        raise ValueError("The diagnostic symbol listing exceeds its finite bound")
    selected = [line for line in output.splitlines() if "disk_pack" in line and "live_parts" in line]
    return {"argv": ["nm", "-j", str(binary)], "live_part_symbol_count": len(selected),
            "sample_symbols": selected[:32], "auxiliary_only": True}


def check_variant(variant):
    node, disk = load(variant + "-build.json"), load(variant + "-disk-build.json")
    owner = load(variant + "-isolated-owner.json")
    root, repo = Path(owner["root"]), Path(node["source_worktree"])
    info = root.stat()
    if (root.is_symlink() or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or (info.st_dev, info.st_ino, info.st_uid) != (owner["device"], owner["inode"], os.getuid())
            or stat.S_IMODE(info.st_mode) != 0o700 or load_marker(root) != owner):
        raise ValueError("The isolated target owner differs from its original private identity")
    target = root / "target"
    frozen(repo, node["revision"])
    graph = load(variant + "-isolated-source-graph.json")
    if graph["metadata_target_directory"] != str(target) or not graph["all_local_manifests_under_source"]:
        raise ValueError("Cargo metadata did not select its distinct fresh target/source graph")
    for row in graph["local_packages"]:
        manifest = Path(row["manifest"])
        if not manifest.is_relative_to(repo) or digest(manifest) != row["manifest_sha256"]:
            raise ValueError("An accepted local manifest changed source identity")
    items = [node, load(variant + "-cli-build.json"), *disk["binaries"].values()]
    for item in items:
        source, copied = Path(item["isolated_target_output"]), Path(item["binary"])
        if (not source.is_relative_to(target) or copied.parent != root / "bin"
                or digest(source) != item["binary_sha256"] or digest(copied) != item["binary_sha256"]):
            raise ValueError("An accepted binary differs from its own isolated compiled target")
    if (node["fresh_empty_per_variant_target"] is not True
            or node["build_environment"]["CARGO_TARGET_DIR"] != str(target)
            or len(node["build_environment"]["compiler_wrappers_disabled"]) != 4):
        raise ValueError("The original empty-target/compiler-wrapper predicates are absent")
    return {"variant": variant, "owner": owner, "source_worktree": str(repo),
            "engine_and_core_dependencies": engine_dependency_files(repo, target),
            "node_symbols": symbols(Path(node["binary"])),
            "writer_symbols": symbols(Path(disk["binaries"]["lastdb-disk-home"]["binary"])),
            "source_graph_sha256": digest(ROOT / (variant + "-isolated-source-graph.json")),
            "binary_source_paths_sha256": digest(ROOT / (variant + "-compiled-source-paths.json"))}


def load_marker(root):
    return json.loads((root / "owner.json").read_text())


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for final source/build acceptance")
    target = ROOT / "isolated-build-acceptance.json"
    if target.exists():
        raise ValueError("Do not replace a final isolated source acceptance receipt")
    rows = [check_variant(variant) for variant in ("before", "after")]
    before, after = load("before-disk-build.json"), load("after-disk-build.json")
    if any(before["binaries"][name]["binary_sha256"] == after["binaries"][name]["binary_sha256"] for name in before["binaries"]):
        raise ValueError("Old and counted-format physical tools unexpectedly share identical linked bytes")
    accept_builds.main()
    accepted = load("final-build-checks.json")
    accepted.pop("symmetric_cache_preparation")
    accepted["binary_acceptance_helper_sha256"] = accepted["acceptance_driver_sha256"]
    accepted.update(acceptance_driver_sha256=digest(Path(__file__)),
                    symmetric_compilation_isolation="Fresh empty target per source variant; compiler wrappers disabled",
                    isolated_owner_receipts=["before-isolated-owner.json", "after-isolated-owner.json"],
                    isolated_source_graphs=["before-isolated-source-graph.json", "after-isolated-source-graph.json"],
                    rejected_shared_target_method="rejected-build-1", writer_capability_gate_still_required=True)
    save(ROOT / "final-build-checks.json", accepted)
    save(target, {"complete": True, "at_unix_ns": time.time_ns(), "variants": rows,
         "distinct_empty_compilation_targets": True, "real_writer_feature_gate_required_before_any_profile": True,
         "driver_sha256": digest(Path(__file__)), "final_binary_acceptance_sha256": digest(ROOT / "final-build-checks.json")})
    print(json.dumps({"isolated_source_and_binary_acceptance": True, "capability_gate_still_required": True}))


if __name__ == "__main__":
    main()

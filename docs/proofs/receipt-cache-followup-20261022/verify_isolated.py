#!/usr/bin/env python3
"""Accept isolated source builds and real writer formats before any profile."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from reuse import ROOT, original
import accept_isolated


def load(name):
    return json.loads((ROOT / name).read_text())


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use one standard exclusive lock for source and runtime acceptance")
    target = ROOT / "writer-capabilities.json"
    if target.exists():
        raise ValueError("Do not replace a real writer acceptance receipt")
    accept_isolated.main()
    paths = [ROOT / name for name in ("capability_capture.py", "auto_retained.py", "retention_capture.py",
                                     "reuse.py", "verify_isolated.py")]
    checksums = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    value = {"complete": False, "runs": [], "helpers_sha256": checksums, "start_unix_ns": time.time_ns()}
    try:
        for variant in ("before", "after"):
            build, disk = load(variant + "-build.json"), load(variant + "-disk-build.json")
            output = ROOT / (variant + "-writer-capability")
            if output.exists():
                raise ValueError("Do not replace a prior capability capture")
            scratch = Path(tempfile.mkdtemp(prefix="rccap.", dir=os.environ["TMPDIR"]))
            argv = [sys.executable, str(ROOT / "capability_capture.py"), "--binary", build["binary"],
                    "--disk-binary", disk["binaries"]["lastdb-disk-home"]["binary"],
                    "--source-revision", build["revision"], "--label", output.name,
                    "--scratch", str(scratch), "--output", str(output),
                    "--expected-counted", "yes" if variant == "after" else "no"]
            row = {"variant": variant, "argv": argv, "start_unix_ns": time.time_ns()}
            value["runs"].append(row)
            process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            if process.returncode:
                raise RuntimeError("Stop before profiling at a failed real writer capability check")
            summary = load(output.name + "/summary.json")
            if summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True:
                raise ValueError("The writer capability capture or shutdown is incomplete")
            original("run_pairs").cleanup(output.resolve(strict=True))
            row.update(home_removed=not scratch.exists(), node_features=summary["node_created"]["features"],
                       tool_features=summary["physical_writer"]["features"])
        if any(hashlib.sha256(Path(path).read_bytes()).hexdigest() != checksum for path, checksum in checksums.items()):
            raise ValueError("A frozen capability helper changed during acceptance")
        value["complete"] = True
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"real_node_and_physical_writer_capabilities": True, "variants": 2}), flush=True)


if __name__ == "__main__":
    main()

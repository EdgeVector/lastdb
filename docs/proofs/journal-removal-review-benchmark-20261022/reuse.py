"""Import frozen first-round helpers from explicit proof paths."""

import importlib.util
import json
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
ORIGINAL = ROOT.parent / "journal-removal-benchmark-20261022"
sys.path.insert(0, str(ORIGINAL))
MODULES = {}


def original(name):
    if name not in MODULES:
        spec = importlib.util.spec_from_file_location(f"journal_review_original_{name}", ORIGINAL / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        MODULES[name] = module
    return MODULES[name]


def helper_files():
    paths = [ROOT / name for name in ("reuse.py", "benchmark.py", "focused.py", "run_pairs.py", "summarize.py")]
    paths += [ORIGINAL / name for name in ("benchmark.py", "benchmark_support.py", "run_pairs.py", "summarize.py")]
    paths += [ROOT.parent / "snapshot-commits-20261022" / name for name in ("capture.py", "cleanup.py")]
    paths += [ROOT.parent / "write-overhead-20261021" / name for name in ("measure.py", "io_probe.py")]
    return paths


def capture(args):
    value = original("benchmark").Benchmark(args)
    metadata = json.loads((value.output / "inputs.json").read_text())
    from benchmark_support import file_hash, save
    metadata["helpers_sha256"] = {str(path): file_hash(path) for path in helper_files()}
    metadata.update(benchmark_round=2, original_workload_unchanged=True,
                    focused_probe_sweep={"requests_per_role": 64, "first_delay_seconds": 0.02,
                                         "nominal_interval_seconds": 0.05,
                                         "warm_read_key": 1, "durable_write_key": 400,
                                         "position": "After complete matched sequence and graceful reopen"})
    save(value.output / "inputs.json", metadata)
    return value


def validate_revisions(root, before, after):
    builds = {}
    for label, revision in (("before", before), ("after", after)):
        if not re.fullmatch("[0-9a-f]{40}", revision):
            raise ValueError("Provide the full frozen source commit for each variant")
        value = json.loads((root / f"{label}-build.json").read_text())
        if value.get("revision") != revision or value.get("product_source_clean") is not True:
            raise ValueError(f"The {label} build receipt differs from its required clean source commit")
        builds[label] = value
    return builds

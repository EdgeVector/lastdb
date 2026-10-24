#!/usr/bin/env python3
"""Run Tom's one-time native cleanup operation on generated data in hosted CI."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/benchmarks"))
from node import Node, file_hash, save
from workload import PROFILES, Workload


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("One-time cleanup operation interrupted")


class Upgrade(Workload):
    def __init__(self, node, output, args):
        recipe = {**PROFILES["standard"], "native_timing": False}
        super().__init__(node, recipe, output, minimum_retired=12048)
        self.args = args

    def helper(self, mode):
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.node.home), FOLDDB_HOME=str(self.node.home),
                           RUST_MIN_STACK="16777216")
        with (self.output / f"helper-{mode}.log").open("w") as log:
            result = subprocess.run([str(self.args.helper), mode, str(self.node.home)],
                                    env=environment, stdout=subprocess.PIPE, stderr=log,
                                    text=True, timeout=180, check=True)
        value = json.loads(result.stdout)
        save(self.output / f"helper-{mode}.json", value)
        return value

    def seed(self):
        super().seed()
        save(self.output / "seed-version.json", json.loads((self.output / "version.json").read_text()))
        self.node.stop()
        save(self.output / "seed-shutdown.json", json.loads((self.output / "shutdown.json").read_text()))
        (self.node.root / ".retry-cleanup-ci").write_text("generated data only\n")
        self.helper("seed")
        self.upgrade_target = (self.args.candidate, self.args.candidate_revision, 0)
        self.node.binary, self.node.revision, self.node.workers = self.upgrade_target
        self.node.start()
        result, _, _ = self.node.call("/api/admin/shed", {})
        save(self.output / "cold-boundary.json", result["shed"])
        if result["shed"].get("target_met") is not True:
            raise RuntimeError("The candidate cold boundary is incomplete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "helper", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("baseline-revision", "candidate-revision"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("This one-time drill only runs on a hosted GitHub runner")
    signal.signal(signal.SIGTERM, interrupted)
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "programs.json", {
        name: {"path": str(getattr(args, name)), "sha256": file_hash(getattr(args, name))}
        for name in ("baseline", "candidate", "helper")})
    node = Node(args.baseline, args.baseline_revision, args.output)
    try:
        node.start()
        workload = Upgrade(node, args.output, args)
        workload.run()
        compact, _, _ = node.call("/api/db/compact", {"dry_run": False, "full_copy": True, "grace_ms": 0})
        save(args.output / "compaction.json", compact)
        receipt = compact["compact"]["native_disk"]
        if (receipt.get("executed") is not True or receipt.get("dry_run") is not False
                or receipt.get("reclaim_deferred") is not False
                or receipt.get("reclaim_remaining") is not False or receipt.get("reclaim_error")):
            raise RuntimeError("The one-time compaction did not complete")
        node.stop()
        preserved = workload.helper("verify")
        node.start()
        native, _, _ = node.call("/api/db/native-status")
        workload.verify_upgrade(native)
        record, _, _ = node.call("/api/query", {"schema_name":"GitHubBenchmark",
            "fields":["bucket", "id", "title", "body"],
            "filter":{"HashRangeKeys":[["generated", "signed-retry"]]}, "limit":1})
        if (record.get("returned_count") != 1 or record["results"][0]["fields"] != {
                "bucket":"generated", "id":"signed-retry", "title":"authored retry", "body":"unchanged"}):
            raise RuntimeError("The authored retry record differs after restart")
        proof = {"candidate_revision":args.candidate_revision, "baseline_revision":args.baseline_revision,
                 "metadata":preserved, "upgrade":json.loads((args.output / "upgrade-complete.json").read_text()),
                 "restart_and_compaction":True}
        save(args.output / "complete.json", proof)
        print(json.dumps(proof), flush=True)
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write("## One-time mixed legacy cleanup\n\n" + json.dumps(proof, indent=2) + "\n")
    finally:
        node.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Check real erase acknowledgements and recovery; do not measure performance."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("integrated", PROOF / "capture.py")
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError("scratch owner differs")
    output = PROOF / "final-erases"
    output.mkdir()
    home = scratch / "he"
    home.mkdir()
    binary = scratch / "final"
    node = capture.capture.Node(binary, home, scratch / "final-erases.log", 4096, False, workers=8)
    node.profile, node.maintenance = False, True
    capture.save(output / "identity.json", {"performance_claim": False, "compiler_exclusion": False,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "generated_data_only": True})
    calls = []
    try:
        node.start()
        capture.schemas.seed(node, 2, output)
        for number, operation in enumerate(("create", "delete", "create", "purge")):
            items = [capture.schemas.payload(capture.schemas.schema_name(n), number, 1, operation)[0]["mutations"][0]
                     for n in range(2)]
            if operation in ("delete", "purge"):
                for item in items:
                    item["fields_and_values"] = {}
            response = node.call("/api/mutations/batch", {"mutations": items})
            calls.append({"operation": operation, "response": response})
            capture.save(output / "calls.json", calls)
            if response.get("durability") != "durable" or len(response.get("mutation_ids", [])) != 2:
                raise RuntimeError("erase check did not return two durable identities")
            if operation not in ("delete", "purge"):
                continue
            capture.save(output / (operation + "-stop.json"), {"exit_code": node.stop(signal.SIGKILL)})
            capture.save(output / (operation + "-boundary.json"), node.stopped_boundaries())
            node.start()
            queries = [{"schema_name": capture.schemas.schema_name(n), "fields": ["value"],
                "filter": {"HashRangeKeys": [[capture.schemas.schema_name(n), "record-000"]]}, "limit": 1}
                for n in range(2)]
            results = capture.independent_queries(node, queries)
            notices = capture.notices(node)
            identities = [identity for call in calls for identity in call["response"]["mutation_ids"]]
            capture.save(output / (operation + "-recovery.json"), {"queries": results, "notices": notices})
            if any(result["results"] for result in results):
                raise RuntimeError("erased key reappeared after forced stop")
            if len(notices) != len(identities) or {n["mutation_id"] for n in notices} != set(identities):
                raise RuntimeError("erase notices differ after forced stop")
        capture.save(output / "shutdown.json", {"exit_code": node.stop()})
        capture.save(output / "native-shutdown.json", json.loads((home / ".shutdown_flush_ready").read_text()))
    except Exception as error:
        capture.save(output / "failure.json", {"error": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        capture.preserve_log(scratch, "final-erases", output)


if __name__ == "__main__":
    main()

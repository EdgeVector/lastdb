#!/usr/bin/env python3
"""Record actual receipt and notice identities for a repeated batch on a fresh daemon."""

import argparse
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]
spec = importlib.util.spec_from_file_location(
    "durable_capture", PROOF.parent / "snapshot-write-timing-20261022/capture.py")
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_uid != os.getuid() or any(scratch.iterdir()):
        raise RuntimeError("use the fresh owned temporary root")
    binary = scratch / "candidate"
    shutil.copyfile(ROOT / "target/dev-release/lastdbd", binary)
    binary.chmod(0o755)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if digest != "18b61d57b703ecb0b676fcfd52aa1e2604bf87bba6b273e3e9e9587abdc70871":
        raise RuntimeError("the candidate differs from the final benchmark binary")
    home = scratch / "h"
    home.mkdir()
    output = PROOF / "receipts"
    output.mkdir()
    node = capture.Node(binary, home, scratch / "node.log", 4096, False, workers=8)
    node.profile, node.maintenance = False, True
    capture.save(output / "identity.json", {"binary_sha256": digest, "home": str(home),
        "uid": os.getuid(), "generated_data_only": True, "performance_claim": False})
    try:
        node.start()
        capture.base.schemas.seed(node, 2, output)
        names = [capture.base.schemas.schema_name(n) for n in range(2)]
        requests = []
        for operation, value in (("create", "initial"), ("update", "next"), ("update", "next")):
            body = {"mutations": [{"type": "mutation", "schema": name,
                "mutation_type": operation, "key_value": {"hash": name, "range": "record-000"},
                "fields_and_values": {"bucket": name, "id": "record-000", "value": value}}
                for name in names]}
            requests.append({"request": body, "response": node.call("/api/mutations/batch", body)})
        live = node.call("/api/app/changes", {"limit": 500})
        capture.save(output / "requests.json", requests)
        capture.save(output / "live-notices.json", live)
        capture.save(output / "first-stop.json", {"exit_code": node.stop()})
        node.start()
        notices = node.call("/api/app/changes", {"limit": 500})
        capture.save(output / "reopened-notices.json", notices)
        returned = {identity for item in requests for identity in item["response"].get("mutation_ids", [])}
        observed = {item["mutation_id"] for item in notices["changes"]}
        capture.save(output / "observations.json", {
            "first_update_ids": requests[1]["response"].get("mutation_ids"),
            "repeat_update_ids": requests[2]["response"].get("mutation_ids"),
            "repeat_returns_prior_ids": requests[1]["response"].get("mutation_ids") == requests[2]["response"].get("mutation_ids"),
            "unique_returned_ids": sorted(returned), "notice_ids_absent_from_receipts": sorted(observed - returned),
            "reopened_notice_count": len(notices["changes"]),
            "all_receipts_durable": all(item["response"].get("durability") == "durable" for item in requests)})
        print((output / "observations.json").read_text(), flush=True)
        capture.save(output / "shutdown.json", {"exit_code": node.stop()})
    except Exception as error:
        capture.save(output / "failure.json", {"error": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        raw = (scratch / "node.log").read_bytes()
        packed = gzip.compress(raw, mtime=0)
        (output / "daemon.txt.gz").write_bytes(packed)
        capture.save(output / "log-receipt.json", {"original_sha256": hashlib.sha256(raw).hexdigest(),
            "gzip_sha256": hashlib.sha256(packed).hexdigest()})


if __name__ == "__main__":
    main()

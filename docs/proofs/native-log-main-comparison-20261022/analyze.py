#!/usr/bin/env python3
"""Summarize retained real-daemon receipts for the pinned main comparison."""

import json
from pathlib import Path


def load(path):
    return json.loads(path.read_text())


def summarize(root, run):
    path = root/run["name"]
    inputs, writes = load(path/"inputs.json"), load(path/"writes.json")
    samples = writes["samples"]
    recovery = load(path/"recovery.json")
    assert len(samples) == inputs["count"] and all(row["ok"] for row in samples)
    assert load(path/"memory-read.json")["all_values_match"] and recovery["all_values_match"]
    assert load(path/"shutdown.json")["exit_code"] == 0
    assert load(path/"native-shutdown.json")["flush_ok"]
    if run["mode"] == "candidate":
        assert writes["durability_receipt_counts"] == {"durable": inputs["count"]}
        assert recovery["clock_advanced"] and recovery["next_write"]["durability"] == "durable"
    events = []
    for row in samples:
        events += [(row["start_unix_us"],1,row["schema"]),(row["end_unix_us"],-1,row["schema"])]
    active, maximum = {}, 0
    for _, delta, schema in sorted(events):
        active[schema] = active.get(schema,0)+delta
        maximum = max(maximum,active[schema])
    assert maximum == 1 and len({row["schema"] for row in samples}) == inputs["schemas"]
    return {"run":run["name"],"mode":run["mode"],"source_commit":inputs["source_commit"],
            "binary_sha256":inputs["binary_sha256"],"clients":inputs["clients"],
            "records_per_batch":inputs["records_per_batch"],"schemas":inputs["schemas"],
            "same_schema_overlap":False,"summary":writes["summary"],
            "durability_receipt_counts":writes["durability_receipt_counts"],
            "known_final_records":recovery["actual"],"recovered_values_match":True,
            "recovery_method":"forced stop after durable sentinel" if run["mode"] == "candidate"
                else "explicit flush and graceful stop after memory acknowledgments",
            "parts_before":writes["before"]["native_disk"]["parts"],
            "parts_after":writes["after"]["native_disk"]["parts"],
            "writer_failed":writes["after"]["native_disk"]["writer_failed"]}


def main():
    root = Path(__file__).resolve().parent
    plan = load(root/"plan.json")
    rows = [summarize(root,run) for run in plan["runs"]]
    (root/"matrix.json").write_text(json.dumps(rows,indent=2)+"\n")
    comparisons = []
    for label,clients,batch in (("small",1,1),("batch",1,8),("four-clients",4,8)):
        main = next(row for row in rows if row["mode"] == "main" and row["clients"] == clients and row["records_per_batch"] == batch)
        candidate = next(row for row in rows if row["mode"] == "candidate" and row["clients"] == clients and row["records_per_batch"] == batch)
        old,new = main["summary"],candidate["summary"]
        comparisons.append({"workload":label,"clients":clients,"records_per_batch":batch,
            "main":old,"candidate":new,
            "average_candidate_to_main_ratio":new["average_ms"]/old["average_ms"],
            "average_reduction_percent":100*(1-new["average_ms"]/old["average_ms"]),
            "p99_reduction_percent":100*(1-new["p99_ms"]/old["p99_ms"]),
            "throughput_candidate_to_main_ratio":new["batches_per_second"]/old["batches_per_second"]})
    (root/"comparison.json").write_text(json.dumps(comparisons,indent=2)+"\n")
    print(json.dumps(comparisons,indent=2))


if __name__ == "__main__":
    main()

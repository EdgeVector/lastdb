"""Summarize existing per-query receipts without inferring unmeasured guard work."""

import statistics


def observations(grouped, mode):
    lines = ["", "Native costs for identical narrow requests (median of two per-run request means):", "",
        "| Mode / source | Internal key requests | Warm hits | File read calls | Read bytes | Peak transient bytes |",
        "|---|---:|---:|---:|---:|---:|"]
    memory = []
    for role, values in grouped.items():
        means = []
        for value in values:
            receipts = [sample["storage_read_receipt"] for sample in value["narrow_samples"]]
            means.append({name: statistics.mean(receipt[name] for receipt in receipts)
                          for name in ("requested_keys", "logical_warm_hits", "read_operations", "temporary_bytes_peak")})
            means[-1]["read_bytes"] = statistics.mean(sum(receipt[name] for name in (
                "legacy_replay_bytes", "footer_bytes", "payload_bytes", "format_probe_bytes")) for receipt in receipts)
            memory.append("| " + mode + " / " + role + " / pair " + str(value["pair"]) + " | "
                + " | ".join(str(value["narrow_memory_" + phase]["process_memory_bytes"][name])
                    for phase, name in (("before", "VmRSS"), ("after", "VmRSS"), ("after", "VmHWM"))) + " |")
        names = ("requested_keys", "logical_warm_hits", "read_operations", "read_bytes", "temporary_bytes_peak")
        numbers = [f"{statistics.median(row[name] for row in means):.1f}" for name in names]
        lines.append("| " + mode + " / " + role + " | " + " | ".join(numbers) + " |")
    lines.extend(["", "Internal key requests count logical storage requests, including repeated requests. They are not the native guard's examined-key count.",
        "Read bytes include OS-cache reads. Transient bytes omit returned bodies and warm caches.", "",
        "| Mode / source / pair | Process memory before bytes | Process memory after bytes | Process lifetime peak bytes |",
        "|---|---:|---:|---:|", *memory, "",
        "Memory before/after the narrow requests is an observation. The lifetime peak can include startup and seed work.",
        "Neither these samples nor transient buffer receipts establish a total-memory cap or an in-flight request peak."])
    return lines

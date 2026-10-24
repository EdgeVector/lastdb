"""Print all matched condition measurements without pooling independent runs."""

import statistics


def summarize(values, phase, label):
    cells = [next(cell for cell in value["conditions"]["cells"]
                  if (cell["phase"], cell["label"]) == (phase, label)) for value in values]
    row = {name: statistics.median(cell["latency"][name] for cell in cells)
           for name in ("p50_ms", "p95_ms", "p99_ms")}
    for name in ("requested_keys", "logical_warm_hits", "read_operations", "temporary_bytes_peak"):
        row[name] = statistics.median(statistics.mean(sample["storage_read_receipt"][name]
            for sample in cell["samples"]) for cell in cells)
    row["read_bytes"] = statistics.median(statistics.mean(sum(sample["storage_read_receipt"][name]
        for name in ("legacy_replay_bytes", "footer_bytes", "payload_bytes", "format_probe_bytes"))
        for sample in cell["samples"]) for cell in cells)
    return row


def condition_summary(grouped, mode):
    lines = ["", "### Keyed conditions: " + mode, "",
        "All values and per-field atom, molecule, writer, and source metadata match the same-home reference.",
        "The primary-only pass A reference uses the existing secondary reader. Conditions verify every generated condition key."]
    labels = [cell["label"] for cell in grouped["baseline"][0]["conditions"]["cells"] if cell["phase"] == "warm"]
    for phase in ("warm", "fresh-process"):
        count = 32 if phase == "warm" else 1
        timing = "p50 / p95 / p99 ms" if phase == "warm" else "elapsed ms"
        lines.extend(["", "Condition phase: " + phase + ". Counts and bytes are per request.", "",
            "| Case | Requests/run | Base " + timing + " | Candidate " + timing + " | Change | Key requests base/candidate | Warm hits base/candidate | Read calls base/candidate | Read bytes base/candidate | Transient peak bytes base/candidate |",
            "|---|---:|---|---|---:|---:|---:|---:|---:|---:|"])
        for label in labels:
            rows = {role: summarize(values, phase, label) for role, values in grouped.items()}
            statistic = "p95_ms" if phase == "warm" else "p50_ms"
            change = (rows["candidate"][statistic] / rows["baseline"][statistic] - 1) * 100
            names = ("p50_ms", "p95_ms", "p99_ms") if phase == "warm" else ("p50_ms",)
            elapsed = [" / ".join(f"{rows[role][name]:.3f}" for name in names) for role in ("baseline", "candidate")]
            costs = [" / ".join(f"{rows[role][name]:.1f}" for role in ("baseline", "candidate"))
                     for name in ("requested_keys", "logical_warm_hits", "read_operations", "read_bytes", "temporary_bytes_peak")]
            lines.append("| " + label + " | " + str(count) + " | " + " | ".join(elapsed)
                         + f" | {change:+.1f}% | " + " | ".join(costs) + " |")
            if change > 20:
                print("::notice::" + mode + " " + phase + " " + label + " condition time increased more than20%; advisory only")
    lines.extend(memory_summary(grouped))
    lines.extend(["", "Each warm cell has32 requests per run. Each fresh-process cell has one request after its own restart, eight restarts per run.",
        "Cells use medians of two per-run results. Fresh-process elapsed times do not establish a latency distribution or statistical significance.",
        "Internal key requests are logical storage requests, not the native guard's examined-key count. Read bytes include OS-cache reads.",
        "Transient byte receipts omit returned values, captured field maps, warm caches, and allocator overhead. Process snapshots do not prove an in-flight peak.",
        "The API offset/limit control also matches. Public Delete/Purge removes records; legacy body tombstones and concurrent mutations remain outside this recipe."])
    return lines


def memory_summary(grouped):
    lines = ["", "Condition process memory ranges across both runs and all eight cases, in MiB:", "",
        "| Phase/source | Before | After | Process lifetime peak |", "|---|---:|---:|---:|"]
    for phase in ("warm", "fresh-process"):
        for role, values in grouped.items():
            cells = [cell for value in values for cell in value["conditions"]["cells"] if cell["phase"] == phase]
            columns = []
            for when, name in (("before", "VmRSS"), ("after", "VmRSS"), ("after", "VmHWM")):
                sizes = [cell["memory_" + when]["process_memory_bytes"][name] / 1024 ** 2 for cell in cells]
                columns.append(f"{min(sizes):.2f}–{max(sizes):.2f}")
            lines.append("| " + phase + " / " + role + " | " + " | ".join(columns) + " |")
    return lines

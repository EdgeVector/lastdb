"""Print all matched condition measurements without pooling independent runs."""

import statistics

from ranges import latency


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
        lines.extend(phase_summary(grouped, phase, labels))
    lines.extend(memory_summary(grouped))
    lines.extend(["", "Each warm cell has32 requests per run. Each fresh-process cell has one request after its own restart, eight restarts per run.",
        "Cells use medians of two per-run results. Fresh-process elapsed times do not establish a latency distribution or statistical significance.",
        "Internal key requests are logical storage requests, not the native guard's examined-key count. Read bytes include OS-cache reads.",
        "Transient byte receipts omit returned values, captured field maps, warm caches, and allocator overhead. Process snapshots do not prove an in-flight peak.",
        "The API offset/limit and four duplicate-projection controls also match, with include_tombstones off/on for ordinary and conditional reads.",
        "Every condition row retains its exact four-key envelope and both key coordinates. Its author is null or a projected metadata writer.",
        "Nested bodies retain signed/unsigned integer boundaries, an integer above2^53, float types, and negative zero.",
        "Only the fixed128-key hash uses nested JSON bodies below9KiB. Original large-range bodies and eight timed selectors stay unchanged.",
        "Public Delete/Purge removes records; legacy body tombstones and concurrent mutations remain outside this recipe."])
    return lines


def phase_summary(grouped, phase, labels):
    names = ("p50_ms", "p95_ms", "p99_ms") if phase == "warm" else ("p50_ms",)
    lines = ["", "Existing server phases: p50 / p95 / p99 ms" if phase == "warm"
             else "Existing server phases: elapsed ms", "",
        "| Case | Exact ID coverage/run | Server base/candidate | Core query base/candidate | Format base/candidate |",
        "|---|---|---|---|---|"]
    count = 32 if phase == "warm" else 1
    for label in labels:
        columns = []
        for name in ("duration_ms", "hydrate_atoms_us", "hydrate_format_us"):
            roles = []
            for role in ("baseline", "candidate"):
                values = grouped[role]
                rows = []
                for value in values:
                    cell = next(cell for cell in value["conditions"]["cells"]
                                if (cell["phase"], cell["label"]) == (phase, label))
                    samples = []
                    for sample in cell["samples"]:
                        operation = sample["server_request_ops"]
                        fields = operation if name == "duration_ms" else operation["phases"]
                        if name in fields:
                            samples.append({"elapsed_ms": fields[name] / (1 if name == "duration_ms" else 1000)})
                    if len(samples) == len(cell["samples"]):
                        rows.append(latency(samples))
                roles.append(" / ".join(f"{statistics.median(row[field] for row in rows):.3f}"
                    for field in names) if len(rows) == len(values) else "unreported")
            columns.append(" ; ".join(roles))
        lines.append("| " + label + f" | {count}/{count} | " + " | ".join(columns) + " |")
    lines.extend(["", "Exact IDs match the current request ring after each cell, outside its latency and memory windows.",
        "Core query is hydrate_atoms; format is hydrate_format. Missing phase fields remain unreported, not zero.",
        "Server duration excludes socket queue wait. Phase totals are separate observations and must not be added to a parent phase."])
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

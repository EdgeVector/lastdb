"""Human-readable isolated write stages and native storage checkpoint changes."""

MIB = 1024 ** 2
FAMILIES = ("request", "scope", "commit", "merge_main", "merge_cas")
COLLECTION_FIELDS = ("live_keys", "live_key_bytes", "live_stored_value_bytes", "deleted_keys", "deleted_key_bytes")


def value(number, divisor=1):
    return "unreported" if number is None else f"{number / divisor:.3f}"


def metric(family, field):
    return family.get("scalars", {}).get(field, {})


def timing_lines(report):
    lines = ["", "### Numeric timing captures", "",
             "Request, native scope, physical commit, and background merge captures are separate layers.",
             "Concurrent durations overlap. Nested stages overlap their parents. Do not add stage or layer totals.",
             "Background merge events are observed in a phase window; they are not assigned to particular writes.",
             "Request total excludes socket queue wait. Scope durable wait overlaps physical commits.",
             "Request JSON omits zero phase fields. Request stage means and percentiles use only events with the field present.",
             "Histogram percentiles are upper bounds from approximately 10% bins.", "",
             "| Interval / layer | Events | Expected HTTP writes | Mean capture ms | Covered total ms | Unassigned total ms | Crossing events |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for window in report["intervals"]:
        for name in FAMILIES:
            family = window["families"].get(name, {"events": 0})
            if not family["events"] and not window["crossing"].get(name):
                continue
            expected = window["expected_requests"] if name == "request" else None
            lines.append(f"| {window['name']} / {name} | {family['events']} | {expected if expected is not None else 'unreported'} | "
                         f"{value(metric(family, 'total_us').get('mean'), 1000)} | "
                         f"{value(metric(family, 'covered_us').get('total'), 1000)} | "
                         f"{value(metric(family, 'unassigned_us').get('total'), 1000)} | {window['crossing'].get(name, 0)} |")
    lines += ["", f"Crossing events, each counted once globally: `{report['crossing_events']}`.",
              f"Events outside every supplied interval: `{report['unassigned_events']}`.",
              "A crossing event enters no contained-duration total. Per-window crossing counts can refer to the same event."]
    for window in report["intervals"]:
        for name, family in window["families"].items():
            lines += ["", f"#### {window['name']} / {name}: every reported stage", "",
                      "| Stage | Reported / events | Total ms | Mean ms per reported event | p50 ms | p95 ms | Max ms |",
                      "|---|---:|---:|---:|---:|---:|---:|"]
            for stage, item in sorted(family["phases_us"].items(), key=lambda row: row[1]["total"], reverse=True):
                lines.append(f"| {stage} | {item['reported']}/{family['events']} | {value(item['total'], 1000)} | "
                             f"{value(item['mean'], 1000)} | {value(item.get('p50'), 1000)} | "
                             f"{value(item.get('p95'), 1000)} | {value(item['max'], 1000)} |")
            lines += ["", "| Emitted numeric field (units in name) | Reported / events | Total | Mean per reported event |", "|---|---:|---:|---:|"]
            for count, item in sorted(family["counts"].items()):
                if item["total"]:
                    lines.append(f"| {count} | {item['reported']}/{family['events']} | {item['total']} | {value(item['mean'])} |")
            for count, item in sorted(family["scalars"].items()):
                if not count.endswith("_us"):
                    lines.append(f"| emitted {count} | {item['reported']}/{family['events']} | {item['total']} | {value(item['mean'])} |")
            lines.append("Zero counters remain explicit in diagnostics.json. Absent fields are unreported, not measured zero.")
            lines.append(f"Event classifications: `{family['labels']}`.")
    return lines


def storage_lines(report):
    lines = ["", "### Physical native files at checkpoints", "",
             "File lengths and allocated blocks are separate. The two homes capture their roots independently.",
             "Allocated blocks can be shared outside these homes and do not measure device traffic.", "",
             "| Checkpoint / home | Published MiB | Retired held MiB | Temporary MiB | Unreferenced MiB | Other MiB | Total length MiB | Allocated MiB | Published runs |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for checkpoint in report["checkpoints"]:
        for name, home in checkpoint["audit"].items():
            files = home.get("files", {})
            cells = [value(files.get(f"{kind}_file_bytes"), MIB)
                     for kind in ("published", "retired_held", "temporary", "unreferenced", "other_home")]
            cells += [value(files.get("total_file_bytes"), MIB), value(files.get("total_allocated_bytes"), MIB),
                      str(files.get("published_runs", "unreported"))]
            lines.append(f"| {checkpoint['name']} / {name} | " + " | ".join(cells) + " |")
    lines += ["", "### Selected records and newest-key inventory", "",
              "Stored-value and key bytes precede block compression. Stored values include their tag, not expanded JSON.",
              "Published record bytes include collection prefixes and old versions. Inventory key bytes exclude collection prefixes.",
              "These byte measures are not subtracted from compressed file bytes or from each other.", "",
              "| Checkpoint / home | Generation / frontier | Selected records | Published record MiB | Published tombstones | Counted tier stale MiB | Newest live keys | Newest deleted keys | Inventory |",
              "|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for checkpoint in report["checkpoints"]:
        for name, home in checkpoint["audit"].items():
            inventory = home.get("inventory") or {}
            collections = inventory.get("collections", {})
            parts = [home.get("base", {}), home.get("merging", {}), *home.get("levels", [])]
            live = sum(row.get("live_keys", 0) for row in collections.values())
            deleted = sum(row.get("deleted_keys", 0) for row in collections.values())
            state = "complete" if inventory.get("complete") else f"partial: {inventory.get('stopped_reason', 'unreported')}"
            lines.append(f"| {checkpoint['name']} / {name} | {home.get('generation')} / {home.get('frontier')} | "
                         f"{sum(p.get('records', 0) for p in parts)} | {value(home.get('published_record_bytes'), MIB)} | "
                         f"{home.get('published_tombstones', 'unreported')} | {value(home.get('counted_tier_stale_bytes'), MIB)} | "
                         f"{live} | {deleted} | {state} |")
    return lines


def home_file_lines(report):
    lines = ["", "### Whole generated home files", "",
             "The native audit and whole-home directory walk run sequentially within each timed checkpoint.",
             "These observations are not one atomic snapshot; background merges can change files between them.",
             "Family totals count directory entries. Global unique-inode totals remove hardlink duplicates observed within this home.",
             "Unique-inode allocation does not measure unique filesystem blocks, links outside the home, or device traffic.",
             "File lengths remain separate from encrypted native files and precompression logical-record bytes. No logical/native subtraction is performed.", "",
             "| Checkpoint | Span ms | File entries | Entry length MiB | Entry allocated MiB | Unique-inode length MiB | Unique-inode allocated MiB | Hardlink duplicate entries |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for checkpoint in report["checkpoints"]:
        home = checkpoint.get("home_files")
        if home is None:
            lines.append(f"| {checkpoint['name']} | {value(checkpoint['ended_us'] - checkpoint['started_us'], 1000)} | unreported | unreported | unreported | unreported | unreported | unreported |")
            continue
        families = home.get("families", {})
        count = sum(row.get("files", 0) for row in families.values())
        cells = [str(count)] + [value(home.get(field), MIB) for field in
                ("logical_bytes", "allocated_bytes", "unique_file_logical_bytes", "unique_file_allocated_bytes")]
        cells.append(str(home.get("hardlink_duplicate_entries", "unreported")))
        lines.append(f"| {checkpoint['name']} | {value(checkpoint['ended_us'] - checkpoint['started_us'], 1000)} | " + " | ".join(cells) + " |")
    lines += ["", "| Checkpoint / file family | File entries | Entry length MiB | Entry allocated MiB |",
              "|---|---:|---:|---:|"]
    for checkpoint in report["checkpoints"]:
        home = checkpoint.get("home_files")
        if home is None:
            lines.append(f"| {checkpoint['name']} / unreported | unreported | unreported | unreported |")
            continue
        for name, row in sorted(home.get("families", {}).items(), key=lambda item: item[1].get("logical_bytes", 0), reverse=True):
            lines.append(f"| {checkpoint['name']} / {name} | {row.get('files', 'unreported')} | "
                         f"{value(row.get('logical_bytes'), MIB)} | {value(row.get('allocated_bytes'), MIB)} |")
    lines.append("A hardlinked file can appear in more than one family. Per-family unique-inode ownership is not inferred.")
    return lines


def collection_lines(report):
    lines = ["", "### Newest-key collection changes", "",
             "Ranked by change in tagged stored-value bytes, then key bytes. Labels are fixed internal names; other names share other.",
             "Only consecutive complete inventories enter these deltas. They describe logical stored forms, not compressed disk allocation."]
    for before, after in zip(report["checkpoints"], report["checkpoints"][1:]):
        for name in ("main", "cas"):
            old = before["audit"].get(name, {}).get("inventory") or {}
            new = after["audit"].get(name, {}).get("inventory") or {}
            if not old.get("complete") or not new.get("complete"):
                lines += ["", f"{before['name']} → {after['name']} / {name}: collection growth is unreported (partial inventory)."]
                continue
            rows = []
            for collection in old["collections"].keys() | new["collections"].keys():
                first, last = old["collections"].get(collection, {}), new["collections"].get(collection, {})
                delta = {field: last.get(field, 0) - first.get(field, 0) for field in COLLECTION_FIELDS}
                rows.append((collection, delta))
            rows.sort(key=lambda row: (row[1]["live_stored_value_bytes"], row[1]["live_key_bytes"]), reverse=True)
            lines += ["", f"#### {before['name']} → {after['name']} / {name}", "",
                      "| Internal collection | Live key change | Key MiB change | Stored-value MiB change | Deleted key change | Deleted-key MiB change |",
                      "|---|---:|---:|---:|---:|---:|"]
            for collection, delta in rows:
                lines.append(f"| {collection} | {delta['live_keys']:+d} | {delta['live_key_bytes'] / MIB:+.3f} | "
                             f"{delta['live_stored_value_bytes'] / MIB:+.3f} | {delta['deleted_keys']:+d} | "
                             f"{delta['deleted_key_bytes'] / MIB:+.3f} |")
    return lines


def render(report):
    lines = ["## Isolated write stages and disk growth", "",
             f"Capture complete: **{report['complete']}**. Numeric events: {report['events']}. Log lines read: {report['log_lines']}.",
             "Diagnostics add CPU, file reads, and log writes. These times are not an uninstrumented capacity result."]
    if report["errors"]:
        lines += ["", "Capture errors:"] + [f"- {error}" for error in report["errors"]]
    for region in report.get("log_exclusions", []):
        lines += ["", f"Intentional log exclusion `{region['name']}`: byte offsets [{region['start_offset']}, {region['end_offset']}).",
                  f"Excluded region bytes: {region['excluded_bytes']}; overlapping lines: {region['excluded_lines']}; complete line bytes skipped: {region['excluded_line_bytes']}.",
                  "Only this explicit crash region and its boundary lines leave numeric capture. The raw log remains in the artifact.",
                  "The recovery canary and reopened inventory remain separate evidence. Load request coverage stays strict."]
    lines += timing_lines(report) + home_file_lines(report) + storage_lines(report) + collection_lines(report)
    return "\n".join(lines) + "\n"

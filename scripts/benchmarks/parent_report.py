"""Reconcile complete incremental-parent captures with exact integer totals."""

KINDS = ("locator", "directory")
MODES = ("tracked", "untracked")
EDITS = ("edit_started", "edit_completed", "edit_keys", "root_leaf_visits", "root_parent_visits",
         "root_leaf_promotions", "root_parent_to_leaf", "parent_input_pages", "parent_rewrite_inputs",
         "parent_split_extra_pages", "parent_added_level_pages", "parent_output_pages", "parent_added_level_calls")
BANDS = ("children_le2", "children_le4", "children_le8", "children_le16", "children_le32", "children_gt32")
INPUTS = ("pages", "payload_bytes", "children", "same_generation_pages", "older_generation_pages", *BANDS)
ORIGINS = ("from_root_input_output_pages", "from_nonroot_input_output_pages", "added_level_output_pages")
VALUES = ("inserted", "changed", "identical")
CONTROLS = ("snapshot_parent_usage_version", "parent_invalid_inputs", "parent_invalid_outputs",
            "publisher_batch_calls", "locator_template_create_calls", "locator_bulk_create_calls")
NAMES = (*[f"{kind}_{mode}_{name}" for kind in KINDS for mode in MODES for name in EDITS],
         *[f"{kind}_parent_{role}_input_{name}" for kind in KINDS for role in ("root", "nonroot") for name in INPUTS],
         *[f"{kind}_parent_input_level_{level}_pages" for kind in KINDS for level in range(1, 8)],
         *[f"{kind}_parent_{name}" for kind in KINDS for name in ORIGINS],
         *[f"{kind}_edit_values_{name}" for kind in KINDS for name in VALUES], *CONTROLS)


def capture(case):
    counts, n = case.get("physical_counts", {}), case["distinct_physical_commits"]
    if not any(name in counts for name in NAMES):
        return None
    if n <= 0:
        raise ValueError("Parent counters lack physical commits")
    totals = {}
    for name in (*NAMES, "snapshot_touched_groups", "locator_parent_output_pages", "directory_parent_output_pages"):
        item = counts.get(name, {})
        value = item.get("total")
        if item.get("reported") != n or type(value) is not int or value < 0:
            raise ValueError(f"Incomplete integer parent counter: {name}")
        totals[name] = value
    if totals["snapshot_parent_usage_version"] != n or totals["parent_invalid_inputs"] or totals["parent_invalid_outputs"]:
        raise ValueError("Invalid parent capture version or page shape")
    for kind in KINDS:
        modes = [{name: totals[f"{kind}_{mode}_{name}"] for name in EDITS} for mode in MODES]
        for m in modes:
            if m["edit_started"] != m["edit_completed"] or m["edit_started"] != m["root_leaf_visits"] + m["root_parent_visits"]:
                raise ValueError(f"{kind}: incomplete edit/root coverage")
            if m["parent_input_pages"] != m["parent_rewrite_inputs"] or m["parent_output_pages"] != (
                    m["parent_input_pages"] + m["parent_split_extra_pages"] + m["parent_added_level_pages"]):
                raise ValueError(f"{kind}: input + split extras + added levels differs from output")
            if (m["root_leaf_promotions"] > m["root_leaf_visits"] or m["root_parent_to_leaf"] != 0
                    or m["parent_added_level_calls"] > m["parent_added_level_pages"]):
                raise ValueError(f"{kind}: invalid root transition or added-level counts")
        inputs = []
        for role in ("root", "nonroot"):
            p = {name: totals[f"{kind}_parent_{role}_input_{name}"] for name in INPUTS}
            if p["same_generation_pages"] + p["older_generation_pages"] != p["pages"] or sum(p[b] for b in BANDS) != p["pages"]:
                raise ValueError(f"{kind}/{role}: generation or child bands differ from input pages")
            if p["children"] < p["pages"] * 2 or p["payload_bytes"] > p["pages"] * 16256:
                raise ValueError(f"{kind}/{role}: invalid child count or payload bytes")
            inputs.append(p["pages"])
        if inputs[0] != sum(m["root_parent_visits"] for m in modes) or sum(inputs) != sum(m["parent_input_pages"] for m in modes):
            raise ValueError(f"{kind}: root/nonroot inputs differ from edit-mode totals")
        if sum(totals[f"{kind}_parent_input_level_{level}_pages"] for level in range(1, 8)) != sum(inputs):
            raise ValueError(f"{kind}: input levels differ from input pages")
        origins = [totals[f"{kind}_parent_{name}"] for name in ORIGINS]
        output = sum(m["parent_output_pages"] for m in modes)
        if (sum(origins) != output or origins[2] != sum(m["parent_added_level_pages"] for m in modes)
                or any(a < b for a, b in zip(origins[:2], inputs))):
            raise ValueError(f"{kind}: output-origin totals differ from edit output")
        if totals[f"{kind}_parent_output_pages"] < output:
            raise ValueError(f"{kind}: incremental output exceeds all parent-page writes")
        if sum(totals[f"{kind}_edit_values_{name}"] for name in VALUES) != sum(m["edit_keys"] for m in modes):
            raise ValueError(f"{kind}: inserted + changed + identical differs from explicit edit keys")
    return totals


def render(rows):
    checked = [(label, name, case["distinct_physical_commits"], capture(case)) for label, name, case in rows]
    shown = [r for r in checked if r[1] in ("insert/1", "update/1", "update/128")]
    lines = ["", "## Incremental parent reconstruction", "",
             "The tables focus on insert/1, update/1, and update/128. Counter validation covers all selected native cases.",
             "Numbers are exact accepted-commit totals divided by distinct physical commits. Missing historical captures show unreported.",
             "Each complete mode satisfies P = V + S + A: output parents = input parents + split extras + added-level pages.",
             "Tracked selects lifetime-reference accounting. It does not identify foreground work or compaction.",
             "These counters describe visited inputs and emitted output. They do not count every live parent page.", "",
             "| Run / case / kind / mode | Edits | Keys | Root leaf visits | Root parent visits | Promotions | Parent-to-leaf | Input V | Split S | Added A | Output P |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]

    def number(t, n, key):
        return "unreported" if t is None else f"{t[key] / n:.3f}"

    def ratio(a, b, scale=1):
        return f"{a / b * scale:.3f}" if b else "n/a"

    for label, _, n, t in shown:
        for kind in KINDS:
            for mode in MODES:
                keys = ("edit_completed", "edit_keys", "root_leaf_visits", "root_parent_visits", "root_leaf_promotions",
                        "root_parent_to_leaf", "parent_input_pages", "parent_split_extra_pages", "parent_added_level_pages", "parent_output_pages")
                lines.append(f"| {label} / {kind} / {mode} | " + " | ".join(number(t, n, f"{kind}_{mode}_{k}") for k in keys) + " |")
    lines.extend(["", "| Run / case / input | Pages | Same generation | Older generation | Children/page | Payload fullness % | Child bands ≤2 / (2,4] / (4,8] / (8,16] / (16,32] / >32 |",
                  "|---|---:|---:|---:|---:|---:|---|"])
    for label, _, n, t in shown:
        for kind in KINDS:
            for role in ("root", "nonroot"):
                p = f"{kind}_parent_{role}_input_"
                cells = [number(t, n, p + k) for k in ("pages", "same_generation_pages", "older_generation_pages")]
                cells += ["unreported", "unreported"] if t is None else [ratio(t[p + "children"], t[p + "pages"]), ratio(t[p + "payload_bytes"], t[p + "pages"] * 16256, 100)]
                cells.append(" / ".join(number(t, n, p + b) for b in BANDS))
                lines.append(f"| {label} / {kind} / {role} | " + " | ".join(cells) + " |")
    lines.extend(["", "| Run / case / kind | Input levels 1 / 2 / 3 / 4 / 5 / 6 / 7 | From root input | From nonroot input | Added levels | All parent writes | Other/bulk output | Inserted / changed / identical keys |",
                  "|---|---|---:|---:|---:|---:|---:|---|"])
    for label, _, n, t in shown:
        for kind in KINDS:
            cells = [" / ".join(number(t, n, f"{kind}_parent_input_level_{level}_pages") for level in range(1, 8))]
            cells += [number(t, n, f"{kind}_parent_{k}") for k in ORIGINS]
            cells.append(number(t, n, f"{kind}_parent_output_pages"))
            cells.append("unreported" if t is None else f"{(t[f'{kind}_parent_output_pages'] - sum(t[f'{kind}_{mode}_parent_output_pages'] for mode in MODES)) / n:.3f}")
            cells.append(" / ".join(number(t, n, f"{kind}_edit_values_{k}") for k in VALUES))
            lines.append(f"| {label} / {kind} | " + " | ".join(cells) + " |")
    lines.extend(["", "| Run / case | Publisher batches | Touched hash groups | Locator tracked edits | Untracked edits | Template creates | Bulk creates | Group join residual | Incremental locator parents/touched hash group |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for label, _, n, t in shown:
        keys = ("publisher_batch_calls", "snapshot_touched_groups", "locator_tracked_edit_completed",
                "locator_untracked_edit_completed", "locator_template_create_calls", "locator_bulk_create_calls")
        cells = [number(t, n, k) for k in keys]
        if t is None:
            cells += ["unreported", "unreported"]
        else:
            residual = t["snapshot_touched_groups"] - sum(t[k] for k in (keys[2], keys[4], keys[5]))
            cells += [f"{residual / n:.3f}", ratio(sum(t[f"locator_{m}_parent_output_pages"] for m in MODES), t["snapshot_touched_groups"])]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines.extend(["", "Input-root output descends from an old root; it can split and does not identify final roots.",
                  "Level 1 is a parent above leaves. Child bands are disjoint. Generation equality does not identify private or published pages.",
                  "Other/bulk output is all parent-page writes minus tracked and untracked incremental output.",
                  "The group join subtracts tracked locator edits, template creates, and bulk creates from touched hash groups.",
                  "It is meaningful only for isolated commits with one Publisher batch and no other locator edit calls.",
                  "Case totals alone do not establish that per-commit condition. The raw artifact retains each commit for review.",
                  "A low payload fullness or a same-generation visit alone does not establish redundant copies."])
    return lines

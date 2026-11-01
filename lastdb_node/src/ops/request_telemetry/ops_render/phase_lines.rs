use super::*;

/// The "Request phases" table shared by the live snapshot and the durable
/// rollup: for each (client, kind, schema) key whose requests reported a
/// per-phase breakdown, where inside the node that key's time went.
///
/// Zero-hide on both axes. As with the body-volume table, the table is ABSENT
/// when no key reports phases. Within a row, zero phases are also omitted
/// ([`PhaseTimings::detail`]). Flush in particular is ~0 by design
/// on default builds (background flusher), so a printed `flush=0us` would
/// read as broken instrumentation rather than a measurement.
///
/// The PHASED column uses `<reported>/<total>` as its honest denominator.
/// Phase coverage is partial while only the mutation path reports, so a
/// per-request average over `count` would dilute.
pub(crate) fn phase_table_lines(aggregates: &[OpAggregate], labels: &SchemaLabels) -> Vec<String> {
    let mut phased: Vec<&OpAggregate> = aggregates
        .iter()
        .filter(|a| !a.phase_sums.is_empty())
        .collect();
    if phased.is_empty() {
        return Vec::new();
    }
    // `disjoint_us`, not `total_us`: the latter sums the `hydrate` parent
    // beside the four children that partition it, which ranked every query
    // row at ~2x its real phase cost and every mutation row honestly. That
    // put query keys above mutation keys this command's own `Top by total
    // time` table placed below them.
    phased.sort_by(|a, b| {
        b.phase_sums
            .disjoint_us()
            .cmp(&a.phase_sums.disjoint_us())
            .then_with(|| b.phase_count.cmp(&a.phase_count))
            .then_with(|| a.client.cmp(&b.client))
    });
    let selected: Vec<&OpAggregate> = phased.into_iter().take(10).collect();
    let rows: Vec<Vec<String>> = selected
        .iter()
        .enumerate()
        .map(|(i, a)| {
            // Closes the accounting by construction: every phased request's
            // wall clock is inside a named phase or this remainder.
            let residual = a.unphased_us().map_or_else(
                || "-".to_string(),
                |us| error_cell(&residual_detail(us, a.phased_sum_ms.saturating_mul(1_000))),
            );
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                format!("{}/{}", human_count(a.phase_count), human_count(a.count)),
                if a.phased_sum_ms == 0 {
                    "-".to_string()
                } else {
                    human_duration_ms(a.phased_sum_ms)
                },
                residual,
            ]
        })
        .collect();
    // The parent note is not decoration: `hydrate` renders immediately beside
    // the four children that partition it and looks like one more sibling, so
    // a reader adding the printed phases up by hand reproduces exactly the
    // double-count the ranking key was fixed to drop.
    let mut lines = vec![
        String::new(),
        "Request phases — per-key sums; zero phases omitted; ranked by phase time counted once"
            .to_string(),
        "  hydrate is the PARENT of the hydrate_* phases — do not add it to them".to_string(),
    ];
    lines.extend(aligned_table(
        &[
            "#",
            "CLIENT",
            "KIND",
            "SCHEMA / ROUTE",
            "PHASED",
            "WALL",
            "REMAINDER",
        ],
        &rows,
        &[true, false, false, false, true, true, true],
    ));
    lines.push("  Phase details:".to_string());
    for (i, a) in selected.iter().enumerate() {
        // `sum_molecules_persisted` and `sum_molecule_store_commits` stay in
        // the JSON and the rollup rows for their readers. The record model
        // keeps both at zero, so the text output leaves them out.
        let mut tokens = phase_tokens(a.phase_sums);
        let resident = resident_commit_detail(a.sum_resident_operations, a.sum_resident_commits);
        if !resident.is_empty() {
            tokens.push(resident.trim().to_string());
        }
        lines.extend(wrap_detail(&format!("    {}. ", i + 1), tokens));
    }
    lines
}

/// `resident=<operations>/<commits>` — the ratio the one-resident-commit
/// design is defined by.
///
/// The numerator is work asked for, the denominator is commits issued. `9/1`
/// is one atomic batch, `9/9` is the serial path it replaces.
pub(crate) fn resident_commit_detail(operations: u64, commits: u64) -> String {
    if operations == 0 && commits == 0 {
        return String::new();
    }
    // A commit count of zero with operations counted cannot happen from the
    // recording sites, but rendering `inf` on a truncated aggregate is worse
    // than rendering the raw pair.
    if commits == 0 {
        return format!(" resident={operations}/0");
    }
    let per_commit = operations as f64 / commits as f64;
    format!(" resident={operations}/{commits} ({per_commit:.2} per commit)")
}

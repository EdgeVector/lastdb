use super::*;

/// One line for `lastdb status` — but only when something was actually
/// dropped. A healthy node should not carry a permanent "0 rows lost" line
/// that operators learn to skip past; the whole value of this signal is that
/// its presence means something.
/// Always emits a line so "never probed" is distinguishable from "zero locator-only".
pub(super) fn locator_only_status_line(probe: Option<&LocatorOnlyHealth>) -> String {
    match probe {
        None => format!(
            "Locator-only: never probed this process — run \
             `lastdb db probe-locator-only` (default {} tips across {} positions; \
             always read-only). Do not infer population from silence.",
            fold_db::db_operations::DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS,
            fold_db::db_operations::LOCATOR_ONLY_PROBE_STRATA,
        ),
        Some(p) => {
            let rate = match p.locator_only_per_mille.as_wire_u64() {
                Some(r) => format!("; {r}‰ of sample"),
                None => String::new(),
            };
            // Say how many independent POSITIONS the sample had, not just how
            // many windows were partly read. Windows whose quota binds are head
            // reads of their own window, so the count of them is what bounds
            // what this gauge can see — 17 windows could not see a class
            // occupying a sixth of `mk:`, and said `0`.
            let complete = if p.completed {
                "full walk".to_string()
            } else {
                format!(
                    "stratified sample, {} of {} windows hit max_tips",
                    gauge_or_zero(&p.strata).saturating_sub(gauge_or_zero(&p.strata_exhausted)),
                    gauge_or_zero(&p.strata),
                )
            };
            // The floor is what makes a `0` readable. Without it, "no damage"
            // and "no damage this sample could have seen" render identically,
            // and the second one shipped for a month reading as the first.
            let floor = match p.detection_floor_per_mille.as_wire_u64() {
                Some(f) if !p.completed => format!(
                    "; sees a contiguous class down to {f}‰ — a narrower one can                      hide, raise --max-tips"
                ),
                _ => String::new(),
            };
            // The dangling class rides the SAME sample, so it costs nothing
            // extra to report and is the only number here that means damage.
            // `Read integrity` counts rows a query happened to touch, so it is
            // bounded by read traffic; this one is bounded by the store AND by
            // the sample's own resolution, which is why the floor is printed
            // beside it rather than left for the reader to assume away.
            let dangling = match p.dangling.as_wire_u64() {
                None => String::new(),
                Some(d) => {
                    let drate = match p.dangling_per_mille.as_wire_u64() {
                        Some(r) => format!(", {r}‰"),
                        None => String::new(),
                    };
                    format!(" dangling={d}{drate} (no reader route reaches the body);")
                }
            };
            // A level cannot answer "did the last repair hold". Two levels can,
            // so say what the previous probe found and how fast the gap between
            // them opened. Silence here means one probe this process, not calm.
            let recurrence = match p.prev_dangling.as_wire_u64() {
                None => String::new(),
                Some(prev) => {
                    let now = gauge_or_zero(&p.dangling);
                    let direction = if now > prev {
                        match p.dangling_recurrence_per_hour.as_wire_u64() {
                            Some(rate) => format!(", +{rate}/h"),
                            // Withheld on purpose: the two samples used
                            // different budgets, so their difference is not a
                            // statement about the store.
                            None => ", rate withheld (budget changed)".to_string(),
                        }
                    } else {
                        String::new()
                    };
                    format!(
                        " recurrence: dangling was {prev} at {}{direction};",
                        p.prev_probed_at_unix,
                    )
                }
            };
            format!(
                "Locator-only: {} of {} tips ({complete}{rate}{floor}; \
                 body_ok={}{dangling}{recurrence} other_unresolved={} probed_at={}). \
                 Body reachable only via aloc: locator — not tip-derived/flat. \
                 Dual-write residual is separate (rekey would_dual_write).",
                gauge_or_zero(&p.locator_only),
                gauge_or_zero(&p.tips_sampled),
                gauge_or_zero(&p.body_at_derived_or_flat),
                gauge_or_zero(&p.other_unresolved),
                p.probed_at_unix,
            )
        }
    }
}

pub(super) fn integrity_status_line(i: &IntegrityHealth) -> Option<String> {
    let skips = match i.unresolved_atom_skips.value {
        Availability::Measured(0) | Availability::Unavailable(_) => return None,
        Availability::Measured(n) => n,
    };
    // Lead with damage size. Edges are typed Unit::Edges so this cannot print
    // them as row(s) (historical mislabel). Window/noun come from the gauges.
    let floor = if i.unresolved_distinct_capped {
        "+"
    } else {
        ""
    };
    let row_noun = i.unresolved_atom_rows.unit.noun();
    let edge_noun = i.unresolved_atom_distinct.unit.noun();
    let event_noun = i.unresolved_atom_skips.unit.noun();
    let window_q = i.unresolved_atom_skips.window.qualifier();
    let scope = match (
        i.unresolved_atom_rows.as_wire_u64(),
        i.unresolved_atom_distinct.as_wire_u64(),
    ) {
        (Some(rows), Some(edges)) => {
            format!("{rows}{floor} {row_noun} unreadable across {edges}{floor} {edge_noun}")
        }
        (_, Some(edges)) => {
            format!("{edges}{floor} {edge_noun} (row count unavailable from this daemon)")
        }
        _ => "damage scope unavailable from this daemon".to_string(),
    };
    // `\` continuations, so the source indentation stays in the source. Without
    // them this rendered as one ~600-character run carrying literal ten-space
    // gaps mid-sentence, in a `status` block of otherwise short labelled lines
    // — read as corrupted output, and missed entirely by a reader who did not
    // scroll to line 43. The advice inside it is load-bearing ("size a repair
    // with the dry run, not with these counts"), so it has to be readable.
    Some(format!(
        "Read integrity: DEGRADED — {scope}, {skips} {event_noun} {window_q}, \
         because their tip pointed at a missing atom. Reads on the affected \
         partitions return 200 with fewer rows than the index claims. Rows are \
         how much is broken; events are how often callers were served short. \
         Size a repair with 'lastdb db repair-dangling-tips' (dry run), not \
         with these counts. Keys: grep the daemon log for 'Skipping unresolved \
         atom ref'."
    ))
}

/// One line for `lastdb status`: file blobs the remote CAS proved absent while
/// this node held an "already uploaded" memo for them.
///
/// Silent unless the durability class is non-zero. An ordinary miss
/// (`absent_without_memo`) is not a defect and must not raise a line — a
/// warning that fires for healthy behaviour is one an operator learns to skip,
/// which is how the condition this counts went eleven days without a reader.
///
/// The benign count still appears *inside* the line once it fires, because
/// "3 blobs gone" and "3 blobs gone out of 4,000 fetches that missed for
/// ordinary reasons" call for different urgency.
pub(super) fn file_blob_status_line(f: &FileBlobHealth) -> Option<String> {
    let events = match f.absent_with_memo.value {
        Availability::Measured(0) | Availability::Unavailable(_) => return None,
        Availability::Measured(n) => n,
    };
    let floor = if f.absent_distinct_capped { "+" } else { "" };
    let blob_noun = f.absent_with_memo_distinct.unit.noun();
    let event_noun = f.absent_with_memo.unit.noun();
    let window_q = f.absent_with_memo.window.qualifier();
    // Lead with damage size, as the read-integrity line does: distinct blobs
    // are how much is gone, events are how often a caller was refused.
    let scope = match f.absent_with_memo_distinct.as_wire_u64() {
        Some(blobs) => format!("{blobs}{floor} {blob_noun} unretrievable"),
        None => "damage scope unavailable from this daemon".to_string(),
    };
    let benign = match f.absent_without_memo.as_wire_u64() {
        Some(n) => format!("{n} ordinary miss {event_noun}"),
        None => "ordinary misses unavailable from this daemon".to_string(),
    };
    Some(format!(
        "File blob durability: DEGRADED — {scope}, {events} {event_noun} {window_q}          ({benign}), because a presigned GET proved the object absent while this          node still held an upload memo for it. These are writes this node          accepted and returned a pointer for; the pointer still reads back from          the row, so only a fetch can see it. Blobs are how much is gone; events          are how often a caller was refused. Size a repair from the blobs, not          the events. Keys: grep the daemon log for 'remote CAS has no object'."
    ))
}

/// One line for `lastdb status`: resident graph T0 activity and deferred drain.
///
/// `graph_budget=` is the ResidentGraph byte cap (`LASTDB_RESIDENT_BYTES`).
/// The logical-key memory limit is the `Logical keys:` line.
pub(super) fn resident_status_line(r: &ResidentHealth) -> String {
    let graph_budget = if r.budget_bytes == 0 {
        "unbounded".to_string()
    } else {
        format_bytes(r.budget_bytes)
    };
    // Three readings, not two. `completed=0 total_us=0` is what a daemon that
    // predates these counters produces AND what a node whose acked writes have
    // never reached disk produces, and under `LASTDB_RESIDENT_MODE=write` the
    // second is a durability emergency. Say "unavailable from this daemon"
    // rather than print a zero this process never measured; a live snapshot is
    // always `Some`, so the fallback can only be reached by an older payload.
    // Within `Some`, `completed=` still separates "no samples yet" from
    // "genuinely instant".
    let deferred = match (
        r.deferred_persist_completed.value,
        r.deferred_persist_us.value,
    ) {
        (Availability::Measured(completed), Availability::Measured(total_us)) => format!(
            "deferred_persist completed={completed} total_us={total_us} avg_us={} ({})",
            total_us.checked_div(completed).unwrap_or(0),
            r.deferred_persist_completed.window.qualifier(),
        ),
        _ => "deferred_persist unavailable from this daemon".to_string(),
    };
    // The two non-hit outcomes were one counter until 2026-08-17, and a daemon
    // staged behind the CLI still serves only the total. Rendering the split as
    // 0/0 next to a large total would be a false zero of exactly the kind
    // `deferred_persist completed=0` already taught us to avoid, so say the
    // split is unavailable instead of implying both halves are empty.
    let key_set = if r.key_set_misses > 0 && r.key_set_overlays == 0 && r.key_set_unknowns == 0 {
        format!(
            "key_set hit/miss/demote={}/{}/{} (overlay/unknown split unavailable from this daemon)",
            r.key_set_hits, r.key_set_misses, r.key_set_demotes,
        )
    } else {
        format!(
            "key_set hit/overlay/unknown/demote={}/{}/{}/{} complete={}",
            r.key_set_hits,
            r.key_set_overlays,
            r.key_set_unknowns,
            r.key_set_demotes,
            r.key_set_marked_complete,
        )
    };
    format!(
        "Resident: entries={} bytes={} graph_budget={} hits schema/molecule/atom/blob/protein={}/{}/{}/{}/{} \
         rehydrates={}/{}/{}/{}/{} {key_set} \
         persist enqueued/flushed/failed={}/{}/{} \
         lanes depth/age_ms/fail/lag={}/{}/{}/{} \
         {deferred} evicted={} dirty_refused={}",
        r.resident_entries,
        format_bytes(r.resident_bytes),
        graph_budget,
        r.schema_hits,
        r.molecule_hits,
        r.atom_hits,
        r.file_blob_hits,
        r.protein_hits,
        r.schema_rehydrates,
        r.molecule_rehydrates,
        r.atom_rehydrates,
        r.file_blob_rehydrates,
        r.protein_rehydrates,
        r.persist_enqueued,
        r.persist_flushed,
        r.deferred_persist_failed,
        r.persist_lane_depth,
        r.persist_lane_oldest_age_ms,
        r.persist_lane_failures,
        r.resident_minus_durable_revision,
        r.evicted,
        r.evict_refused_dirty,
    )
}

/// One line for `lastdb status`: dual-read residue still being served.
///
/// `by_design` is reported next to `legacy_hits` rather than folded into it —
/// an operator needs to read "reads fell through, and none of it is debt"
/// straight off the line, without inferring it from a plane rollup.
pub(super) fn dual_read_status_line(d: &DualReadHealth) -> String {
    format!(
        "Dual-read: gets={} target_hits={} legacy_hits={} by_design={}{} misses={}{}",
        gauge_or_zero(&d.gets),
        gauge_or_zero(&d.target_hits),
        gauge_or_zero(&d.legacy_hits),
        gauge_or_zero(&d.by_design_hits),
        dual_read_by_design_suffix(d),
        gauge_or_zero(&d.misses),
        dual_read_served_by_suffix(d),
    )
}

/// `[<collection>]` naming the top by-design plane, so `by_design=790172` is
/// self-explaining instead of prompting the same "what is that?" every time.
pub(super) fn dual_read_by_design_suffix(d: &DualReadHealth) -> String {
    let Some((name, _)) = d
        .by_design_hits_by_collection
        .iter()
        .max_by_key(|(name, hits)| (*hits, std::cmp::Reverse(name.clone())))
    else {
        return String::new();
    };
    format!("[{name}]")
}

/// ` served by: <collection>=<hits> …` — the part that names the residue.
///
/// Empty when nothing fell through, so a clean home keeps the short line.
/// When plane rollups are present, append `planes: role=hits …` so milestone
/// proofs can attribute legacy hits without re-walking disk.
pub(super) fn dual_read_served_by_suffix(d: &DualReadHealth) -> String {
    if d.legacy_hits_by_collection.is_empty() && d.legacy_hits_by_plane.is_empty() {
        return String::new();
    }
    let mut parts = Vec::new();
    if !d.legacy_hits_by_collection.is_empty() {
        let mut by_collection = d.legacy_hits_by_collection.clone();
        by_collection.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let served: Vec<String> = by_collection
            .iter()
            .take(4)
            .map(|(name, hits)| format!("{name}={hits}"))
            .collect();
        parts.push(format!("served by: {}", served.join(" ")));
    }
    if !d.legacy_hits_by_plane.is_empty() {
        let mut by_plane = d.legacy_hits_by_plane.clone();
        by_plane.sort_by(|a, b| {
            gauge_or_zero(&b.legacy_hits)
                .cmp(&gauge_or_zero(&a.legacy_hits))
                .then_with(|| a.role.cmp(&b.role))
        });
        let plane_bits: Vec<String> = by_plane
            .iter()
            .take(4)
            .map(|p| {
                let hits = gauge_or_zero(&p.legacy_hits);
                if p.label.is_empty() || p.label == p.role {
                    format!("{}={}", p.role, hits)
                } else {
                    format!("{}({})={}", p.role, p.label, hits)
                }
            })
            .collect();
        parts.push(format!("planes: {}", plane_bits.join(" ")));
    }
    format!(" {}", parts.join("; "))
}

/// One line for `lastdb status`: what reads cost on this node.
pub(super) fn read_cost_status_line(rc: &ReadCostHealth) -> String {
    let warm_resident = gauge_or_zero(&rc.warm_resident_bytes);
    let warm_budget = gauge_or_zero(&rc.warm_budget_bytes);
    let warm = match rc.warm_fill_percent() {
        Some(pct) => format!(
            "{} / {} ({pct:.1}%)",
            format_bytes(warm_resident),
            format_bytes(warm_budget),
        ),
        // Budget 0 means eviction is off — say so rather than printing a
        // division-by-zero percentage or a bare "0 B" budget that reads as a
        // misconfiguration.
        None => format!("{} (eviction disabled)", format_bytes(warm_resident)),
    };
    // The descriptor budget gets its own clause, with its own numerator. On
    // 2026-07-30 an operator reading this line saw `warm_groups=4915` and a
    // 4,915 cap and concluded the warm set was at its ceiling; the node held 2
    // descriptors and the cap was costing it millions of cold loads. Printing
    // the group count next to a descriptor cap is the whole trap.
    let handle_budget = gauge_or_zero(&rc.warm_budget_handles);
    let fds = match (
        rc.open_append_handles.as_wire_u64(),
        rc.warm_handle_fill_percent(),
    ) {
        (Some(open), Some(pct)) => {
            format!(" fds={open}/{handle_budget} ({pct:.1}%)")
        }
        (Some(open), None) => format!(" fds={open} (cap off)"),
        (None, _) => " fds unavailable from this daemon".to_string(),
    };
    // The id tiers get their own clause because they answer a different
    // question from every other field on this line. The rest describe the warm
    // BODY set; these describe what happens to a keys-only resolution the warm
    // set did not cover, and whether it still avoided a segment read. Without
    // them the warm byte budget was the only cache number the node published,
    // so it was the only knob any read-thrash diagnosis could reach for — and
    // it is the most expensive one, charged 1:1 into the process memory budget
    // and multiplied into the projection against the guard ceiling.
    let key_cache = match rc.key_cache_fill_percent() {
        Some(pct) => format!(
            "{} / {} ({pct:.1}%)",
            format_bytes(gauge_or_zero(&rc.key_cache_bytes)),
            format_bytes(gauge_or_zero(&rc.key_cache_budget_bytes)),
        ),
        None => "disabled".to_string(),
    };
    let id_tiers = match (
        rc.id_tier_live_scans.as_wire_u64(),
        rc.id_tier_hit_percent(),
    ) {
        (Some(_), Some(pct)) => format!(
            " id_tiers: key_cache={} sidecar={} scans={} hit={pct:.1}% key_cache_bytes={key_cache}",
            gauge_or_zero(&rc.id_tier_key_cache_hits),
            gauge_or_zero(&rc.id_tier_sidecar_hits),
            gauge_or_zero(&rc.id_tier_live_scans),
        ),
        // Served, but nothing has missed the warm set yet: print the counters
        // and say the rate is unmeasured rather than printing a 0% that reads
        // as both tiers failing.
        (Some(_), None) => format!(
            " id_tiers: key_cache={} sidecar={} scans={} hit=n/a (nothing missed the warm set) key_cache_bytes={key_cache}",
            gauge_or_zero(&rc.id_tier_key_cache_hits),
            gauge_or_zero(&rc.id_tier_sidecar_hits),
            gauge_or_zero(&rc.id_tier_live_scans),
        ),
        (None, _) => " id_tiers unavailable from this daemon".to_string(),
    };
    format!(
        "Read cost: cold_shard_loads={} warm_groups={} warm={}{} torn_txn_rollbacks={} fail={} txn_refresh_fail={}{}",
        gauge_or_zero(&rc.cold_shard_loads),
        gauge_or_zero(&rc.warm_resident_groups),
        warm,
        fds,
        gauge_or_zero(&rc.torn_transaction_rollbacks),
        gauge_or_zero(&rc.torn_transaction_rollback_failures),
        gauge_or_zero(&rc.transaction_residency_refresh_failures),
        id_tiers,
    )
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass

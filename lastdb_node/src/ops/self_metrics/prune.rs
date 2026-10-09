use super::*;

/// Remove rows from a telemetry series by HARD DELETE, in one batch.
///
/// Two properties, both load-bearing, neither available from the per-row
/// `MutationType::Delete` loop this replaces:
///
/// 1. **`Purge`, not `Delete`.** On this layer `Delete` is repurposed as the
///    *tombstone write* (`mutation_manager/molecules/prepare.rs`): it
///    synthesizes a tombstone atom for **every field in the schema at that
///    key**. At 39 `REQUEST_OPS_ROLLUP_FIELDS`, draining the primary's ~74k
///    orphaned rows that way would write ~2.9M new atoms plus ~2.9M order-log
///    entries and free **zero** tips — the plane grows as it is "pruned". That
///    is why the retention this file used to run could never have reclaimed
///    anything, and why the obvious repair ("just call the prune") was the
///    worst available action. `Purge` hard-deletes atom history, molecule
///    entries and embeddings, which is what reclaim means.
///
/// 2. **One batch, not one submit per row.** `process_purges` groups by schema
///    and runs a single `O(F*R)` pass per group; a per-row submit is `N` such
///    passes. Measured (fold #1204): a drain's cost tracks the **plane's**
///    record count, not how many rows it removes — 16x more records removed
///    cost the same wall time (232/335/256 ms at N=50/200/800 against a fixed
///    R=2000 plane). So the cheapest drain removes everything owed at once, and
///    chunking a drain into C passes costs C full drains.
///
/// A note for a future reader tempted to assert on the row count afterwards:
/// a row count cannot tell these two implementations apart. `Delete` makes the
/// row vanish from every query *while adding atoms*. The oracle for this
/// function is `list_atoms_by_schema`, not `query_*_ids`.
pub(super) async fn purge_series_rows(
    host: &Host,
    schema_name: &str,
    series: &str,
    sample_ids: &[String],
) -> Result<(), String> {
    if sample_ids.is_empty() {
        return Ok(());
    }
    let mutations: Vec<Mutation> = sample_ids
        .iter()
        .map(|sample_id| {
            Mutation::new(
                schema_name.to_string(),
                HashMap::new(),
                KeyValue::new(Some(series.to_string()), Some(sample_id.clone())),
                host.public_key(),
                MutationType::Purge,
            )
        })
        .collect();
    host.db
        .mutation_manager()
        .write_mutations_with_access(mutations, &owner_context(host))
        .await
        .map_err(|e| e.to_string())?;
    // Auto reclaim does not delete order-log rows. Explicit compact does.
    Ok(())
}

/// What one retention pass did, as distinct from what the plane now holds.
///
/// `remaining` alone cannot answer "did this pass reclaim anything" — it is the
/// same number on a pass that removed 60,000 rows and on the pass after it. The
/// drain needs the difference to know when it can stop looking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PruneOutcome {
    pub(super) removed: usize,
    pub(super) remaining: usize,
}

impl PruneOutcome {
    /// A series that was not there to prune. Distinct from a pass that ran and
    /// found nothing only in intent, but both are "no work done".
    pub(super) const NOTHING: Self = Self {
        removed: 0,
        remaining: 0,
    };
}

pub(super) async fn prune_retention(host: &Host, cap: usize) -> Result<PruneOutcome, String> {
    let ids = query_sample_ids(host).await?;
    let prune = sample_ids_to_prune(ids, cap);
    let removed = prune.len();
    purge_series_rows(host, SELF_METRIC_SCHEMA, SELF_METRIC_SERIES, &prune).await?;
    let remaining = query_sample_ids(host).await?.len();
    check_prune_landed(SELF_METRIC_SCHEMA, removed, remaining, cap)?;
    Ok(PruneOutcome { removed, remaining })
}

/// `removed` counts the rows a pass ASKED to purge, not the rows that left the
/// plane — the purge mutations are written and the count is taken before the
/// re-query. Without this check a purge that acked and changed nothing reports
/// success, and the drain then reads that success two ways, both wrong: it logs
/// a reclaim that did not happen, and the next pass — finding the same rows and
/// removing them again to no effect — eventually settles the drain for the rest
/// of the process, so those rows are never reclaimed at all.
///
/// Only checked when the pass claimed to remove something. A pass that found
/// nothing above the cap leaves `remaining` at whatever the plane holds, and on
/// the writer-on path that is legitimately over any cap the caller passed.
pub(super) fn check_prune_landed(
    schema: &str,
    removed: usize,
    remaining: usize,
    cap: usize,
) -> Result<(), String> {
    if removed > 0 && remaining > cap {
        return Err(format!(
            "purged {removed} {schema} rows but {remaining} remain, above the cap of {cap}; \
             the purge acked without leaving the read path"
        ));
    }
    Ok(())
}

pub(super) async fn prune_request_ops_rollup_retention(
    host: &Host,
    cap: usize,
) -> Result<PruneOutcome, String> {
    let ids = query_request_ops_rollup_ids(host).await?;
    let prune = sample_ids_to_prune(ids, cap);
    let removed = prune.len();
    purge_series_rows(
        host,
        REQUEST_OPS_ROLLUP_SCHEMA,
        REQUEST_OPS_ROLLUP_SERIES,
        &prune,
    )
    .await?;
    let remaining = query_request_ops_rollup_ids(host).await?.len();
    check_prune_landed(REQUEST_OPS_ROLLUP_SCHEMA, removed, remaining, cap)?;
    Ok(PruneOutcome { removed, remaining })
}

pub(super) async fn query_sample_ids(host: &Host) -> Result<Vec<String>, String> {
    let query = Query::new_with_filter(
        SELF_METRIC_SCHEMA.to_string(),
        vec!["sample_id".to_string()],
        Some(HashRangeFilter::HashKey(SELF_METRIC_SERIES.to_string())),
    );
    let rows = host
        .db
        .query_executor()
        .query_with_access(query, &owner_context(host))
        .await
        .map_err(|e| e.to_string())?;
    let mut ids: Vec<String> = rows
        .get("sample_id")
        .into_iter()
        .flat_map(|field| field.keys())
        .filter_map(|key| key.range.clone())
        .collect();
    ids.sort();
    Ok(ids)
}

pub(super) async fn query_request_ops_rollup_ids(host: &Host) -> Result<Vec<String>, String> {
    let query = Query::new_with_filter(
        REQUEST_OPS_ROLLUP_SCHEMA.to_string(),
        vec!["sample_id".to_string()],
        Some(HashRangeFilter::HashKey(
            REQUEST_OPS_ROLLUP_SERIES.to_string(),
        )),
    );
    let rows = host
        .db
        .query_executor()
        .query_with_access(query, &owner_context(host))
        .await
        .map_err(|e| e.to_string())?;
    let mut ids: Vec<String> = rows
        .get("sample_id")
        .into_iter()
        .flat_map(|field| field.keys())
        .filter_map(|key| key.range.clone())
        .collect();
    ids.sort();
    Ok(ids)
}

pub(super) fn sample_ids_to_prune(mut ids: Vec<String>, cap: usize) -> Vec<String> {
    ids.sort();
    let excess = ids.len().saturating_sub(cap);
    ids.into_iter().take(excess).collect()
}

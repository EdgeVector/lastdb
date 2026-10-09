//! Bounded production driver for the durable atom reverse-edge rebuild.
//!
//! The core store owns the resumable two-pass cursor. This node task only
//! supplies bounded background execution. It never runs on a request path.
//! Each sampler tick can start one pass, and each pass advances a fixed page
//! budget across every storage prefix in the in-memory schema catalog.
//!
//! Legacy v1 rebuild: `LASTDB_ATOM_REF_BACKFILL=1`. Default **off**.
//! Compact v2 rebuild: `LASTDB_ATOM_REF_V2_BACKFILL=0` kill switch. Default **on**.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fold_db::db_operations::atom_store::{AtomRefBackfillPhase, ATOM_REF_MANIFEST_VERSION_HISTORY};

use crate::host::Host;

pub const ATOM_REF_BACKFILL_ENV: &str = "LASTDB_ATOM_REF_BACKFILL";
pub const ATOM_REF_V2_BACKFILL_ENV: &str = "LASTDB_ATOM_REF_V2_BACKFILL";
pub const ATOM_REF_V2_MAX_BYTES_ENV: &str = "LASTDB_ATOM_REF_V2_MAX_BYTES";
pub const ATOM_REF_V1_DRAIN_ENV: &str = "LASTDB_ATOM_REF_V1_DRAIN";

pub const DEFAULT_ATOM_REF_V2_MAX_BYTES: u64 = 1_610_612_736;
pub const ATOM_REF_V2_MAX_BYTES_PER_EDGE: u64 = 320;

const SLOT_PAGE: usize = 256;
const MAX_PAGES_PER_PASS: usize = 256;
const PAGE_PAUSE: Duration = Duration::from_millis(10);

#[derive(Debug, Default)]
pub struct AtomRefBackfillRuntime {
    in_flight: AtomicBool,
}

impl AtomRefBackfillRuntime {
    fn begin(&self) -> bool {
        self.in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn end(&self) {
        self.in_flight.store(false, Ordering::Release);
    }

    pub fn in_flight(&self) -> bool {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// Legacy v1 JSON reverse-edge rebuild stays off. Compact v2 owns the plane.
pub fn atom_ref_backfill_enabled() -> bool {
    env_flag::var_truthy(ATOM_REF_BACKFILL_ENV)
}

/// Compact backfill is default on. Set `LASTDB_ATOM_REF_V2_BACKFILL=0` to stop it.
pub fn atom_ref_v2_backfill_enabled() -> bool {
    env_flag::var_parse(ATOM_REF_V2_BACKFILL_ENV).unwrap_or(true)
}

/// The final legacy drain stays off until the compact-only write boundary.
pub fn atom_ref_v1_drain_enabled() -> bool {
    env_flag::var_truthy(ATOM_REF_V1_DRAIN_ENV)
}

pub fn atom_ref_v2_max_bytes() -> u64 {
    env_flag::var_parsed::<u64>(ATOM_REF_V2_MAX_BYTES_ENV)
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_ATOM_REF_V2_MAX_BYTES)
}

pub fn projected_atom_ref_v2_bytes(current_bytes: u64, active_edges: u64) -> u64 {
    current_bytes.max(active_edges.saturating_mul(ATOM_REF_V2_MAX_BYTES_PER_EDGE))
}

/// Start one bounded pass, at most once per host.
pub fn maybe_spawn_atom_ref_backfill(host: &std::sync::Arc<Host>) {
    let run_v1 = atom_ref_backfill_enabled();
    let run_v2 = atom_ref_v2_backfill_enabled();
    let drain_v1 = atom_ref_v1_drain_enabled();
    let atom_ref_store = host.db.db_ops().atoms();
    let refresh_health = atom_ref_store.atom_ref_v2_dual_write_enabled()
        || atom_ref_store.atom_ref_v2_reads_enabled()
        || atom_ref_store.atom_ref_v2_only_writes_enabled();
    if drain_v1 && run_v1 {
        tracing::warn!(
            target: "lastdbd::atom_ref_backfill",
            "legacy atom reverse-edge drain requires LASTDB_ATOM_REF_BACKFILL=0"
        );
        return;
    }
    if (!run_v1 && !run_v2 && !drain_v1 && !refresh_health) || !host.atom_ref_backfill.begin() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    tokio::spawn(async move {
        if run_v1 {
            if let Err(error) = run_atom_ref_backfill_pages(&host, MAX_PAGES_PER_PASS).await {
                tracing::warn!(
                    target: "lastdbd::atom_ref_backfill",
                    error = %error,
                    "atom reverse-edge backfill pass failed; the durable cursor will retry"
                );
            }
        }
        if run_v2 {
            if let Err(error) = run_atom_ref_v2_backfill_pages(&host, MAX_PAGES_PER_PASS).await {
                tracing::warn!(
                    target: "lastdbd::atom_ref_backfill",
                    error = %error,
                    "compact atom reverse-edge backfill pass failed; the durable cursor will retry"
                );
            }
        }
        if drain_v1 {
            if let Err(error) = run_atom_ref_v1_drain_pages(&host, MAX_PAGES_PER_PASS).await {
                tracing::warn!(
                    target: "lastdbd::atom_ref_backfill",
                    error = %error,
                    "legacy atom reverse-edge drain pass failed; the durable cursor will retry"
                );
            }
        }
        crate::self_metrics::refresh_atom_ref_edge_health(&host).await;
        host.atom_ref_backfill.end();
    });
}

/// Advance at most `page_budget` durable pages across known storage prefixes.
pub(crate) async fn run_atom_ref_backfill_pages(
    host: &Host,
    page_budget: usize,
) -> Result<usize, String> {
    if page_budget == 0 || host.db.backup_publish_target_is_held().await {
        return Ok(0);
    }

    let mut molecules_by_prefix: BTreeMap<Option<String>, BTreeSet<String>> =
        BTreeMap::from([(None, BTreeSet::new())]);
    for schema in host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| format!("read schema catalog for atom backfill: {error}"))?
        .into_values()
    {
        for field in schema.runtime_fields.values() {
            let Some(molecule_uuid) = field.common().molecule_uuid() else {
                continue;
            };
            molecules_by_prefix
                .entry(field.common().storage_prefix().map(str::to_string))
                .or_default()
                .insert(molecule_uuid.clone());
        }
    }

    let started = std::time::Instant::now();
    let mut pages = 0usize;
    let mut slots = 0u64;
    let mut edges = 0u64;
    let mut completed_prefixes = BTreeSet::new();
    let mut blocked_prefixes = BTreeSet::new();

    while pages < page_budget {
        let mut made_progress = false;
        for (storage_prefix, molecule_uuids) in &molecules_by_prefix {
            if pages >= page_budget {
                break;
            }
            if host.db.backup_publish_target_is_held().await {
                tracing::info!(
                    target: "lastdbd::atom_ref_backfill",
                    pages,
                    "atom reverse-edge backfill paused: backup cut is held"
                );
                return Ok(pages);
            }
            let prefix = storage_prefix.as_deref();
            let status = host
                .db
                .db_ops()
                .atoms()
                .atom_ref_backfill_status(prefix)
                .await
                .map_err(|error| format!("read atom backfill cursor: {error}"))?;
            match status.phase {
                AtomRefBackfillPhase::Complete => {
                    let mut history_complete = true;
                    for molecule_uuid in molecule_uuids {
                        if pages >= page_budget {
                            history_complete = false;
                            break;
                        }
                        if host.db.backup_publish_target_is_held().await {
                            tracing::info!(
                                target: "lastdbd::atom_ref_backfill",
                                pages,
                                "atom reverse-edge backfill paused: backup cut is held"
                            );
                            return Ok(pages);
                        }
                        let Some(manifest) = host
                            .db
                            .db_ops()
                            .atoms()
                            .ensure_atom_ref_molecule_manifest_after_reindex(molecule_uuid, prefix)
                            .await
                            .map_err(|error| format!("seed atom backfill manifest: {error}"))?
                        else {
                            history_complete = false;
                            continue;
                        };
                        if manifest.version == ATOM_REF_MANIFEST_VERSION_HISTORY
                            && manifest.replay_complete
                        {
                            continue;
                        }
                        let report = host
                            .db
                            .db_ops()
                            .atoms()
                            .upgrade_atom_ref_molecule_history_page(
                                molecule_uuid,
                                prefix,
                                SLOT_PAGE,
                            )
                            .await
                            .map_err(|error| format!("advance atom history backfill: {error}"))?;
                        pages = pages.saturating_add(1);
                        slots = slots.saturating_add(report.rows_walked);
                        edges = edges.saturating_add(report.edges_written);
                        made_progress = true;
                        if !report.complete {
                            history_complete = false;
                        }
                        tokio::time::sleep(PAGE_PAUSE).await;
                    }
                    if history_complete {
                        completed_prefixes.insert(storage_prefix.clone());
                    }
                    continue;
                }
                AtomRefBackfillPhase::Blocked => {
                    blocked_prefixes.insert(storage_prefix.clone());
                    continue;
                }
                AtomRefBackfillPhase::Backfill | AtomRefBackfillPhase::Replay => {}
            }

            let report = host
                .db
                .db_ops()
                .atoms()
                .reindex_atom_ref_edges(prefix, Some(SLOT_PAGE))
                .await
                .map_err(|error| format!("advance atom backfill cursor: {error}"))?;
            pages = pages.saturating_add(1);
            slots = slots.saturating_add(report.slots_walked);
            edges = edges.saturating_add(report.edges_written);
            made_progress = true;
            tokio::time::sleep(PAGE_PAUSE).await;
        }
        if !made_progress {
            break;
        }
    }

    if pages > 0 || !blocked_prefixes.is_empty() {
        tracing::info!(
            target: "lastdbd::atom_ref_backfill",
            pages,
            slots,
            edges,
            prefixes = molecules_by_prefix.len(),
            completed_prefixes = completed_prefixes.len(),
            blocked_prefixes = blocked_prefixes.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "atom reverse-edge backfill pass finished"
        );
    }
    Ok(pages)
}

/// Advance compact backfill and per-molecule history audit work.
pub(crate) async fn run_atom_ref_v2_backfill_pages(
    host: &Host,
    page_budget: usize,
) -> Result<usize, String> {
    if page_budget == 0 || host.db.backup_publish_target_is_held().await {
        return Ok(0);
    }
    let mut molecules_by_prefix: BTreeMap<Option<String>, BTreeSet<String>> =
        BTreeMap::from([(None, BTreeSet::new())]);
    for schema in host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| format!("read schema catalog for compact atom backfill: {error}"))?
        .into_values()
    {
        for field in schema.runtime_fields.values() {
            let Some(molecule_uuid) = field.common().molecule_uuid() else {
                continue;
            };
            molecules_by_prefix
                .entry(field.common().storage_prefix().map(str::to_string))
                .or_default()
                .insert(molecule_uuid.clone());
        }
    }

    let started = std::time::Instant::now();
    let max_bytes = atom_ref_v2_max_bytes();
    let mut pages = 0usize;
    let mut slots = 0u64;
    let mut edges = 0u64;
    let mut completed_prefixes = BTreeSet::new();
    let mut blocked_prefixes = BTreeSet::new();

    while pages < page_budget {
        let mut made_progress = false;
        for (storage_prefix, molecule_uuids) in &molecules_by_prefix {
            if pages >= page_budget {
                break;
            }
            if host.db.backup_publish_target_is_held().await {
                tracing::info!(
                    target: "lastdbd::atom_ref_backfill",
                    pages,
                    "compact atom reverse-edge backfill paused: backup cut is held"
                );
                return Ok(pages);
            }
            let prefix = storage_prefix.as_deref();
            let status = host
                .db
                .db_ops()
                .atoms()
                .atom_ref_v2_backfill_status(prefix)
                .await
                .map_err(|error| format!("read compact atom backfill cursor: {error}"))?;
            let current_bytes = host
                .db
                .db_ops()
                .namespaced_store()
                .collection_disk_bytes("atom_ref_edges_v2")
                .unwrap_or(0);
            let projected_bytes = projected_atom_ref_v2_bytes(current_bytes, status.edges_written);
            if current_bytes > max_bytes || projected_bytes > max_bytes {
                tracing::warn!(
                    target: "lastdbd::atom_ref_backfill",
                    current_bytes,
                    projected_bytes,
                    max_bytes,
                    "compact atom reverse-edge backfill stopped at the growth gate"
                );
                return Ok(pages);
            }

            match status.phase {
                AtomRefBackfillPhase::Backfill | AtomRefBackfillPhase::Replay => {
                    let report = host
                        .db
                        .db_ops()
                        .atoms()
                        .reindex_atom_ref_v2_edges(prefix, Some(SLOT_PAGE))
                        .await
                        .map_err(|error| {
                            format!("advance compact atom backfill cursor: {error}")
                        })?;
                    pages = pages.saturating_add(1);
                    slots = slots.saturating_add(report.slots_walked);
                    edges = edges.saturating_add(report.edges_written);
                    made_progress = true;
                    tokio::time::sleep(PAGE_PAUSE).await;
                }
                AtomRefBackfillPhase::Blocked => {
                    blocked_prefixes.insert(storage_prefix.clone());
                }
                AtomRefBackfillPhase::Complete => {
                    let mut history_complete = true;
                    for molecule_uuid in molecule_uuids {
                        if pages >= page_budget {
                            history_complete = false;
                            break;
                        }
                        let Some(manifest) = host
                            .db
                            .db_ops()
                            .atoms()
                            .ensure_atom_ref_v2_molecule_manifest_after_reindex(
                                molecule_uuid,
                                prefix,
                            )
                            .await
                            .map_err(|error| {
                                format!("seed compact atom backfill manifest: {error}")
                            })?
                        else {
                            history_complete = false;
                            continue;
                        };
                        if manifest.version == ATOM_REF_MANIFEST_VERSION_HISTORY
                            && manifest.replay_complete
                        {
                            continue;
                        }
                        let report = host
                            .db
                            .db_ops()
                            .atoms()
                            .upgrade_atom_ref_v2_molecule_history_page(
                                molecule_uuid,
                                prefix,
                                SLOT_PAGE,
                            )
                            .await
                            .map_err(|error| {
                                format!("advance compact atom history backfill: {error}")
                            })?;
                        pages = pages.saturating_add(1);
                        slots = slots.saturating_add(report.rows_walked);
                        edges = edges.saturating_add(report.edges_written);
                        made_progress = true;
                        if !report.complete {
                            history_complete = false;
                        }
                        tokio::time::sleep(PAGE_PAUSE).await;
                    }
                    if history_complete {
                        host.db
                            .db_ops()
                            .atoms()
                            .mark_atom_ref_v2_history_complete(prefix)
                            .await
                            .map_err(|error| {
                                format!("mark compact atom history complete: {error}")
                            })?;
                        completed_prefixes.insert(storage_prefix.clone());
                    }
                }
            }
        }
        if !made_progress {
            break;
        }
    }

    if pages > 0 || !blocked_prefixes.is_empty() {
        tracing::info!(
            target: "lastdbd::atom_ref_backfill",
            pages,
            slots,
            edges,
            prefixes = molecules_by_prefix.len(),
            completed_prefixes = completed_prefixes.len(),
            blocked_prefixes = blocked_prefixes.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "compact atom reverse-edge backfill pass finished"
        );
    }
    Ok(pages)
}

/// Advance the final v1 physical-plane drain.
pub(crate) async fn run_atom_ref_v1_drain_pages(
    host: &Host,
    page_budget: usize,
) -> Result<usize, String> {
    if page_budget == 0 || host.db.backup_publish_target_is_held().await {
        return Ok(0);
    }
    if atom_ref_backfill_enabled() {
        return Err(format!(
            "{ATOM_REF_V1_DRAIN_ENV}=1 requires {ATOM_REF_BACKFILL_ENV}=0"
        ));
    }

    let mut readiness_prefixes = BTreeSet::from([None]);
    for schema in host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| format!("read schema catalog for legacy atom drain: {error}"))?
        .into_values()
    {
        for field in schema.runtime_fields.values() {
            if field.common().molecule_uuid().is_some() {
                readiness_prefixes.insert(field.common().storage_prefix().map(str::to_string));
            }
        }
    }
    let readiness_prefixes = readiness_prefixes.into_iter().collect::<Vec<_>>();

    let started = std::time::Instant::now();
    let mut pages = 0usize;
    let mut keys_deleted = 0u64;
    while pages < page_budget {
        if host.db.backup_publish_target_is_held().await {
            tracing::info!(
                target: "lastdbd::atom_ref_backfill",
                pages,
                keys_deleted,
                "legacy atom reverse-edge drain paused: backup cut is held"
            );
            return Ok(pages);
        }
        let status = host
            .db
            .db_ops()
            .atoms()
            .atom_ref_v1_drain_status()
            .await
            .map_err(|error| format!("read legacy atom drain cursor: {error}"))?;
        if status.completed {
            break;
        }
        let report = host
            .db
            .db_ops()
            .atoms()
            .drain_atom_ref_v1_page(SLOT_PAGE, &readiness_prefixes)
            .await
            .map_err(|error| format!("advance legacy atom drain cursor: {error}"))?;
        pages = pages.saturating_add(1);
        keys_deleted = keys_deleted.saturating_add(report.keys_deleted);
        if report.completed {
            break;
        }
        tokio::time::sleep(PAGE_PAUSE).await;
    }

    if pages > 0 {
        tracing::info!(
            target: "lastdbd::atom_ref_backfill",
            pages,
            keys_deleted,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "legacy atom reverse-edge drain pass finished"
        );
    }
    Ok(pages)
}

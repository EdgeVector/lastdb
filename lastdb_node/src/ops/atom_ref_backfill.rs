//! Bounded production driver for the durable atom reverse-edge rebuild.
//!
//! The core store owns the resumable two-pass cursor. This node task only
//! supplies bounded background execution. It never runs on a request path.
//! Each sampler tick can start one pass, and each pass advances a fixed page
//! budget across every storage prefix in the in-memory schema catalog.
//!
//! Compact v2 rebuild: `LASTDB_ATOM_REF_V2_BACKFILL=0` kill switch. Default **on**.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fold_db::db_operations::atom_store::{AtomRefBackfillPhase, ATOM_REF_MANIFEST_VERSION_HISTORY};

use crate::host::Host;

pub const ATOM_REF_V2_BACKFILL_ENV: &str = "LASTDB_ATOM_REF_V2_BACKFILL";
pub const ATOM_REF_V2_MAX_BYTES_ENV: &str = "LASTDB_ATOM_REF_V2_MAX_BYTES";

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

/// Compact backfill is default on. Set `LASTDB_ATOM_REF_V2_BACKFILL=0` to stop it.
pub fn atom_ref_v2_backfill_enabled() -> bool {
    env_flag::var_parse(ATOM_REF_V2_BACKFILL_ENV).unwrap_or(true)
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
    let run_v2 = atom_ref_v2_backfill_enabled();
    // Health always refreshes: the compact reverse-edge plane is always on.
    if !host.atom_ref_backfill.begin() {
        return;
    }
    let host = std::sync::Arc::clone(host);
    tokio::spawn(async move {
        crate::maintenance_write_gate::run(&host, async {
        if run_v2 {
            if let Err(error) = run_atom_ref_v2_backfill_pages(&host, MAX_PAGES_PER_PASS).await {
                tracing::warn!(
                    target: "lastdbd::atom_ref_backfill",
                    error = %error,
                    "compact atom reverse-edge backfill pass failed; the durable cursor will retry"
                );
            }
        }
        crate::self_metrics::refresh_atom_ref_edge_health(&host).await;
        }).await;
        host.atom_ref_backfill.end();
    });
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

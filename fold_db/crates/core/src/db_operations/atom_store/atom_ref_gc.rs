//! Refcount-driven atom garbage collection.
//!
//! A durable live count of zero creates a candidate epoch. The epoch survives
//! until a committed reference returns. Physical removal requires three facts
//! about that same epoch:
//!
//! 1. the durable count is still zero;
//! 2. the grace deadline has passed; and
//! 3. an isolated reachability audit reported the atom unreachable.
//!
//! Candidate rows live in the local `aref:` plane. One exact row supports
//! O(1) cancellation when a reference returns. One time-ordered queue row
//! supports a bounded oldest-first maintenance range without an atom-plane
//! walk.

use super::AtomStore;
use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding};
use crate::clock::unix_nanos;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::laststore::CollectionCompactReport;
use crate::storage::KvMutation;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Duration;

mod body;
mod reap;
mod rearm;

const ATOM_GC_CANDIDATE_PREFIX: &str = "aref:gc:v1:c:";
const ATOM_GC_QUEUE_PREFIX: &str = "aref:gc:v1:q:";
const ATOM_GC_QUEUE_TIMESTAMP_WIDTH: usize = 20;
type AtomGcCandidateItems = Vec<(Vec<u8>, Vec<u8>)>;

/// Default hold after the count reaches zero.
///
/// The 2026-09-28 real-data CoW proof measured a 286.768-second p99 for 100
/// queued sibling folds. This value is more than 300 times that p99. The audit
/// gate remains mandatory after this duration passes.
pub const DEFAULT_ATOM_GC_GRACE_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// The audit result bound to one zero-count epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomGcAuditResult {
    /// Stable identifier for the isolated-copy audit run.
    pub audit_id: String,
    /// Candidate epoch that the audit inspected.
    pub zero_since_unix_nanos: u64,
    /// Wall-clock time when the audit result landed.
    pub audited_at_unix_nanos: u64,
    /// True means a valid root still reaches the atom. Such an atom cannot reap.
    pub reachable: bool,
}

/// Durable candidate state for one atom.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomGcCandidate {
    pub atom_uuid: String,
    pub zero_since_unix_nanos: u64,
    /// Exact queue key. Positive-count cancellation deletes it without a scan.
    pub queue_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit: Option<AtomGcAuditResult>,
}

impl AtomGcCandidate {
    /// First instant at which this epoch passes `grace_window`.
    #[must_use]
    pub fn eligible_at_unix_nanos(&self, grace_window: Duration) -> u64 {
        self.zero_since_unix_nanos
            .saturating_add(duration_nanos(grace_window))
    }

    fn audit_clears_delete(&self) -> bool {
        self.audit.as_ref().is_some_and(|audit| {
            !audit.reachable
                && audit.zero_since_unix_nanos == self.zero_since_unix_nanos
                && audit.audited_at_unix_nanos >= self.zero_since_unix_nanos
        })
    }
}

/// One bounded oldest-first candidate page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AtomGcCandidatePage {
    pub candidates: Vec<AtomGcCandidate>,
    pub truncated: bool,
}

/// Result of recording an isolated reachability decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AtomGcAuditDecision {
    /// The exact zero epoch now carries an unreachable audit result.
    Cleared,
    /// The audit found a valid root. The candidate remains but cannot reap.
    StillReachable,
    /// A reference returned or a newer zero epoch replaced the audited one.
    StaleEpoch,
    /// An in-flight writer still holds the atom.
    PendingReference,
}

/// Knobs for one bounded reap call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomGcReapOptions {
    pub now_unix_nanos: u64,
    pub grace_window: Duration,
    pub max_candidates: usize,
    pub dry_run: bool,
    /// Rewrite the atom plane after deletes so dead segment bytes return.
    pub compact_after_delete: bool,
}

impl Default for AtomGcReapOptions {
    fn default() -> Self {
        Self {
            now_unix_nanos: unix_nanos(),
            grace_window: DEFAULT_ATOM_GC_GRACE_WINDOW,
            max_candidates: 256,
            dry_run: true,
            compact_after_delete: false,
        }
    }
}

/// Result of one bounded candidate reap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AtomGcReapReport {
    pub dry_run: bool,
    pub candidates_examined: u64,
    /// Permanent references from an encrypted retired-intent outcome.
    #[serde(default)]
    pub retained_retired_intent_reference: u64,
    pub eligible: u64,
    pub atoms_deleted: u64,
    pub storage_keys_deleted: u64,
    pub bytes_deleted_approx: u64,
    pub retained_before_grace: u64,
    pub retained_without_audit: u64,
    pub retained_audit_reachable: u64,
    pub retained_pending_reference: u64,
    pub cancelled_positive_count: u64,
    /// Candidates removed because their atom has no count record. The body leaks.
    #[serde(default)]
    pub cancelled_missing_count: u64,
    pub stale_queue_rows_deleted: u64,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CollectionCompactReport>,
}

impl AtomStore {
    /// List the oldest candidate epochs without walking atom bodies.
    pub async fn list_atom_gc_candidates(
        &self,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<AtomGcCandidatePage, SchemaError> {
        let limit = limit.max(1);
        let prefix = build_storage_key(storage_prefix, ATOM_GC_QUEUE_PREFIX);
        let rows = self
            .main_store
            .scan_items_with_prefix_paged::<AtomGcCandidate>(&prefix, limit.saturating_add(1))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("list atom GC candidates: {error}"))
            })?;
        let truncated = rows.len() > limit;
        Ok(AtomGcCandidatePage {
            candidates: rows
                .into_iter()
                .take(limit)
                .map(|(_, candidate)| candidate)
                .collect(),
            truncated,
        })
    }

    /// Bind one isolated-copy reachability result to an exact zero epoch.
    pub async fn record_atom_gc_audit(
        &self,
        atom_uuid: &str,
        zero_since_unix_nanos: u64,
        audit_id: &str,
        reachable: bool,
        storage_prefix: Option<&str>,
    ) -> Result<AtomGcAuditDecision, SchemaError> {
        let atom_uuid = atom_uuid.to_string();
        let _count_guards = self
            .lock_atom_ref_counts(std::slice::from_ref(&atom_uuid))
            .await;
        let count = self.atom_live_ref_count(&atom_uuid, storage_prefix).await?;
        let Some(mut candidate) = self
            .load_atom_gc_candidate(&atom_uuid, storage_prefix)
            .await?
        else {
            return Ok(AtomGcAuditDecision::StaleEpoch);
        };
        if count != 0 || candidate.zero_since_unix_nanos != zero_since_unix_nanos {
            return Ok(AtomGcAuditDecision::StaleEpoch);
        }
        if self
            .has_any_pending_atom_refs(&atom_uuid, storage_prefix)
            .await?
        {
            return Ok(AtomGcAuditDecision::PendingReference);
        }
        candidate.audit = Some(AtomGcAuditResult {
            audit_id: audit_id.to_string(),
            zero_since_unix_nanos,
            audited_at_unix_nanos: unix_nanos(),
            reachable,
        });
        let value = serde_json::to_vec(&candidate).map_err(|error| {
            SchemaError::InvalidData(format!("serialize atom GC audit result: {error}"))
        })?;
        self.main_store
            .inner()
            .batch_mutate(vec![
                KvMutation::put(
                    atom_gc_candidate_key(&atom_uuid, storage_prefix).into_bytes(),
                    value.clone(),
                ),
                KvMutation::put(candidate.queue_key.as_bytes().to_vec(), value),
            ])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("persist atom GC audit result: {error}"))
            })?;
        Ok(if reachable {
            AtomGcAuditDecision::StillReachable
        } else {
            AtomGcAuditDecision::Cleared
        })
    }

    /// Reap old, still-zero candidates that the audit marked unreachable.
    ///
    /// The delete path uses exact candidate/count/body keys. It never walks the
    /// atom plane. Every candidate is revalidated while it holds the same locks
    /// as atom body writes and refcount transitions.
    pub async fn reap_audited_atom_gc_candidates(
        &self,
        storage_prefix: Option<&str>,
        options: AtomGcReapOptions,
    ) -> Result<AtomGcReapReport, SchemaError> {
        let page = self
            .list_atom_gc_candidates(storage_prefix, options.max_candidates)
            .await?;
        let mut report = AtomGcReapReport {
            dry_run: options.dry_run,
            truncated: page.truncated,
            ..AtomGcReapReport::default()
        };
        let retired_roots = self.retired_gc_page(&page, storage_prefix).await?;
        for queued in page.candidates {
            report.candidates_examined = report.candidates_examined.saturating_add(1);
            if retired_roots.retains(&queued.atom_uuid, &mut report) {
                continue;
            }
            if options.now_unix_nanos < queued.eligible_at_unix_nanos(options.grace_window) {
                report.retained_before_grace = report.retained_before_grace.saturating_add(1);
                continue;
            }
            self.reap_audited_atom_gc_candidate(&queued, storage_prefix, &options, &mut report)
                .await?;
        }
        self.flush_and_compact_after_atom_gc(&options, &mut report)
            .await?;
        Ok(report)
    }

    pub(super) fn new_atom_gc_candidate_items(
        atom_uuid: &str,
        storage_prefix: Option<&str>,
        zero_since_unix_nanos: u64,
    ) -> Result<AtomGcCandidateItems, SchemaError> {
        let candidate = new_candidate(atom_uuid, storage_prefix, zero_since_unix_nanos);
        let value = serde_json::to_vec(&candidate).map_err(|error| {
            SchemaError::InvalidData(format!("serialize new atom GC candidate: {error}"))
        })?;
        Ok(vec![
            (
                atom_gc_candidate_key(atom_uuid, storage_prefix).into_bytes(),
                value.clone(),
            ),
            (candidate.queue_key.as_bytes().to_vec(), value),
        ])
    }

    pub(super) async fn load_atom_gc_candidate(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomGcCandidate>, SchemaError> {
        self.main_store
            .get_item(&atom_gc_candidate_key(atom_uuid, storage_prefix))
            .await
            .map_err(|error| SchemaError::InvalidData(format!("load atom GC candidate: {error}")))
    }

    async fn delete_stale_atom_gc_queue_row(&self, queue_key: &str) -> Result<(), SchemaError> {
        self.main_store
            .inner()
            .batch_mutate(vec![KvMutation::delete(queue_key.as_bytes().to_vec())])
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete stale atom GC queue row: {error}"))
            })
    }

    async fn delete_atom_gc_candidate(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.main_store
            .inner()
            .batch_mutate(candidate_delete_mutations(candidate, storage_prefix))
            .await
            .map_err(|error| SchemaError::InvalidData(format!("cancel atom GC candidate: {error}")))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DeletedAtom {
    atom: bool,
    storage_keys: u64,
    bytes_approx: u64,
}

fn new_candidate(
    atom_uuid: &str,
    storage_prefix: Option<&str>,
    zero_since_unix_nanos: u64,
) -> AtomGcCandidate {
    AtomGcCandidate {
        atom_uuid: atom_uuid.to_string(),
        zero_since_unix_nanos,
        queue_key: atom_gc_queue_key(atom_uuid, zero_since_unix_nanos, storage_prefix),
        audit: None,
    }
}

fn candidate_put_mutations(
    candidate: &AtomGcCandidate,
    storage_prefix: Option<&str>,
) -> Result<Vec<KvMutation>, SchemaError> {
    let value = serde_json::to_vec(candidate).map_err(|error| {
        SchemaError::InvalidData(format!("serialize atom GC candidate: {error}"))
    })?;
    Ok(vec![
        KvMutation::put(
            atom_gc_candidate_key(&candidate.atom_uuid, storage_prefix).into_bytes(),
            value.clone(),
        ),
        KvMutation::put(candidate.queue_key.as_bytes().to_vec(), value),
    ])
}

fn candidate_delete_mutations(
    candidate: &AtomGcCandidate,
    storage_prefix: Option<&str>,
) -> Vec<KvMutation> {
    vec![
        KvMutation::delete(
            atom_gc_candidate_key(&candidate.atom_uuid, storage_prefix).into_bytes(),
        ),
        KvMutation::delete(candidate.queue_key.as_bytes().to_vec()),
    ]
}

fn atom_gc_candidate_key(atom_uuid: &str, storage_prefix: Option<&str>) -> String {
    build_storage_key(
        storage_prefix,
        &format!("{ATOM_GC_CANDIDATE_PREFIX}{}", stable_token(atom_uuid)),
    )
}

fn atom_gc_queue_key(
    atom_uuid: &str,
    zero_since_unix_nanos: u64,
    storage_prefix: Option<&str>,
) -> String {
    build_storage_key(
        storage_prefix,
        &format!(
            "{ATOM_GC_QUEUE_PREFIX}{zero_since_unix_nanos:0ATOM_GC_QUEUE_TIMESTAMP_WIDTH$}:{}",
            stable_token(atom_uuid)
        ),
    )
}

fn stable_token(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(value.as_bytes()))
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

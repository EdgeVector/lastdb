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
use crate::storage::laststore::{CollectionCompactOptions, CollectionCompactReport};
use crate::storage::KvMutation;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Duration;

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
    pub eligible: u64,
    pub atoms_deleted: u64,
    pub storage_keys_deleted: u64,
    pub bytes_deleted_approx: u64,
    pub retained_before_grace: u64,
    pub retained_without_audit: u64,
    pub retained_audit_reachable: u64,
    pub retained_pending_reference: u64,
    pub cancelled_positive_count: u64,
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
        for queued in page.candidates {
            report.candidates_examined = report.candidates_examined.saturating_add(1);
            if options.now_unix_nanos < queued.eligible_at_unix_nanos(options.grace_window) {
                report.retained_before_grace = report.retained_before_grace.saturating_add(1);
                continue;
            }
            let atom_uuid = queued.atom_uuid.clone();
            // Body writers take these locks in this order.
            let _body_guards = self
                .lock_automatic_gc_atoms(std::slice::from_ref(&atom_uuid))
                .await;
            let _count_guards = self
                .lock_atom_ref_counts(std::slice::from_ref(&atom_uuid))
                .await;
            let Some(candidate) = self
                .load_atom_gc_candidate(&atom_uuid, storage_prefix)
                .await?
            else {
                if !options.dry_run {
                    self.delete_stale_atom_gc_queue_row(&queued.queue_key)
                        .await?;
                    report.stale_queue_rows_deleted =
                        report.stale_queue_rows_deleted.saturating_add(1);
                }
                continue;
            };
            if candidate.zero_since_unix_nanos != queued.zero_since_unix_nanos
                || candidate.queue_key != queued.queue_key
            {
                if !options.dry_run {
                    self.delete_stale_atom_gc_queue_row(&queued.queue_key)
                        .await?;
                    report.stale_queue_rows_deleted =
                        report.stale_queue_rows_deleted.saturating_add(1);
                }
                continue;
            }
            if self.atom_live_ref_count(&atom_uuid, storage_prefix).await? != 0 {
                report.cancelled_positive_count = report.cancelled_positive_count.saturating_add(1);
                if !options.dry_run {
                    self.delete_atom_gc_candidate(&candidate, storage_prefix)
                        .await?;
                }
                continue;
            }
            // A pre-count atom can have a false zero. Verify the authoritative
            // reverse-edge partition while both writer locks are held.
            if self
                .has_active_atom_refs(&atom_uuid, storage_prefix)
                .await?
            {
                report.cancelled_positive_count = report.cancelled_positive_count.saturating_add(1);
                if !options.dry_run {
                    self.delete_atom_gc_candidate(&candidate, storage_prefix)
                        .await?;
                }
                continue;
            }
            if self
                .has_any_pending_atom_refs(&atom_uuid, storage_prefix)
                .await?
            {
                report.retained_pending_reference =
                    report.retained_pending_reference.saturating_add(1);
                continue;
            }
            match candidate.audit.as_ref() {
                None => {
                    report.retained_without_audit = report.retained_without_audit.saturating_add(1);
                    continue;
                }
                Some(audit) if audit.reachable => {
                    report.retained_audit_reachable =
                        report.retained_audit_reachable.saturating_add(1);
                    continue;
                }
                Some(_) if !candidate.audit_clears_delete() => {
                    report.retained_without_audit = report.retained_without_audit.saturating_add(1);
                    continue;
                }
                Some(_) => {}
            }
            report.eligible = report.eligible.saturating_add(1);
            if options.dry_run {
                continue;
            }
            let deleted = self
                .delete_atom_gc_candidate_body(&candidate, storage_prefix)
                .await?;
            report.atoms_deleted = report.atoms_deleted.saturating_add(u64::from(deleted.atom));
            report.storage_keys_deleted = report
                .storage_keys_deleted
                .saturating_add(deleted.storage_keys);
            report.bytes_deleted_approx = report
                .bytes_deleted_approx
                .saturating_add(deleted.bytes_approx);
        }
        if !options.dry_run && report.atoms_deleted > 0 {
            self.main_store.inner().flush().await.map_err(|error| {
                SchemaError::InvalidData(format!("flush atom GC deletes: {error}"))
            })?;
            if options.compact_after_delete {
                let namespaced = self.namespaced_store.as_ref().ok_or_else(|| {
                    SchemaError::InvalidData(
                        "atom GC physical delete requires a namespaced store".to_string(),
                    )
                })?;
                report.compaction = Some(
                    namespaced
                        .compact_collection(CollectionCompactOptions {
                            collection: "atoms".to_string(),
                            dry_run: false,
                            seed_committed_history: false,
                        })
                        .await
                        .map_err(|error| {
                            SchemaError::InvalidData(format!(
                                "compact atom segments after audited GC: {error}"
                            ))
                        })?,
                );
            }
        }
        Ok(report)
    }

    pub(super) async fn atom_gc_candidate_mutations_for_count(
        &self,
        atom_uuid: &str,
        after: u64,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let existing = self
            .load_atom_gc_candidate(atom_uuid, storage_prefix)
            .await?;
        if after == 0 {
            if existing.is_some() {
                return Ok(Vec::new());
            }
            let candidate = new_candidate(atom_uuid, storage_prefix, unix_nanos());
            return candidate_put_mutations(&candidate, storage_prefix);
        }
        Ok(existing.map_or_else(Vec::new, |candidate| {
            candidate_delete_mutations(&candidate, storage_prefix)
        }))
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

    async fn load_atom_gc_candidate(
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

    async fn delete_atom_gc_candidate_body(
        &self,
        candidate: &AtomGcCandidate,
        storage_prefix: Option<&str>,
    ) -> Result<DeletedAtom, SchemaError> {
        let locator_key = build_storage_key(
            storage_prefix,
            &atom_locator_codec::locator_key(&candidate.atom_uuid),
        );
        let locator: Option<Value> =
            self.main_store
                .get_item(&locator_key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("load atom locator before GC delete: {error}"))
                })?;
        let mut body_keys = vec![build_storage_key(
            storage_prefix,
            &atom_key_codec::storage_key(AtomKeyEncoding::Flat, None, &candidate.atom_uuid),
        )];
        if let Some(partition) = locator.as_ref().and_then(atom_locator_codec::decode_value) {
            body_keys.push(build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    &candidate.atom_uuid,
                ),
            ));
        }
        body_keys.sort();
        body_keys.dedup();
        let raw_bodies = self
            .main_store
            .inner()
            .get_many(
                body_keys
                    .iter()
                    .map(|key| key.as_bytes().to_vec())
                    .collect(),
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("load atom body before GC delete: {error}"))
            })?;
        let present: Vec<(String, Vec<u8>)> = body_keys
            .into_iter()
            .zip(raw_bodies)
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        let atom = match present.first() {
            Some((_, raw)) => Some(self.decode_atom_bytes(raw).await.map_err(|error| {
                SchemaError::InvalidData(format!("decode atom before audited GC delete: {error}"))
            })?),
            None => None,
        };
        let mut mutations = Vec::new();
        let mut bytes_approx = 0u64;
        for (key, value) in &present {
            bytes_approx = bytes_approx.saturating_add((key.len() + value.len()) as u64);
            mutations.push(KvMutation::delete(key.as_bytes().to_vec()));
        }
        if let Some(atom) = atom.as_ref() {
            for blob_ref in
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
            {
                mutations.push(KvMutation::delete(
                    super::BlobRefEdge::atom(&candidate.atom_uuid, &blob_ref)
                        .storage_key(storage_prefix)
                        .into_bytes(),
                ));
            }
        }
        mutations.push(KvMutation::delete(locator_key.into_bytes()));
        mutations.push(KvMutation::delete(
            super::atom_ref_edges::atom_live_ref_count_key(&candidate.atom_uuid, storage_prefix)
                .into_bytes(),
        ));
        // The schema marker lives in a separate namespace, so LastStore cannot
        // include it in the main-store transaction. Delete it first and keep
        // both candidate rows until that delete succeeds. If the following
        // main transaction fails, the intact candidate retries the idempotent
        // schema-marker delete and the body transaction on the next pass.
        if let Some(atom) = atom.as_ref() {
            let index_key = build_storage_key(
                storage_prefix,
                &super::helpers::schema_index_codec::record_key(
                    atom.source_schema_name(),
                    &candidate.atom_uuid,
                ),
            );
            self.schema_index_store
                .delete_item(&index_key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("delete audited atom schema marker: {error}"))
                })?;
        }
        mutations.extend(candidate_delete_mutations(candidate, storage_prefix));
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("delete audited zero-count atom: {error}"))
            })?;
        Ok(DeletedAtom {
            atom: !present.is_empty(),
            storage_keys: u64::try_from(present.len()).unwrap_or(u64::MAX),
            bytes_approx,
        })
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

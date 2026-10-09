use super::*;

/// Server-side ceiling on the per-request page size for
/// `GET /v1/canonicalization-near-misses`. Matches the
/// "pagination assumed bounded" stance taken elsewhere in this
/// service — schemas are hundreds, not millions, so a 1k cap is
/// plenty and protects the worker from a misconfigured client
/// asking for everything at once.
pub const MAX_NEAR_MISSES_LIMIT: usize = 1000;

/// Default `?limit=` when the caller doesn't specify one.
pub const DEFAULT_NEAR_MISSES_LIMIT: usize = 100;

impl SchemaServiceState {
    /// Query the in-memory near-miss audit log with optional RFC 3339
    /// `since`/`until` bounds and offset/limit pagination. Returned
    /// records are sorted newest-first; `total` is the count after
    /// filtering, pre-pagination; `next_offset` is `Some(offset + limit)`
    /// when more records remain.
    ///
    /// Caps `limit` at `MAX_NEAR_MISSES_LIMIT`. Lock-poisoning is
    /// surfaced as an empty result with `total = 0` — operationally that
    /// matches "nothing recorded" so callers don't need a separate error
    /// path for a state that should never occur (the writer never
    /// panics under the lock).
    pub fn query_near_misses(
        &self,
        since: Option<&str>,
        until: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> (Vec<NearMissRecord>, usize, Option<usize>) {
        let limit = limit.min(MAX_NEAR_MISSES_LIMIT);
        let Ok(records) = self.near_misses.read() else {
            tracing::warn!(
                target: "schema_service::schema",
                "near_misses read lock poisoned — returning empty page",
            );
            return (Vec::new(), 0, None);
        };
        let mut filtered: Vec<NearMissRecord> = records
            .iter()
            .filter(|r| match since {
                Some(s) => r.timestamp.as_str() >= s,
                None => true,
            })
            .filter(|r| match until {
                Some(u) => r.timestamp.as_str() < u,
                None => true,
            })
            .cloned()
            .collect();
        filtered.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        let total = filtered.len();
        let page: Vec<_> = filtered.into_iter().skip(offset).take(limit).collect();
        // An empty page means the caller can't make progress with this
        // request (either `limit == 0` or `offset >= total`). Returning
        // `Some(offset)` in either case would make a follow-up request
        // with the cursor loop forever on the same empty page.
        let next_offset = if !page.is_empty() && offset + page.len() < total {
            Some(offset + page.len())
        } else {
            None
        };
        (page, total, next_offset)
    }

    /// Combined Phase B veto + Phase C shadow gate. Replaces the bare
    /// dual-signal call at each structural merge seam in
    /// [`Self::add_schema`].
    ///
    /// Returns `true` when the merge should proceed, `false` when it
    /// must be vetoed. Behavior depends on two env flags (see
    /// [`crate::state_matching::dual_signal_canonicalization_enabled`]
    /// and [`crate::state_matching::shadow_mode_enabled`]):
    ///
    /// | Shadow | Phase B | Returned   | Side effect                              |
    /// |--------|---------|------------|------------------------------------------|
    /// | off    | off     | `true`     | (none — single-signal hot path)          |
    /// | off    | on      | dual-allow | "Dual-signal veto" log on `false`        |
    /// | on     | off     | `true`     | NearMissRecord persisted on disagreement |
    /// | on     | on      | `true`     | NearMissRecord persisted on disagreement |
    ///
    /// Shadow wins over Phase B when both are on: the disagreement data
    /// must reflect a stable single-signal baseline, not a baseline
    /// already perturbed by Phase B vetoes.
    ///
    /// `single_signal_decision` is what the legacy single-signal algorithm
    /// decides at this seam (`Expanded` at the structural-merge seams,
    /// `AlreadyExists` at the identity-hash dedup seam); `veto_decision`
    /// is what the dual-signal outcome would have been instead — together
    /// they form the near-miss record's decision columns. The two
    /// descriptive-name seams fall through to a fresh `Added` registration
    /// on veto; the race-condition and identity-hash seams fall through to
    /// `DescriptiveNameConflict`.
    pub(super) async fn shadow_aware_dual_signal_check(
        &self,
        incoming: &Schema,
        existing: &Schema,
        candidate_hash: &str,
        existing_hash: &str,
        single_signal_decision: NearMissDecision,
        veto_decision: NearMissDecision,
    ) -> bool {
        use crate::state_matching::{dual_signal_canonicalization_enabled, shadow_mode_enabled};

        let shadow = shadow_mode_enabled();
        let phase_b = dual_signal_canonicalization_enabled();
        if !shadow && !phase_b {
            return true;
        }

        let diagnostic = self.dual_signal_diagnostic(incoming, existing);
        // Embedder failure is treated as a veto in the Phase B hot path
        // (mirrors the original `purpose_signal_passes` contract); in
        // shadow-only mode we still allow the merge because shadow MUST
        // be transparent — we just have no data to record.
        let dual_allows = diagnostic.as_ref().is_some_and(|d| d.allows_merge);

        if shadow {
            if let Some(d) = diagnostic.as_ref() {
                if !dual_allows {
                    let record = NearMissRecord {
                        registration_id: uuid::Uuid::new_v4().to_string(),
                        candidate_schema_hash: candidate_hash.to_string(),
                        existing_canonical_hash: existing_hash.to_string(),
                        single_signal_decision,
                        dual_signal_decision: veto_decision,
                        struct_similarity: d.struct_similarity,
                        purpose_similarity: d.purpose_similarity,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                    self.append_near_miss(record).await;
                }
            }
            // Shadow forces the single-signal decision regardless of
            // whether Phase B is also on — see the doc-comment table.
            return true;
        }

        // Shadow off, Phase B on. Preserve the original veto log so
        // operators reading existing Phase B observability see the same
        // message they did before this refactor.
        if !dual_allows {
            tracing::info!(
                target: "schema_service::schema",
                incoming_desc = %incoming.descriptive_name.as_deref().unwrap_or(""),
                existing_desc = %existing.descriptive_name.as_deref().unwrap_or(""),
                "Dual-signal veto: structural match rejected by purpose-signal gate — registering as new canonical",
            );
        }
        dual_allows
    }

    /// Append a Phase C near-miss record to both the in-memory cache and
    /// the configured persistence backend. Best-effort — persistence
    /// failures are logged at `warn!` but do not bubble up, because the
    /// shadow-mode call site MUST NOT fail the registration on a logging
    /// error.
    pub async fn append_near_miss(&self, record: NearMissRecord) {
        // 1. In-memory append first — the audit endpoint reads from here
        // and a persistence failure shouldn't hide the disagreement.
        match write_lock(&self.near_misses, "near_misses") {
            Ok(mut near_misses) => near_misses.push(record.clone()),
            Err(e) => {
                tracing::warn!(
                    target: "schema_service::schema",
                    error = %e,
                    "Failed to grab near_misses write lock — skipping in-memory append",
                );
            }
        }

        // 2. Persist to the backend. We open the sled tree on demand
        // (mirroring `persist_view`) rather than threading it through the
        // SchemaStorage enum, which would touch every variant call site.
        let result = match &self.storage {
            SchemaStorage::External(backend) => backend.append_near_miss(&record).await,
        };
        if let Err(e) = result {
            tracing::warn!(
                target: "schema_service::schema",
                error = %e,
                registration_id = %record.registration_id,
                "Failed to persist near-miss record",
            );
        }
    }
}

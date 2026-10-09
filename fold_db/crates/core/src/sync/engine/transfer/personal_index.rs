//! Personal log index read/write/merge.

use super::super::*;
use crate::sync::error::SyncResult;
use crate::sync::org_sync::SyncTarget;
use std::sync::atomic::Ordering;

const PERSONAL_LOG_INDEX_CAS_RETRIES: usize = 5;
pub(crate) const PERSONAL_LOG_INDEX_RECONCILE_EVERY_READS: u64 = 16;

impl SyncEngine {
    pub(crate) fn supports_personal_log_index(target: &SyncTarget) -> bool {
        target.prefix.is_empty()
    }

    pub(crate) async fn read_personal_log_index(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<Option<PersonalLogIndex>> {
        Ok(self
            .read_personal_log_index_versioned(target)
            .await?
            .map(|(index, _etag)| index))
    }

    async fn read_personal_log_index_versioned(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<Option<(PersonalLogIndex, Option<String>)>> {
        debug_assert!(Self::supports_personal_log_index(target));
        let url = self
            .auth
            .presign_snapshot_download_for_target(target, PERSONAL_LOG_INDEX_SNAPSHOT)
            .await?;
        let Some((bytes, etag)) = self.s3.download_with_etag(&url).await? else {
            return Ok(None);
        };
        let plaintext = target.crypto.decrypt(&bytes).await?;
        let index = serde_json::from_slice(&plaintext)?;
        Ok(Some((index, etag)))
    }

    async fn write_personal_log_index_if_current(
        &self,
        target: &SyncTarget,
        index: &PersonalLogIndex,
        expected_etag: Option<&str>,
    ) -> SyncResult<bool> {
        debug_assert!(Self::supports_personal_log_index(target));
        let plaintext = serde_json::to_vec(index)?;
        let ciphertext = target.crypto.encrypt(&plaintext).await?;
        let url = self
            .auth
            .presign_snapshot_upload_for_target(target, PERSONAL_LOG_INDEX_SNAPSHOT)
            .await?;
        let written = self
            .s3
            .upload_conditional(&url, ciphertext, expected_etag)
            .await?;
        if !written {
            return Ok(false);
        }
        if let Err(e) = self
            .auth
            .confirm_snapshot_upload(PERSONAL_LOG_INDEX_SNAPSHOT)
            .await
        {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                "confirm_snapshot_upload metering failed for personal log index (non-fatal)"
            );
        }
        Ok(true)
    }

    pub(crate) async fn merge_personal_log_index<I>(
        &self,
        target: &SyncTarget,
        uploaded_seqs: I,
    ) -> SyncResult<()>
    where
        I: IntoIterator<Item = u64>,
    {
        if !Self::supports_personal_log_index(target) {
            return Ok(());
        }

        let uploaded: Vec<u64> = uploaded_seqs.into_iter().collect();
        if uploaded.is_empty() {
            return Ok(());
        }

        for attempt in 1..=PERSONAL_LOG_INDEX_CAS_RETRIES {
            let (mut index, etag) = match self.read_personal_log_index_versioned(target).await? {
                Some((index, Some(etag))) => (index, Some(etag)),
                Some((_index, None)) => {
                    return Err(SyncError::S3(
                        "personal log index response omitted ETag; refusing blind overwrite"
                            .to_string(),
                    ));
                }
                None => {
                    // A create must start from the complete object list, not
                    // only this device's upload, or it can hide older sparse
                    // seqs that predate the index.
                    let objects = self.auth.list_log_objects(target).await?;
                    self.prove_prefix_decryptable_from_objects(target, &objects)
                        .await?;
                    (
                        PersonalLogIndex::from_seqs(
                            objects
                                .iter()
                                .filter_map(|obj| parse_flat_log_key(&obj.key)),
                        ),
                        None,
                    )
                }
            };
            index.append(uploaded.iter().copied());
            if self
                .write_personal_log_index_if_current(target, &index, etag.as_deref())
                .await?
            {
                return Ok(());
            }
            tracing::info!(
                target = %target.label,
                attempt,
                "personal log index CAS conflicted; re-reading and unioning"
            );
        }
        Err(SyncError::S3(format!(
            "personal log index remained contended after {PERSONAL_LOG_INDEX_CAS_RETRIES} CAS attempts"
        )))
    }

    async fn refresh_personal_log_index_from_listing(
        &self,
        target: &SyncTarget,
        listed: &PersonalLogIndex,
    ) -> SyncResult<()> {
        for attempt in 1..=PERSONAL_LOG_INDEX_CAS_RETRIES {
            let (mut merged, etag) = match self.read_personal_log_index_versioned(target).await? {
                Some((index, Some(etag))) => (index, Some(etag)),
                Some((_index, None)) => {
                    return Err(SyncError::S3(
                        "personal log index response omitted ETag; refusing blind refresh"
                            .to_string(),
                    ));
                }
                None => (PersonalLogIndex::from_seqs([]), None),
            };
            // A routine refresh is monotonic. Compaction has a separate CAS
            // prune path because it alone has proof that older objects were
            // deliberately deleted.
            merged.append(listed.seqs.iter().copied());
            if self
                .write_personal_log_index_if_current(target, &merged, etag.as_deref())
                .await?
            {
                return Ok(());
            }
            tracing::info!(
                target = %target.label,
                attempt,
                "personal log index refresh CAS conflicted; re-reading and unioning"
            );
        }
        Err(SyncError::S3(format!(
            "personal log index refresh remained contended after {PERSONAL_LOG_INDEX_CAS_RETRIES} CAS attempts"
        )))
    }

    pub(crate) async fn prune_personal_log_index_after_compact(
        &self,
        target: &SyncTarget,
        retained: &PersonalLogIndex,
        compacted_through: u64,
    ) -> SyncResult<()> {
        for attempt in 1..=PERSONAL_LOG_INDEX_CAS_RETRIES {
            let (mut desired, etag) = match self.read_personal_log_index_versioned(target).await? {
                Some((current, Some(etag))) => {
                    let mut desired = retained.clone();
                    desired.append(
                        current
                            .seqs
                            .into_iter()
                            .filter(|seq| *seq > compacted_through),
                    );
                    (desired, Some(etag))
                }
                Some((_current, None)) => {
                    return Err(SyncError::S3(
                        "personal log index response omitted ETag; refusing blind compact prune"
                            .to_string(),
                    ));
                }
                None => (retained.clone(), None),
            };
            desired.seqs.retain(|seq| *seq > compacted_through);
            if self
                .write_personal_log_index_if_current(target, &desired, etag.as_deref())
                .await?
            {
                return Ok(());
            }
            tracing::info!(
                target = %target.label,
                attempt,
                "personal log index compact prune CAS conflicted; re-reading and preserving peer seqs"
            );
        }
        Err(SyncError::S3(format!(
            "personal log index compact prune remained contended after {PERSONAL_LOG_INDEX_CAS_RETRIES} CAS attempts"
        )))
    }

    pub(crate) async fn list_log_seqs_and_refresh_personal_index(
        &self,
        target: &SyncTarget,
        cursor: u64,
    ) -> SyncResult<Vec<u64>> {
        let objects = self.auth.list_log_objects(target).await?;
        let index = PersonalLogIndex::from_seqs(
            objects
                .iter()
                .filter_map(|obj| parse_flat_log_key(&obj.key)),
        );
        let new_seqs = index.seqs_after(cursor);

        if Self::supports_personal_log_index(target) {
            // Prove the current key can still decrypt this prefix BEFORE we
            // overwrite its shared `log_index.enc` under that key. A node whose
            // sync key has drifted must not seal a fresh index under the wrong
            // key and clobber the one correct devices rely on.
            //
            // Prefer the full ladder (`prove_prefix_decryptable`) over
            // object-only proof: a decryptable personal index (or keycheck.enc)
            // is already a key proof. Requiring a log-head decrypt here would
            // block legitimate full-list reconciliation whenever the index is
            // incomplete but the head object is temporarily unreadable (or is
            // only present as a list entry for seq discovery).
            // `prove_*` is a no-op (returns Ok) when the prefix is genuinely empty.
            self.prove_prefix_decryptable(target).await?;
            self.refresh_personal_log_index_from_listing(target, &index)
                .await?;
        }

        Ok(new_seqs)
    }

    pub(crate) fn should_reconcile_personal_log_index(&self) -> bool {
        // fetch_add returns the previous counter. Use 1-based read number so
        // reconcile fires on the Nth, 2Nth, … consult — not on the first.
        // Firing when prev==0 (0.is_multiple_of(N)) forced a full list on every
        // cold engine and broke multi-target cycles whose list fixtures share a
        // single object pool (personal would try to decrypt share objects).
        // Incomplete indexes are still healed: every Nth download re-lists, and
        // a missing index still rebuilds immediately on the Ok(None) path.
        let n = self
            .personal_index_reads_since_reconcile
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        n.is_multiple_of(PERSONAL_LOG_INDEX_RECONCILE_EVERY_READS)
    }
}

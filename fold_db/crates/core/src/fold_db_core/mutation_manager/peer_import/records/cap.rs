//! A merged peer record above the record cap stays out of the import.
//!
//! Two devices can each write a record under the cap (decision D3) whose merge
//! is above it. Before this check, `stage_records` refused the merged record,
//! which refused the whole page, and the peer position never moved. Now the
//! import keeps the stored record, drops the losing values that the merge
//! found for it (they describe a merge that does not land), and names it in
//! the receipt with the reason. The rest of the page lands.

use super::merge::PeerRecord;
use crate::db_operations::record_store::LostWrite;
use crate::storage::disk_pack::logical_capture::SkippedPeerRecord;
use lastdb_model::record_value::{RecordValueError, MAX_RECORD_BYTES};
use std::collections::HashSet;

/// The records of the page that fit the cap and their losing values, and the
/// records that stay out.
pub(super) fn split_over_cap(
    changed: Vec<PeerRecord>,
    lost: Vec<LostWrite>,
) -> (Vec<PeerRecord>, Vec<LostWrite>, Vec<SkippedPeerRecord>) {
    let mut kept = Vec::with_capacity(changed.len());
    let mut refused = HashSet::new();
    let mut skipped = Vec::new();
    for record in changed {
        // Another encode error stays for `stage_records`, which refuses it.
        let Err(RecordValueError::TooLarge { bytes }) = record.after.encode() else {
            kept.push(record);
            continue;
        };
        tracing::warn!(
            target: "lastdb::peer_import",
            schema = %record.schema,
            bytes,
            cap = MAX_RECORD_BYTES,
            "a merged peer record is above the record cap; the stored record stays"
        );
        skipped.push(SkippedPeerRecord {
            schema: record.schema,
            key: record.key,
            mutation_id: record.mutation.unwrap_or_default(),
            bytes,
            reason: format!(
                "the merged record is {bytes} bytes; the record cap is {MAX_RECORD_BYTES} bytes"
            ),
        });
        refused.insert(record.record);
    }
    let lost = if refused.is_empty() {
        lost
    } else {
        lost.into_iter()
            .filter(|write| !refused.contains(&write.record))
            .collect()
    };
    (kept, lost, skipped)
}

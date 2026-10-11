use super::*;

/// Authenticated manifest-chain record that sanctions narrowing atom_chunks.
///
/// Two record types:
/// - `retention_floor_deletion_receipt` — user-authorized history delete
///   (floor move). `user_authorized` must be true.
/// - `unbackable_atom_chunk_retirement_receipt` — system-authorized retirement
///   of carried-forward atom refs proven absent from both local disk and the
///   object store. Lists every retired digest in `retired_atom_chunk_shas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupDeletionReceipt {
    /// Type tag for forward-compatible manifest-chain parsing.
    pub record_type: String,
    /// Previous authenticated retention floor / predecessor counter.
    pub floor_from_manifest_counter: u64,
    /// New authenticated retention floor / current counter.
    pub floor_to_manifest_counter: u64,
    /// User floor-moves must be true. Unbackable-atom retirements are
    /// system-authorized (`false`) — they only fire when both local and cloud
    /// absence are proven, so they are not a human history-delete.
    pub user_authorized: bool,
    /// Authorization / retirement timestamp, seconds since the Unix epoch.
    pub authorized_at_unix_secs: u64,
    /// Digests retired by an unbackable-atom receipt (empty for floor moves).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_atom_chunk_shas: Vec<String>,
    /// Machine-readable reason for unbackable retirement (e.g.
    /// [`UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl BackupDeletionReceipt {
    /// Exact owner-authorized purge coverage for one predecessor chunk.
    #[cfg(feature = "cloud-sync")]
    pub(crate) fn covers_purged_atom(&self, from: u64, to: u64, sha: &str) -> bool {
        self.record_type == PURGED_ATOM_RETIREMENT_RECEIPT_TYPE
            && self.reason.as_deref() == Some(PURGED_ATOM_RETIREMENT_REASON)
            && self.user_authorized
            && self.authorized_at_unix_secs > 0
            && self.floor_from_manifest_counter == from
            && self.floor_to_manifest_counter == to
            && to > from
            && self
                .retired_atom_chunk_shas
                .iter()
                .any(|retired| retired == sha)
    }

    /// System receipt: retire carried-forward atom digests that are usable for
    /// neither upload nor restore (absent locally and in cloud).
    pub fn new_unbackable_atom_retirement(
        from_counter: u64,
        to_counter: u64,
        retired_atom_chunk_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut retired_atom_chunk_shas = retired_atom_chunk_shas;
        retired_atom_chunk_shas.sort();
        retired_atom_chunk_shas.dedup();
        Self {
            record_type: UNBACKABLE_ATOM_RETIREMENT_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: false,
            authorized_at_unix_secs,
            retired_atom_chunk_shas,
            reason: Some(UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD.to_string()),
        }
    }

    /// Owner-authorized receipt for atom chunks deliberately retired by the
    /// explicit compaction path.
    pub fn new_purged_atom_retirement(
        from_counter: u64,
        to_counter: u64,
        retired_atom_chunk_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut retired_atom_chunk_shas = retired_atom_chunk_shas;
        retired_atom_chunk_shas.sort();
        retired_atom_chunk_shas.dedup();
        Self {
            record_type: PURGED_ATOM_RETIREMENT_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: true,
            authorized_at_unix_secs,
            retired_atom_chunk_shas,
            reason: Some(PURGED_ATOM_RETIREMENT_REASON.to_string()),
        }
    }

    /// System receipt: leftover names that exist nowhere become named holes
    /// on this stamp. `retired_atom_chunk_shas` is the covering set for chain
    /// validation (any role; the field name is historical).
    pub fn new_named_hole_exclusion(
        from_counter: u64,
        to_counter: u64,
        named_hole_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut named_hole_shas = named_hole_shas;
        named_hole_shas.sort();
        named_hole_shas.dedup();
        Self {
            record_type: NAMED_HOLE_EXCLUSION_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: false,
            authorized_at_unix_secs,
            retired_atom_chunk_shas: named_hole_shas,
            reason: Some(NAMED_HOLE_REASON_ABSENT_LOCAL_AND_CLOUD.to_string()),
        }
    }
}

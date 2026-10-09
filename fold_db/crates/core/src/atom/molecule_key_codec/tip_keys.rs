use super::*;

/// Prefix for archived tip versions (`tv:{version_id}`). Dual-read still
/// recognizes this colon form; new writes use [`tip_version_key`].
pub const TIP_VERSION_PREFIX: &str = "tv:";

/// `history\0{molecule}:{ts}` — one mutation-history row.
#[must_use]
pub fn history_event_key(molecule_uuid: &str, ts: i64) -> String {
    crate::kind_partition::anchored("history", &format!("{molecule_uuid}:{ts:020}"))
}

/// `history\0{molecule}:` — prefix covering one molecule's history rows.
#[must_use]
pub fn history_molecule_prefix(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored("history", &format!("{molecule_uuid}:"))
}

/// `tv\0{version_id}` — one archived tip node in a per-slot tip-version chain.
///
/// Kind-as-partition write form. Dual-read still resolves `tv:{version_id}`.
#[must_use]
pub fn tip_version_key(version_id: &str) -> String {
    crate::kind_partition::anchored("tv", version_id)
}

/// Prefix for the derived atom → tip-version reverse-reference plane.
///
/// `v2` is the partition-local layout. The short-lived v1 layout used `:`
/// between atom and version, so LastStore could not prune candidate lookups to
/// one hash group. Keeping the version in the key makes old derived rows
/// harmless residue rather than ambiguous members of the new access pattern.
pub const TIP_VERSION_BACKREF_PREFIX: &str = "tvr:v2:";

/// Prefix covering every archived tip-version reference to `atom_uuid`.
///
/// The partition separator is both an unambiguous atom/version boundary and
/// LastStore's hash-group routing boundary. Candidate lookups therefore touch
/// only the group for that atom instead of fanning across the collection.
#[must_use]
pub fn tip_version_backref_prefix(atom_uuid: &str) -> String {
    format!("{TIP_VERSION_BACKREF_PREFIX}{atom_uuid}{PARTITION_SEP}")
}

/// One derived atom → archived tip-version row.
#[must_use]
pub fn tip_version_backref_key(atom_uuid: &str, version_id: &str) -> String {
    format!("{}{version_id}", tip_version_backref_prefix(atom_uuid))
}

/// Durable cursor for the background reverse-reference rebuild.
pub const TIP_VERSION_BACKREF_REINDEX_CHECKPOINT_KEY: &str = "tvr-meta:reindex:v2";

/// Completeness marker for the reverse-reference plane in one storage prefix.
pub const TIP_VERSION_BACKREF_COMPLETE_KEY: &str = "tvr-meta:complete:v2";

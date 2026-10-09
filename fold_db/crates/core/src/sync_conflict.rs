/// A merge conflict detected during cloud-sync replay.
///
/// The record is stored in the normal local conflict index, so callers can read
/// and resolve historical conflicts without compiling the cloud-sync engine.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncConflict {
    /// Unique ID: "{mol_uuid}:{ts_nanos_padded}"
    pub id: String,
    /// The molecule where the conflict occurred.
    pub molecule_uuid: String,
    /// The key within the molecule (e.g. "single", hash key, "hash:range").
    pub conflict_key: String,
    /// The atom UUID that won (later written_at).
    pub winner_atom: String,
    /// The atom UUID that lost.
    pub loser_atom: String,
    /// Winner's write timestamp (nanos since epoch).
    pub winner_written_at: u64,
    /// Loser's write timestamp (nanos since epoch).
    pub loser_written_at: u64,
    /// When the conflict was detected.
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub detected_at: chrono::DateTime<chrono::Utc>,
    /// Whether this conflict has been acknowledged/resolved by the user.
    pub resolved: bool,
}

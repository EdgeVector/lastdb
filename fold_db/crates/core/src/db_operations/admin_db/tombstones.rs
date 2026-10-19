//! Tombstone-flag audit/backfill and legacy key-fork audit.

use super::super::atom_store::PerKeyRecord;
use super::*;

mod flag_classify;
mod flags;
mod fork_classify;
mod key_forks;

/// Atom bodies per batched fetch. Bounds peak memory on a store whose live set
/// is far larger than the tombstoned one.
const FETCH_CHUNK: usize = 512;
/// Raw `mk:` rows to fetch per storage page. This bounds one resumed pass's
/// transient scan allocation independently of store size.
const SCAN_PAGE: usize = 2048;
/// Wall-clock budget for one pass, well under the 600s control-socket read
/// deadline.
///
/// A key-count cap alone cannot bound a pass. The per-row cost here is an atom
/// body read, which varies by orders of magnitude across homes, so any count
/// large enough to be useful on a small store is unbounded in time on a large
/// one. That is exactly how a 200,000-key cap let a ~150,000-key workload run
/// twelve minutes and trip the deadline the cap was added to avoid. The bound
/// the caller needs is the one the socket enforces, so bound on that.
const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

/// Mutable state of one `audit_legacy_key_forks` pass.
#[derive(Default)]
struct KeyForkAuditState {
    report: LegacyKeyForkAudit,
    per_molecule: HashMap<String, LegacyKeyForkMoleculeStat>,
    pending: Vec<LegacyKeyForkCandidate>,
}

/// Mutable state of one `audit_key_tombstone_flags` pass.
#[derive(Default)]
struct TombstoneAuditState {
    report: TombstoneFlagBackfillReport,
    per_molecule: HashMap<String, MoleculeTombstoneFlagStat>,
    /// (storage key, molecule, record) for rows the flag calls live.
    pending: Vec<(String, String, PerKeyRecord)>,
    last_key: Option<String>,
}

impl AtomStore {
    /// Fetch one storage page of raw `mk:` rows starting at `page_start`.
    pub(super) async fn scan_mk_page(
        &self,
        page_start: &str,
        mk_end: &str,
        what: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
        self.raw()
            .inner()
            .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{what}: {e}")))
    }
}

//! Complete key membership owned by the core molecule, apart from warm values.
//!
//! The publication owner coordinates captures, source exhaustion, accepted
//! writes, and schema readiness. These types own key data and its byte leases.

pub(crate) use crate::atom::{CompactRangeKey, CoreStoredKey, KeyVisibility};
use crate::atom::{MoleculeKeys, RangeKeyEncoding};
use std::sync::{Arc, Mutex};

mod budget;
mod domain;
mod selection;

pub(crate) use budget::{CoreKeyBudget, CoreKeyReservation};
pub(crate) use selection::CoreKeyRange;

pub(crate) const DEFAULT_SCHEMA_KEY_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const SCHEMA_KEY_BYTES_ENV: &str = "LASTDB_SCHEMA_KEY_BYTES";

/// Only identical membership and encoding domains may share complete keys.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoreKeyIdentity {
    pub(crate) scope: Option<String>,
    pub(crate) molecule: String,
    pub(crate) layout: String,
    pub(crate) encoding_identity: String,
    pub(crate) membership_rules: String,
}

impl CoreKeyIdentity {
    fn allocation_bytes(&self) -> usize {
        self.scope
            .as_ref()
            .map_or(0, String::capacity)
            .saturating_add(self.molecule.capacity())
            .saturating_add(self.layout.capacity())
            .saturating_add(self.encoding_identity.capacity())
            .saturating_add(self.membership_rules.capacity())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoreKeyPhase {
    Loading,
    Ready,
    DiskOnly,
    Failed,
    Retired,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum CoreKeyError {
    #[error("complete key byte allowance refused the allocation")]
    Capacity,
    #[error("complete key owner lock is unavailable")]
    Poisoned,
    #[error("complete key domain does not accept changes in this state")]
    State,
    #[error("complete key page limits are invalid")]
    PageLimit,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct CoreKeyDomainStats {
    pub(crate) phase: &'static str,
    pub(crate) reason: Option<&'static str>,
    pub(crate) keys: usize,
    pub(crate) hashes: usize,
    pub(crate) charged_bytes: usize,
}

#[derive(Debug)]
struct DomainState {
    phase: CoreKeyPhase,
    reason: Option<&'static str>,
    keys: MoleculeKeys,
    charge: CoreKeyReservation,
    live_key_bytes: usize,
    key_bytes_high_water: usize,
    tree_entries_high_water: usize,
}

/// The same Arc is attached to a core molecule and held by selected queries.
/// A retired domain keeps its charge until its final reference is released.
#[derive(Debug)]
pub(crate) struct CoreKeyDomain {
    identity: CoreKeyIdentity,
    range_encoding: RangeKeyEncoding,
    budget: Arc<CoreKeyBudget>,
    state: Mutex<DomainState>,
}

#[derive(Clone, Debug)]
pub(crate) struct CoreKeyLease {
    domain: Arc<CoreKeyDomain>,
}

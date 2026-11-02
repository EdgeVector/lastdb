//! Which verb the TTL sweep erases with, and the warning for a bad name.

use std::sync::Once;

use fold_db::schema::types::operations::MutationType;

/// Environment name: `purge` (default) or `delete`. See [`SweepVerb`].
pub const TTL_SWEEP_VERB_ENV: &str = "LASTDB_TTL_SWEEP_VERB";

/// The delete ledger callers of a TTL sweep. A client erase carries the caller
/// `mutation-pipeline`.
const DELETE_CALLER: &str = "ttl-sweep-delete";
const PURGE_CALLER: &str = "ttl-sweep-purge";

/// How the sweep erases an expired key.
///
/// Both verbs remove the values of the record and keep its delete floor, so an
/// older write cannot bring the key back. Neither needs a proof over the whole
/// schema or a schema barrier.
///
/// `purge` (default) is the hard-erasure verb. It also releases the field
/// history of the record. `delete` is the ordinary delete that a client uses.
/// The erase budget charges a delete one more operation for each field than a
/// purge, so a batch of a wide schema is smaller. See `docs/lastdb-ttl-sweep.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepVerb {
    Delete,
    Purge,
}

impl SweepVerb {
    /// The verb that the node environment names. A value that is neither
    /// `purge` nor `delete` gives the default, and [`warn_on_bad_verb_once`]
    /// says so in the log.
    pub(super) fn from_env() -> Self {
        match std::env::var(TTL_SWEEP_VERB_ENV)
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some(raw) if raw.eq_ignore_ascii_case("delete") => Self::Delete,
            _ => Self::Purge,
        }
    }

    /// The caller tag of the delete ledger row.
    pub(super) fn caller(self) -> &'static str {
        match self {
            Self::Delete => DELETE_CALLER,
            Self::Purge => PURGE_CALLER,
        }
    }

    pub(super) fn mutation_type(self) -> MutationType {
        match self {
            Self::Delete => MutationType::Delete,
            Self::Purge => MutationType::Purge,
        }
    }

    /// A plain delete acknowledges after its resident tombstone, and the
    /// persist lane erases later. The sweep waits for the lane, so a batch is
    /// done when the call returns and the pacing sees the real batch time. A
    /// purge always waits.
    pub(super) fn waits_for_lane(self) -> bool {
        self == Self::Delete
    }
}

/// Log one warning, at the first sampler tick, when the node environment names
/// a verb that the sweep does not know. The sweep then uses `purge`.
pub(super) fn warn_on_bad_verb_once() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        let Ok(raw) = std::env::var(TTL_SWEEP_VERB_ENV) else {
            return;
        };
        let value = raw.trim();
        if !value.is_empty()
            && !value.eq_ignore_ascii_case("delete")
            && !value.eq_ignore_ascii_case("purge")
        {
            tracing::warn!(
                target: "lastdbd::ttl_sweep",
                value,
                "unknown {TTL_SWEEP_VERB_ENV} value; the ttl sweep uses the default verb `purge` (valid: purge, delete)"
            );
        }
    });
}

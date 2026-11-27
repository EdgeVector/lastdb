//! Resolve the shared database read key for sealing and opening log entries.
//!
//! The publisher derives the key from its node seed. A member receives the same
//! key in the open read link and keeps it in the registration. Grant entries
//! carry only role; they never carry the key. Derived sealers are cached per
//! database inside [`super::history_store::HistoryMap`].

use std::path::Path;

use cloud_backup_protocol::Id;
use zeroize::Zeroizing;

use crate::host::owned::{self, Kind};
use crate::host::Host;
use crate::personal_seal::Sealer;
use crate::registered_share;
use crate::shared_seal;

/// The sealer for `database_id` on this node, served from the per-home cache
/// after first derivation, or an error when the read key is unavailable locally.
pub(super) fn sealer(host: &Host, database_id: &str) -> Result<Sealer, String> {
    super::histories_for(host)
        .sealer(database_id)
        .map(|cached| (*cached).clone())
}

/// Derive a sealer from `home` and `node_seed` (used by `HistoryMap` when
/// populating its per-database sealer cache).
pub(super) fn sealer_with_seed(
    home: &Path,
    node_seed: &[u8; 32],
    database_id: &str,
) -> Result<Sealer, String> {
    let key = read_key(home, node_seed, database_id)?;
    shared_seal::for_key(database_id, key)
}

fn read_key(
    home: &Path,
    node_seed: &[u8; 32],
    database_id: &str,
) -> Result<Zeroizing<[u8; 32]>, String> {
    if owned_shareable(home, database_id) {
        return owned::content_encryption_key(node_seed, database_id);
    }
    let id = Id::try_from(database_id.to_owned()).map_err(|_| "database id is not hex32")?;
    registered_share::content_material(home, &id)
        .map_err(|_| "shared database read key is not available on this node".to_owned())
}

fn owned_shareable(home: &Path, database_id: &str) -> bool {
    owned::registered_databases(home)
        .into_iter()
        .any(|row| row.id == database_id && row.kind == Kind::Shareable.as_str())
}

use super::{invalid, map, read_budget, RunRead, StorageResult};
use crate::crypto::content_key_proof;
use crate::db_operations::atom_store::{key_format, AtomStore};
use crate::storage::disk_pack::native_routing;
use crate::storage::disk_pack::restore_safety;
use lastdb_storage_policy::backup_decode as budget;

pub(super) fn verify(main: &RunRead, account_key: &[u8; 32]) -> StorageResult<()> {
    let names = [
        AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes(),
        key_format::DELETE_REPLAY_CUTOFF_KEY.as_bytes(),
        restore_safety::PENDING_KEY.as_bytes(),
    ];
    let coordinates = names.map(|key| {
        (
            native_routing::collection_for_key(key).to_owned(),
            native_routing::encode(key),
        )
    });
    let values = {
        let mut read = main
            .fork_bounded(read_budget(budget::DIRECTORY_READ_BYTES, 3))
            .map_err(map)?;
        read.get_many_bounded(&coordinates, 8192).map_err(map)?
    };
    let values = values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(map)?;
    if values.len() != 3 || values[2].is_some() {
        return Err(invalid("run backup counter restore validation is pending"));
    }
    AtomStore::validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
    let stored = {
        let mut read = main
            .fork_bounded(read_budget(budget::DIRECTORY_READ_BYTES, 1))
            .map_err(map)?;
        read.get_bounded(
            content_key_proof::NAMESPACE,
            &native_routing::encode(content_key_proof::KEY),
            8192,
        )
        .map_err(map)?
        .ok_or_else(|| invalid("run backup account content-key proof is missing"))?
    };
    content_key_proof::verify(account_key, &stored)
        .map_err(|_| invalid("run backup account content-key proof differs"))
}

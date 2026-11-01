use super::{invalid, map, read_budget, RunRead, StorageResult};
use crate::records::key_format::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::content_key_proof;
use crate::storage::disk_pack::native_routing;
use crate::storage::disk_pack::restore_safety;
use lastdb_storage_policy::backup_decode as budget;

pub(super) fn verify(main: &RunRead, account_key: &[u8; 32], version: u16) -> StorageResult<()> {
    let names = [
        CURRENT_KEY_FORMAT_KEY.as_bytes(),
        DELETE_REPLAY_CUTOFF_KEY.as_bytes(),
        restore_safety::PENDING_KEY.as_bytes(),
    ];
    let coordinates = names.map(|key| {
        (
            native_routing::collection_for_key(key).to_owned(),
            native_routing::encode(key),
        )
    });
    let values = {
        let mut read = super::fork_mapped(main, read_budget(budget::DIRECTORY_READ_BYTES, 3))?;
        read.get_many_bounded(&coordinates, 8192).map_err(map)?
    };
    let values = values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(map)?;
    if values.len() != 3 || values[2].is_some() {
        return Err(invalid("run backup counter restore validation is pending"));
    }
    validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| invalid("run backup logical marker is absent"))?;
    if version != crate::storage::disk_pack::run_admission::RECORD_PROFILE
        || !lastdb_model::molecule_key_codec::current_key_format(marker)
    {
        return Err(invalid(
            "run backup logical marker differs from its node profile",
        ));
    }
    let stored = {
        let mut read = super::fork_mapped(main, read_budget(budget::DIRECTORY_READ_BYTES, 1))?;
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

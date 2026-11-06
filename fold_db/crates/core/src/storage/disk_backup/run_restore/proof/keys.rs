use super::{invalid, map, read_budget, RunRead, StorageResult};
use crate::storage::content_key_proof;
use crate::storage::disk_pack::native_routing;
use crate::storage::format::control::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::format::metadata::{validate_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::METADATA_NAMESPACE;
use lastdb_storage_policy::backup_decode as budget;

pub(super) fn verify(main: &RunRead, account_key: &[u8; 32], version: u16) -> StorageResult<()> {
    let coordinates = [
        native_coordinate(CURRENT_KEY_FORMAT_KEY.as_bytes()),
        native_coordinate(DELETE_REPLAY_CUTOFF_KEY.as_bytes()),
        (
            METADATA_NAMESPACE.into(),
            native_routing::encode(LOCAL_AUTHOR_KEY.as_bytes()),
        ),
    ];
    let values = {
        let mut read = super::fork_mapped(
            main,
            read_budget(budget::DIRECTORY_READ_BYTES, coordinates.len() as u64),
        )?;
        read.get_many_bounded(&coordinates, 8192).map_err(map)?
    };
    let values = values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(invalid("run backup logical marker read is incomplete"));
    }
    validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
    use crate::storage::format::{RECORD_PROFILE, SHARED_RECORD_PROFILE};
    match version {
        SHARED_RECORD_PROFILE if values[2].is_none() => {}
        SHARED_RECORD_PROFILE => return Err(invalid("shared root contains a private author")),
        _ => validate_local_author(values[2].as_deref())?,
    }
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| invalid("run backup logical marker is absent"))?;
    if !matches!(version, RECORD_PROFILE | SHARED_RECORD_PROFILE)
        || !lastdb_model::field_key_codec::current_key_format(marker)
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

fn native_coordinate(key: &[u8]) -> (String, String) {
    (
        native_routing::collection_for_key(key).to_owned(),
        native_routing::encode(key),
    )
}

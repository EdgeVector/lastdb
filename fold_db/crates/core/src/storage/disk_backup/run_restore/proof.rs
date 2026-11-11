//! Account and format marker verification against fixed, authenticated disk V2 roots.

use super::{map, phase, Stage};
use crate::storage::content_key_proof;
use crate::storage::disk_pack::native_routing;
use crate::storage::error::{NativeProofCause, StorageResult};
use crate::storage::format::control::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::format::metadata::{validate_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::{
    METADATA_NAMESPACE, RECORDS_COLLECTION, RECORD_PROFILE, SHARED_RECORD_PROFILE,
};
use crate::storage::StorageError;
use lastdb_model::storage_policy::backup_decode as budget;
use laststore::disk_run::home::{ReadBudget, RunRead};

/// Verify account markers and content-key proof on an inactive native root.
pub(super) fn verify(
    main: &RunRead,
    _cas: Option<&RunRead>,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<u64> {
    phase(Stage::Account, verify_account(main, account_key, version))?;
    Ok(0)
}

/// Account admission check against the inactive root's format markers and content-key proof.
pub(super) fn verify_account(
    main: &RunRead,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<()> {
    let coordinates = [
        native_coordinate(CURRENT_KEY_FORMAT_KEY.as_bytes()),
        native_coordinate(DELETE_REPLAY_CUTOFF_KEY.as_bytes()),
        (
            METADATA_NAMESPACE.into(),
            native_routing::encode(LOCAL_AUTHOR_KEY.as_bytes()),
        ),
    ];
    let values = {
        let mut read = fork_mapped(
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
        let mut read = fork_mapped(main, read_budget(budget::DIRECTORY_READ_BYTES, 1))?;
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
    (RECORDS_COLLECTION.to_owned(), native_routing::encode(key))
}

fn fork_mapped(read: &RunRead, budget: ReadBudget) -> StorageResult<RunRead> {
    read.fork_bounded(budget).map_err(map)
}

fn read_budget(bytes: u64, keys: u64) -> ReadBudget {
    ReadBudget {
        pages: budget::RECORD_INVENTORY_PAGES,
        blocks: budget::NATIVE_STREAM_BLOCKS,
        bytes,
        keys,
    }
}

fn invalid(detail: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(detail),
    }
}

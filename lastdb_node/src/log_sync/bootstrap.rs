//! Bootstrap: birth + current records as signed data entries.

use app_identity_crypto::SigningKey;
use mutation_log::{
    bootstrap_from_records, BootstrapGrant, BootstrapParams, BootstrapRecord, DatabaseId, DeviceId,
    Hex32, NodeKey,
};

use super::history_store::{self, HistoryMap};

pub(super) struct BootstrapInput<'a> {
    pub(super) database_id: &'a str,
    pub(super) signing_key: &'a SigningKey,
    pub(super) owner: NodeKey,
    pub(super) device_id: DeviceId,
    pub(super) db_salt: Hex32,
    pub(super) birth_signature: String,
    pub(super) authored_at_ms: u64,
    pub(super) records: &'a [BootstrapRecord],
    pub(super) grants: &'a [BootstrapGrant],
}

pub(super) fn bootstrap_database(
    histories: &HistoryMap,
    home: &std::path::Path,
    input: BootstrapInput<'_>,
) -> Result<(), String> {
    let database_id =
        DatabaseId::from_hex(input.database_id).map_err(|_| "database_id is not hex32")?;
    let result = bootstrap_from_records(&BootstrapParams {
        database_id,
        signing_key: input.signing_key,
        owner: input.owner,
        device_id: input.device_id,
        db_salt: input.db_salt,
        birth_signature: input.birth_signature,
        authored_at_ms: input.authored_at_ms,
        records: input.records,
        grants: input.grants,
    })
    .map_err(|e| e.to_string())?;
    let mut history = result.history;
    history_store::persist_outbox(home, input.database_id, &mut history)?;
    histories.insert(history)?;
    Ok(())
}

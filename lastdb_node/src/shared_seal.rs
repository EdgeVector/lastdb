//! Build the personal database's canonical accepted payload for a validated shared write.
use fold_db::storage::disk_backup::accepted;
use mutation_log::{DatabaseId, SchemaBody, SEALED_NAMESPACE};
use zeroize::Zeroizing;

use crate::personal_seal::Sealer;

/// Use the same schema declaration bytes as a captured personal schema.
pub(crate) fn seal_schema(
    sealer: &Sealer,
    node_key: mutation_log::NodeKey,
    device_id: mutation_log::DeviceId,
    writer_seq: u64,
    body: &SchemaBody,
) -> Result<SchemaBody, String> {
    let definition = serde_json::from_slice(&body.definition)
        .map_err(|error| format!("invalid schema: {error}"))?;
    let plain = accepted::schema_plain(
        &uuid::Uuid::new_v4().to_string(),
        &body.namespace,
        &definition,
    )
    .map_err(|error| error.to_string())?;
    Ok(SchemaBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        definition: sealer.seal(node_key, device_id, writer_seq, &plain)?,
    })
}

/// Build a sealer from a database id and its shared read key.
pub(crate) fn for_key(database_id: &str, key: Zeroizing<[u8; 32]>) -> Result<Sealer, String> {
    Ok(Sealer::for_read_key(
        DatabaseId::from_hex(database_id).map_err(|_| "database id is not hex32")?,
        key,
    ))
}

//! Seal `data` and `schema` entry bodies in a shared database.
//!
//! Shared databases reuse [`crate::personal_seal::Sealer`] (`Sealer::for_read_key`)
//! for position-bound AES-256-GCM encryption under the shared database read key,
//! and pack the real schema namespace (`1..=128` UTF-8 bytes) in front of the
//! record or schema bytes via [`mutation_log::pack_hiding_namespace`]. Every
//! sealed entry carries the fixed wire namespace `sealed`
//! ([`mutation_log::SEALED_NAMESPACE`]), hiding both schema names and payloads
//! from relays and non-holders of the read key.

use mutation_log::{pack_hiding_namespace, DataBody, DatabaseId, SchemaBody, SEALED_NAMESPACE};
use zeroize::Zeroizing;

use crate::personal_seal::Sealer;

/// Seal a data body so the relay sees only the fixed sealed namespace.
pub(crate) fn seal_data(
    sealer: &Sealer,
    node_key: mutation_log::NodeKey,
    device_id: mutation_log::DeviceId,
    writer_seq: u64,
    body: DataBody,
) -> Result<DataBody, String> {
    let plain = pack_hiding_namespace(&body.namespace, &body.payload)
        .map_err(std::borrow::ToOwned::to_owned)?;
    Ok(DataBody {
        namespace: SEALED_NAMESPACE.to_owned(),
        payload: sealer.seal(node_key, device_id, writer_seq, &plain)?,
        ..body
    })
}

/// Seal a schema body so the relay sees only the fixed sealed namespace.
pub(crate) fn seal_schema(
    sealer: &Sealer,
    node_key: mutation_log::NodeKey,
    device_id: mutation_log::DeviceId,
    writer_seq: u64,
    body: &SchemaBody,
) -> Result<SchemaBody, String> {
    let plain = pack_hiding_namespace(&body.namespace, &body.definition)
        .map_err(std::borrow::ToOwned::to_owned)?;
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

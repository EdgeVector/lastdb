//! Seal the opaque content of entries in a shared database.
//!
//! Same contract as the personal database: seal once at write time with the
//! database read key, store and upload only that sealed form, and open in
//! memory when applying or reading. The real schema name and record bytes
//! travel inside the ciphertext; the wire namespace is always `sealed`.

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
    let plain =
        pack_hiding_namespace(&body.namespace, &body.payload).map_err(|error| error.to_owned())?;
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
    body: SchemaBody,
) -> Result<SchemaBody, String> {
    let plain = pack_hiding_namespace(&body.namespace, &body.definition)
        .map_err(|error| error.to_owned())?;
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

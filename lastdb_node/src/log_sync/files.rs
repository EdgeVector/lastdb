//! Stable blob path for one accepted file write.
//!
//! The path is the database id, the schema namespace, the SHA-256 of the
//! canonical record key, and the mutation id. `upload` sends the sealed
//! bytes through the existing backup client. This module does not open a
//! job, scan backup rows, or call a provider.

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{canonical_bytes, Digest, Id};
use sha2::{Digest as _, Sha256};

/// Stable object path for one accepted file write.
pub(super) fn path_for(
    database_id: &str,
    mutation_id: &str,
    canonical: &[u8],
) -> Result<BlobPath, ()> {
    let value: serde_json::Value = serde_json::from_slice(canonical).map_err(|_| ())?;
    if value.get("mutation_id").and_then(|item| item.as_str()) != Some(mutation_id) {
        return Err(());
    }
    let schema = value
        .get("schema")
        .and_then(|item| item.as_str())
        .ok_or(())?;
    let space = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    let key = value.get("key").ok_or(())?;
    let key_bytes = canonical_bytes(key).map_err(|_| ())?;
    let record = fold_db::hex::hex_lower(Sha256::digest(key_bytes));
    let path = BlobPath {
        database: Id::try_from(database_id.to_owned()).map_err(|_| ())?,
        space: space.to_owned().try_into().map_err(|_| ())?,
        record: Digest::try_from(record).map_err(|_| ())?,
        write_id: mutation_id.to_owned().try_into().map_err(|_| ())?,
    };
    path.validate().map_err(|_| ())?;
    if path.database.as_str() != database_id {
        return Err(());
    }
    Ok(path)
}

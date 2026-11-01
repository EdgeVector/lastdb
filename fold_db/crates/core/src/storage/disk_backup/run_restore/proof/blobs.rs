//! Blob counters and the blob dwell queue live in this collection.
//! Each restored row must pass the current key law.
use super::{files, rows::Rows, Proof, StorageResult};

/// Validate every coordinate in this collection with the current key law.
pub(super) fn verify(proof: &Proof<'_>) -> StorageResult<u64> {
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "atoms")?;
    while let Some((coordinate, body)) = rows.next()? {
        drop(body);
        files::validate_restored_record_key(&coordinate)?;
    }
    Ok(0)
}

//! Verify an origin key and a bounded mutation id list from an old backup.

use super::*;
use ed25519_dalek::VerifyingKey;
use std::collections::BTreeSet;

pub(super) fn origin_key(origin: &str) -> StorageResult<VerifyingKey> {
    let (public, generation) = origin
        .split_once(':')
        .ok_or_else(|| invalid("logical capture origin format differs"))?;
    let generation_id = uuid::Uuid::parse_str(generation)
        .map_err(|_| invalid("logical capture origin UUID differs"))?;
    if generation_id.is_nil() || generation_id.to_string() != generation {
        return Err(invalid("logical capture origin UUID is not canonical"));
    }
    VerifyingKey::from_bytes(&unhex::<32>(public)?)
        .map_err(|_| invalid("logical capture origin public key differs"))
}

pub(super) fn mutation_ids(ids: &[String]) -> StorageResult<()> {
    if ids.is_empty()
        || ids.len() > MAX_MUTATIONS
        || ids.iter().any(|id| id.is_empty() || id.len() > 128)
        || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
    {
        return Err(StorageError::MutationLimit {
            detail: "logical capture needs 1..=128 unique bounded mutation IDs".into(),
        });
    }
    Ok(())
}

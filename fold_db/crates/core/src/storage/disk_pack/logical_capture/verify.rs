//! Verify the exact signed logical representation, independent of native files.

use super::*;
use ed25519_dalek::{Signature, VerifyingKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

impl CaptureEvent {
    pub fn verify(&self) -> StorageResult<()> {
        bounded::serialized_size(self)?;
        if self.version != 1 || self.sequence == 0 {
            return Err(invalid("logical capture event version or sequence differs"));
        }
        mutation_ids(&self.mutation_ids)?;
        outcomes(&self.outcomes, &self.mutation_ids)?;
        unhex::<32>(&self.previous_sha256)?;
        let expected = unhex::<32>(&self.sha256)?;
        let signature = Signature::from_bytes(&unhex::<64>(&self.signature)?);
        let signer = origin_key(&self.origin)?;
        let bytes = self.signing_bytes()?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != expected {
            return Err(invalid("logical capture event hash differs"));
        }
        signer
            .verify_strict(&bytes, &signature)
            .map_err(|_| invalid("logical capture event signature differs"))
    }
}

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

pub(super) fn outcomes(
    entries: &[AcceptedFieldOutcome],
    mutation_ids: &[String],
) -> StorageResult<()> {
    if entries.len() > MAX_OUTCOMES {
        return Err(StorageError::MutationLimit {
            detail: "logical capture exceeds 1024 field outcomes".into(),
        });
    }
    if entries
        .iter()
        .any(|entry| !mutation_ids.contains(&entry.tip.mutation_uuid))
    {
        return Err(invalid("logical capture outcome has an unnamed mutation"));
    }
    Ok(())
}

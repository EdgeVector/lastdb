//! Verify the exact signed logical representation, independent of native files.

use super::*;
use ed25519_dalek::{Signature, VerifyingKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

impl CaptureEvent {
    pub fn verify(&self) -> StorageResult<()> {
        bounded::serialized_size(self)?;
        // Version 1 carries field outcomes only, version 2 record outcomes only.
        let shape = match self.version {
            1 => self.records.is_empty(),
            2 => self.outcomes.is_empty(),
            _ => false,
        };
        if !shape || self.sequence == 0 {
            return Err(invalid("logical capture event version or sequence differs"));
        }
        mutation_ids(&self.mutation_ids)?;
        outcomes(&self.outcomes, &self.mutation_ids)?;
        records(&self.records, &self.mutation_ids)?;
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

/// Each record outcome changes something, and each of its stamps names a
/// mutation of the event. One request is one record outcome for each key, and
/// the field outcomes of the event stay under the field cap of version 1.
pub(super) fn records(
    entries: &[AcceptedRecordOutcome],
    mutation_ids: &[String],
) -> StorageResult<()> {
    let fields: usize = entries.iter().map(|entry| entry.fields.len()).sum();
    if entries.len() > MAX_MUTATIONS || fields > MAX_OUTCOMES {
        return Err(StorageError::MutationLimit {
            detail: "logical capture exceeds 128 record or 1024 field outcomes".into(),
        });
    }
    if entries.iter().any(|entry| {
        (entry.fields.is_empty() && entry.meta.is_none() && entry.deleted.is_none())
            || entry
                .stamps()
                .any(|stamp| !mutation_ids.contains(&stamp.mutation_id))
    }) {
        return Err(invalid(
            "logical capture record outcome is empty or has an unnamed mutation",
        ));
    }
    Ok(())
}

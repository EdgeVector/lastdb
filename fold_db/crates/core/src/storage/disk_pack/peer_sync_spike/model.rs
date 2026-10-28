use super::*;
use crate::schema::types::MutationAuthorClockState;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use zeroize::Zeroize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub hash: String,
    pub range: String,
    /// None is a retained Delete, not a request to forget its winner metadata.
    pub value: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub sequence: u64,
    pub sha256: String,
}

/// An accepted batch. Its original identity survives import and snapshot adoption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub version: u8,
    pub origin: String,
    pub sequence: u64,
    pub previous_sha256: String,
    pub mutation_uuid: String,
    /// Change i uses written_at + i, preserving repeated-key order in a batch.
    pub written_at: u64,
    pub logical_counter: u64,
    pub changes: Vec<Change>,
    pub sha256: String,
    pub signature: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ImportReport {
    pub accepted: usize,
    pub duplicates: usize,
    pub changed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RangePage {
    pub items: Vec<Change>,
    /// Last visited range, including a retained Delete. None ends the range.
    pub next: Option<String>,
}

#[derive(Clone)]
pub struct Writer {
    pub origin: String,
    seed: [u8; 32],
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.seed.zeroize();
    }
}

impl Writer {
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self::with_generation(seed, &uuid::Uuid::new_v4().to_string())
            .expect("a generated UUID is canonical")
    }

    pub fn with_generation(seed: [u8; 32], generation: &str) -> StorageResult<Self> {
        let parsed =
            uuid::Uuid::parse_str(generation).map_err(|_| invalid("invalid writer generation"))?;
        if parsed.to_string() != generation {
            return Err(invalid("writer generation must use its canonical UUID"));
        }
        let key = SigningKey::from_bytes(&seed).verifying_key();
        Ok(Self {
            origin: format!("{}:{generation}", hex(key.as_bytes())),
            seed,
        })
    }

    pub(super) fn entry(
        &self,
        position: &Position,
        changes: Vec<Change>,
        clock: &mut MutationAuthorClockState,
    ) -> StorageResult<Entry> {
        validate_changes(&changes)?;
        let (times, logical_counter) = clock
            .advance_local_batch(crate::clock::unix_nanos(), changes.len())
            .ok_or_else(|| invalid("author clock exhausted"))?;
        let mut entry = Entry {
            version: 1,
            origin: self.origin.clone(),
            sequence: position
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("origin sequence exhausted"))?,
            previous_sha256: position.sha256.clone(),
            mutation_uuid: uuid::Uuid::new_v4().to_string(),
            written_at: *times.start(),
            logical_counter,
            changes,
            sha256: String::new(),
            signature: String::new(),
        };
        let bytes = entry.signing_bytes()?;
        entry.sha256 = hex(&Sha256::digest(&bytes));
        entry.signature = hex(&SigningKey::from_bytes(&self.seed).sign(&bytes).to_bytes());
        entry.verify()?;
        Ok(entry)
    }
}

impl Entry {
    fn signing_bytes(&self) -> StorageResult<Vec<u8>> {
        encode(&(
            "lastdb-peer-sync-spike-v1",
            self.version,
            &self.origin,
            self.sequence,
            &self.previous_sha256,
            &self.mutation_uuid,
            self.written_at,
            self.logical_counter,
            &self.changes,
        ))
    }

    pub fn verify(&self) -> StorageResult<()> {
        validate_changes(&self.changes)?;
        if encode(self)?.len() > MAX_ENTRY_BYTES {
            return Err(invalid("peer entry exceeds 256 KiB"));
        }
        if self.version != 1 || self.sequence == 0 || self.logical_counter == 0 {
            return Err(invalid("invalid peer entry version, sequence, or clock"));
        }
        self.last_written_at()?;
        let (key, generation) = self
            .origin
            .split_once(':')
            .ok_or_else(|| invalid("origin lacks a generation"))?;
        let generation = uuid::Uuid::parse_str(generation)
            .map_err(|_| invalid("origin generation is not a UUID"))?;
        if format!("{key}:{generation}") != self.origin {
            return Err(invalid("origin is not canonical"));
        }
        uuid::Uuid::parse_str(&self.mutation_uuid).map_err(|_| invalid("invalid mutation UUID"))?;
        if (self.sequence == 1) != self.previous_sha256.is_empty() {
            return Err(invalid("invalid initial entry hash anchor"));
        }
        if self.sequence > 1 {
            unhex::<32>(&self.previous_sha256)?;
        }
        let bytes = self.signing_bytes()?;
        if hex(&Sha256::digest(&bytes)) != self.sha256 {
            return Err(invalid("peer entry hash mismatch"));
        }
        let key = VerifyingKey::from_bytes(&unhex::<32>(key)?)
            .map_err(|_| invalid("invalid origin public key"))?;
        key.verify_strict(
            &bytes,
            &Signature::from_bytes(&unhex::<64>(&self.signature)?),
        )
        .map_err(|_| invalid("peer entry signature mismatch"))?;
        Ok(())
    }

    pub fn position(&self) -> Position {
        Position {
            sequence: self.sequence,
            sha256: self.sha256.clone(),
        }
    }

    pub(super) fn last_written_at(&self) -> StorageResult<u64> {
        self.written_at
            .checked_add(self.changes.len().saturating_sub(1) as u64)
            .ok_or_else(|| invalid("peer entry clock range exhausted"))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub(super) frontiers: BTreeMap<String, Position>,
    pub(super) clock: MutationAuthorClockState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Winner {
    pub(super) change: Change,
    pub(super) origin: String,
    pub(super) mutation_uuid: String,
    pub(super) written_at: u64,
    pub(super) logical_counter: u64,
    pub(super) entry_hash: String,
}

impl Winner {
    pub(super) fn from_entry(entry: &Entry, index: usize) -> Self {
        Self {
            change: entry.changes[index].clone(),
            origin: entry.origin.clone(),
            mutation_uuid: entry.mutation_uuid.clone(),
            written_at: entry.written_at + index as u64,
            logical_counter: entry.logical_counter,
            entry_hash: entry.sha256.clone(),
        }
    }

    pub(super) fn wins(&self, local: &Self) -> bool {
        crate::atom::incoming_wins_lww(self.order(), local.order())
    }

    fn order(&self) -> (u64, u64, &str, &str, &str) {
        let atom = if self.change.value.is_none() {
            "~"
        } else {
            &self.entry_hash
        };
        crate::atom::lww_order_key(
            self.written_at,
            self.logical_counter,
            &self.origin,
            &self.mutation_uuid,
            atom,
        )
    }
}

fn validate_changes(changes: &[Change]) -> StorageResult<()> {
    if changes.is_empty() || changes.len() > MAX_CHANGES {
        return Err(invalid("a peer batch needs 1..=128 changes"));
    }
    let mut total = 0usize;
    for change in changes {
        if change.hash.len() > 1024
            || change.range.len() > 1024
            || change
                .value
                .as_ref()
                .is_some_and(|value| value.len() > 64 * 1024)
        {
            return Err(invalid("peer key exceeds 1 KiB or value exceeds 64 KiB"));
        }
        total = total.saturating_add(
            change.hash.len() + change.range.len() + change.value.as_ref().map_or(0, String::len),
        );
        if total > MAX_ENTRY_BYTES {
            return Err(invalid("peer change contents exceed 256 KiB"));
        }
    }
    Ok(())
}

pub(super) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            write!(text, "{byte:02x}").expect("writing into a String");
            text
        })
}

pub(super) fn unhex<const N: usize>(text: &str) -> StorageResult<[u8; N]> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid canonical hexadecimal value"));
    }
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid hexadecimal value"))?;
    }
    Ok(bytes)
}

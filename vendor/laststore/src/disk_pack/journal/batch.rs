use super::super::bytes::*;
use super::super::{corrupt, record, Location, MutationVersion, ReadCosts};
use crate::Result;
use sha2::{Digest, Sha256};

/// One complete stored put or delete. Body bytes retain their existing encryption.
pub struct JournalMutation {
    /// Exact collection name, never a filesystem path.
    pub collection: String,
    /// Group selected by the held placement codec.
    pub group: u16,
    /// Full encoded storage key.
    pub key: String,
    /// Unchanged stored body, or None for a versioned tombstone.
    pub body: Option<Vec<u8>>,
}

/// One verified operation and its direct journal location.
pub struct RecoveredMutation {
    /// Original stored mutation bytes.
    pub mutation: JournalMutation,
    /// Direct extent, or tombstone, at this batch's exact version.
    pub location: Location,
}

/// A complete batch returned only after its COMMIT and every operation verify.
pub struct RecoveredBatch {
    /// Complete batch sequence.
    pub sequence: u64,
    /// Immediate predecessor sequence.
    pub previous: u64,
    /// Original accepted time from BEGIN and COMMIT.
    pub accepted_ms: u64,
    /// All operations in original order; never a successful partial batch.
    pub mutations: Vec<RecoveredMutation>,
    /// Complete physical data/map bytes discovered through exact paths.
    pub encoded_bytes: u64,
    /// SHA-256 of the final COMMIT marker.
    pub marker_digest: [u8; 32],
    /// Bounded metadata and payload work for this batch.
    pub costs: ReadCosts,
}

pub(super) struct BatchPlan {
    pub(super) logical_bytes: u64,
    pub(super) physical_reserve: u64,
}

impl BatchPlan {
    pub(super) fn new(operations: &[JournalMutation], operation_limit: usize) -> Result<Self> {
        let quote = super::JournalQuote::from_iter_with_limit(
            operations.iter().map(JournalMutation::shape),
            operation_limit,
        )?;
        Ok(Self {
            logical_bytes: quote.logical_bytes,
            physical_reserve: quote.physical_reserve,
        })
    }

    pub(super) fn reserve(length: u64) -> Result<u64> {
        let blocks = length.div_ceil(4096);
        let physical_reserve = length
            .checked_add(blocks * 80)
            .and_then(|n| n.checked_add(blocks.div_ceil(2008) * 128))
            .and_then(|n| n.checked_add(blocks.div_ceil(677) * 16_384))
            .ok_or_else(|| corrupt("journal space reserve overflow"))?;
        Ok(physical_reserve)
    }
}

impl JournalMutation {
    /// Complete logical operation bytes, including names, key, and record header.
    /// Callers can reserve a batch before they transfer its owned payloads.
    pub fn encoded_bytes(&self) -> Result<u64> {
        self.validate()?;
        self.operation_bytes()
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.shape().encoded_bytes().map(|_| ())
    }

    pub(super) fn operation_bytes(&self) -> Result<u64> {
        let prefix = 64u64 + self.collection.len() as u64 + self.key.len() as u64;
        prefix
            .checked_add(self.payload_bytes())
            .ok_or_else(|| corrupt("operation length overflow"))
    }

    pub(super) fn payload_bytes(&self) -> u64 {
        self.body
            .as_ref()
            .map_or(0, |body| 96 + self.key.len() as u64 + body.len() as u64)
    }

    pub(super) fn encode(&self, sequence: u64, number: u32) -> Result<EncodedOperation> {
        self.validate()?;
        let record = self
            .body
            .as_ref()
            .map(|body| {
                record::record_header(
                    &self.collection,
                    &self.key,
                    body.len() as u64,
                    MutationVersion {
                        batch: sequence,
                        operation: number,
                    },
                )
            })
            .transpose()?;
        let mut header = [0; 64];
        header[0] = if record.is_some() { 1 } else { 2 };
        put16(&mut header, 2, self.group);
        put32(&mut header, 4, number);
        put32(&mut header, 8, self.collection.len() as u32);
        put32(&mut header, 12, self.key.len() as u32);
        put64(&mut header, 16, self.payload_bytes());
        put64(&mut header, 24, self.operation_bytes()?);
        let mut digest = Sha256::new();
        digest.update(header);
        self.hash_suffix(&mut digest, record.as_ref());
        header[32..64].copy_from_slice(&digest.finalize());
        Ok(EncodedOperation { header, record })
    }

    pub(super) fn hash_suffix(&self, hash: &mut Sha256, record: Option<&[u8; 96]>) {
        hash.update(self.collection.as_bytes());
        hash.update(self.key.as_bytes());
        if let Some(record) = record {
            hash.update(record);
            hash.update(self.key.as_bytes());
            hash.update(self.body.as_ref().expect("put record body"));
        }
    }
}

pub(super) struct EncodedOperation {
    pub(super) header: [u8; 64],
    pub(super) record: Option<[u8; 96]>,
}

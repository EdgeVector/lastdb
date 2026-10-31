//! Durable per-device mutation author clocks.

use sha2::{Digest, Sha256};

use super::Mutation;

/// Mutation author signature scheme that binds content, clock, and identity.
pub const MUTATION_AUTHOR_SIGNATURE_VERSION: u8 = 3;

/// Durable high-water state for one device's mutation author clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationAuthorClockState {
    /// Highest physical nanosecond value emitted, observed, or reserved by this device.
    #[serde(default)]
    pub physical_nanos: u64,
    /// Monotonic logical counter. It advances for every local operation.
    #[serde(default)]
    pub logical_counter: u64,
}

impl MutationAuthorClockState {
    /// Reserve consecutive local write times under one batch revision. Each
    /// mutation keeps the same counter, but later items outrank earlier items
    /// without using their UUIDs. Refuse an exhausted range before changing
    /// this state; the durable high-water includes its final timestamp.
    pub fn advance_local_batch(
        &mut self,
        now_nanos: u64,
        count: usize,
    ) -> Option<(std::ops::RangeInclusive<u64>, u64)> {
        let additional = u64::try_from(count.checked_sub(1)?).ok()?;
        let mut state = *self;
        let (first, counter) = state.advance_local(now_nanos)?;
        let last = first.checked_add(additional)?;
        state.physical_nanos = last;
        *self = state;
        Some((first..=last, counter))
    }

    /// Advance one local event. A wall-clock rollback cannot reduce either
    /// component of the durable author clock.
    #[must_use]
    pub fn advance_local(&mut self, now_nanos: u64) -> Option<(u64, u64)> {
        let counter = self.logical_counter.checked_add(1)?;
        self.physical_nanos = self.physical_nanos.max(now_nanos);
        self.logical_counter = counter;
        Some((self.physical_nanos, self.logical_counter))
    }

    /// Observe a remote author clock without changing the received mutation.
    pub fn observe_remote(&mut self, physical_nanos: u64, logical_counter: u64) {
        self.physical_nanos = self.physical_nanos.max(physical_nanos);
        self.logical_counter = self.logical_counter.max(logical_counter);
    }
}

/// Exact metadata key for one device's durable author clock state.
#[must_use]
pub fn mutation_author_clock_key(device_id: &str) -> String {
    let digest = Sha256::digest(device_id.as_bytes());
    format!("mutation_author_clock:{digest:x}")
}

/// The exact bytes that a mutation author signature signs: the framed domain,
/// content hash, mutation id, writer id, write time, and logical counter.
/// Core signs and verifies these bytes; the model holds no key.
#[must_use]
pub fn mutation_author_canonical_bytes(mutation: &Mutation) -> Vec<u8> {
    let written_at = mutation.imported_written_at.unwrap_or(0);
    let content_hash = mutation.content_hash();
    crate::canonical::CanonicalWriter::with_capacity(
        content_hash.len() + mutation.uuid.len() + mutation.author_clock_writer_id.len() + 32,
    )
    .field(b"lastdb-mutation-author-v3")
    .field(content_hash.as_bytes())
    .field(mutation.uuid.as_bytes())
    .field(mutation.author_clock_writer_id.as_bytes())
    .u64(written_at)
    .u64(mutation.logical_counter)
    .finish()
}

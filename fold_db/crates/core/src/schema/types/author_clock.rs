//! Durable per-device mutation author clocks.

use sha2::{Digest, Sha256};

use super::Mutation;

/// Mutation author signature scheme that binds content, clock, and identity.
pub const MUTATION_AUTHOR_SIGNATURE_VERSION: u8 = 2;

/// Durable high-water state for one device's mutation author clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationAuthorClockState {
    /// Highest physical nanosecond value emitted or observed by this device.
    #[serde(default)]
    pub physical_nanos: u64,
    /// Monotonic logical counter. It advances for every local operation.
    #[serde(default)]
    pub logical_counter: u64,
}

impl MutationAuthorClockState {
    /// Advance one local event. A wall-clock rollback cannot reduce either
    /// component of the durable author clock.
    #[must_use]
    pub fn advance_local(&mut self, now_nanos: u64) -> (u64, u64) {
        self.physical_nanos = self.physical_nanos.max(now_nanos);
        self.logical_counter = self.logical_counter.saturating_add(1);
        (self.physical_nanos, self.logical_counter)
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

fn canonical_bytes(mutation: &Mutation) -> Vec<u8> {
    let written_at = mutation.imported_written_at.unwrap_or(0);
    crate::canonical::CanonicalWriter::with_capacity(
        mutation.content_hash().len()
            + mutation.uuid.len()
            + mutation.author_clock_writer_id.len()
            + 32,
    )
    .field(b"lastdb-mutation-author-v2")
    .field(mutation.content_hash().as_bytes())
    .field(mutation.uuid.as_bytes())
    .field(mutation.author_clock_writer_id.as_bytes())
    .u64(written_at)
    .u64(mutation.logical_counter)
    .finish()
}

/// Sign a fully stamped local mutation.
#[must_use]
pub fn sign_mutation_author_clock(
    mutation: &Mutation,
    keypair: &crate::security::Ed25519KeyPair,
) -> String {
    let signature = keypair.sign(&canonical_bytes(mutation));
    crate::security::KeyUtils::signature_to_base64(&signature)
}

/// Verify the mutation author signature.
///
/// Version zero is the legacy envelope contract. It is valid only when the
/// additive clock fields are also zero or empty. Existing molecule signature
/// version one remains independent and unchanged.
#[must_use]
pub fn verify_mutation_author_clock(mutation: &Mutation) -> bool {
    match mutation.author_clock_signature_version {
        0 => mutation.logical_counter == 0 && mutation.author_clock_signature.is_empty(),
        MUTATION_AUTHOR_SIGNATURE_VERSION => {
            !mutation.author_clock_signature.is_empty()
                && !mutation.author_clock_writer_id.is_empty()
                && crate::security::verify_molecule_signature(
                    &canonical_bytes(mutation),
                    &mutation.author_clock_signature,
                    &mutation.author_clock_writer_id,
                )
        }
        _ => false,
    }
}

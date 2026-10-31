//! Sign and verify the mutation author clock.
//!
//! The model (`lastdb_model::schema::types::author_clock`) defines the clock
//! state and the exact signed bytes. Core holds the key and runs the Ed25519
//! operations.

use crate::schema::types::author_clock::{
    mutation_author_canonical_bytes, MUTATION_AUTHOR_SIGNATURE_VERSION,
};
use crate::schema::types::Mutation;
use crate::security::{verify_molecule_signature, Ed25519KeyPair, KeyUtils};

/// Sign a fully stamped local mutation.
#[must_use]
pub fn sign_mutation_author_clock(mutation: &Mutation, keypair: &Ed25519KeyPair) -> String {
    let signature = keypair.sign(&mutation_author_canonical_bytes(mutation));
    KeyUtils::signature_to_base64(&signature)
}

/// Verify the mutation author signature.
///
/// Version zero is the independent unsigned-input contract: it carries no
/// logical counter or author signature. Signed input uses only the current
/// domain and framed content hash; older signature versions receive a refusal.
/// Molecule signature version one remains independent and unchanged.
#[must_use]
pub fn verify_mutation_author_clock(mutation: &Mutation) -> bool {
    match mutation.author_clock_signature_version {
        0 => mutation.logical_counter == 0 && mutation.author_clock_signature.is_empty(),
        MUTATION_AUTHOR_SIGNATURE_VERSION => {
            !mutation.author_clock_signature.is_empty()
                && !mutation.author_clock_writer_id.is_empty()
                && verify_molecule_signature(
                    &mutation_author_canonical_bytes(mutation),
                    &mutation.author_clock_signature,
                    &mutation.author_clock_writer_id,
                )
        }
        _ => false,
    }
}

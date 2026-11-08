//! Sign the mutation author clock.
//!
//! The model (`lastdb_model::schema::types::author_clock`) defines the clock
//! state and the exact signed bytes. Core holds the key and runs the Ed25519
//! operation.

use crate::schema::types::author_clock::mutation_author_canonical_bytes;
use crate::schema::types::Mutation;
use crate::security::{Ed25519KeyPair, KeyUtils};

/// Sign a fully stamped local mutation.
#[must_use]
pub fn sign_mutation_author_clock(mutation: &Mutation, keypair: &Ed25519KeyPair) -> String {
    let signature = keypair.sign(&mutation_author_canonical_bytes(mutation));
    KeyUtils::signature_to_base64(&signature)
}

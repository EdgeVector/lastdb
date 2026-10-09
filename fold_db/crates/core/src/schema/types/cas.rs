//! Compare-and-set (CAS) precondition for the node write path.
//!
//! A [`Mutation`](super::mutation::Mutation) may carry an optional
//! [`CasExpectation`]. When present, the node applies the mutation **iff**
//! the current state at the mutation's key matches the expectation, atomically
//! with respect to other writes to the same key on this node; otherwise it
//! returns [`SchemaError::CasConflict`](super::errors::SchemaError::CasConflict)
//! and writes nothing.
//!
//! Scope is **same-node atomicity only** — this is the primitive that lets a
//! check-then-set client (e.g. a lastgit ref update: "advance `refs/heads/main`
//! from `<old-sha>` to `<new-sha>` only if it still points at `<old-sha>`")
//! reject a losing writer on a single node instead of silently last-write-wins.
//! Cross-node divergence is handled elsewhere (append-only event schemas +
//! conflict records at the app layer).
//!
//! ## Two expectation shapes
//!
//! The card (lastgit review F4) calls for "field value or record content hash".
//! Both are expressed here, plus the create-only "must not exist yet" case a ref
//! *creation* needs:
//!
//! - [`CasExpectation::Value`] — expect a named field to currently hold exactly
//!   this JSON value. Natural for a ref update keyed on the ref's current target
//!   SHA. This is compared against the live molecule head's *value*, so it is
//!   robust to atom-uuid representation and reads naturally at the call site.
//! - [`CasExpectation::ContentHash`] — expect the record's current content hash
//!   (the atom UUID at the mutation's key for the CAS-guarded field) to equal
//!   this string. The whole-record fingerprint form.
//! - [`CasExpectation::Absent`] — expect NO live value at the key yet
//!   (create-if-absent). A tombstoned key counts as absent, matching delete +
//!   recreate semantics.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The precondition a CAS mutation asserts about the current state at its key.
///
/// Serde uses an internally-tagged representation (`{"type": "...", ...}`) so
/// the wire form is self-describing and forward-compatible with new shapes.
/// The field is additive on [`Mutation`](super::mutation::Mutation): absent on
/// every mutation written before CAS existed, and
/// `#[serde(skip_serializing_if = "Option::is_none")]` keeps their serialized
/// bytes byte-for-byte unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CasExpectation {
    /// The named field must currently resolve to exactly `value` at the
    /// mutation's key. A missing / tombstoned value never matches a
    /// `Value` expectation (use [`CasExpectation::Absent`] for the
    /// create-if-absent case).
    Value {
        /// The field whose current value is compared.
        field: String,
        /// The value that field must currently hold for the write to apply.
        value: Value,
    },
    /// The record's current content hash — the atom UUID that the CAS-guarded
    /// `field`'s molecule head resolves to at the mutation's key — must equal
    /// `hash`. A missing / tombstoned head never matches.
    ContentHash {
        /// The field whose current head atom UUID is compared.
        field: String,
        /// The atom UUID the field's head must currently be.
        hash: String,
    },
    /// No live value may exist yet at the mutation's key for `field`
    /// (create-if-absent). A tombstoned head counts as absent.
    Absent {
        /// The field that must currently have no live value.
        field: String,
    },
}

impl CasExpectation {
    /// The field this expectation is asserted against. Every shape names one
    /// field so the write path knows which molecule head to read for the
    /// compare.
    #[must_use]
    pub fn field(&self) -> &str {
        match self {
            Self::Value { field, .. }
            | Self::ContentHash { field, .. }
            | Self::Absent { field } => field,
        }
    }
}

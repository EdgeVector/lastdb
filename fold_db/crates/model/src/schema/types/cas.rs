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
//! ## Expectation shapes
//!
//! - [`CasExpectation::Mutation`] — expect a named field to still hold the
//!   value that one authored mutation wrote.
//! - [`CasExpectation::Value`] — expect a named field to currently hold exactly
//!   this JSON value. Natural for a ref update keyed on the ref's current target
//!   SHA. This is compared against the field's value in the stored record.
//! - [`CasExpectation::Absent`] — expect NO live value at the key yet
//!   (create-if-absent). A deleted record counts as absent, matching delete +
//!   recreate semantics.
//!
//! The content hash shape was dropped with the record model (Tom,
//! 2026-11-01): a record has no record whose identity could be compared.

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
    /// The current field must still be the exact authored write selected by the caller.
    Mutation { field: String, uuid: String },
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
    /// No live value may exist yet at the mutation's key for `field`
    /// (create-if-absent). A tombstoned head counts as absent.
    Absent {
        /// The field that must currently have no live value.
        field: String,
    },
}

impl CasExpectation {
    /// The field this expectation is asserted against. Every shape names one
    /// field so the write path knows which record field to compare.
    #[must_use]
    pub fn field(&self) -> &str {
        match self {
            Self::Mutation { field, .. } | Self::Value { field, .. } | Self::Absent { field } => {
                field
            }
        }
    }
}

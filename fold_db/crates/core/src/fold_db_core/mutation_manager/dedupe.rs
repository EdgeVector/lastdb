//! Per-molecule write dedupe — drop field writes whose value is already the
//! molecule's current tip.
//!
//! ## Why
//!
//! One logical record is one molecule per field (Schema → Molecule → Atom), so
//! write cost scales with the number of fields **sent**, not the number
//! **changed**. Measured on the live primary against `BoardCards_hashrange_v1`
//! (24 fields), 2026-07-31:
//!
//! | update | ms | share |
//! |---|---|---|
//! | 24 fields, all changed | 5376 | 100% |
//! | 24 fields sent, **2** changed | 4695 | **87%** |
//! | 4 fields sent, 2 changed | 1197 | 22% |
//!
//! Twenty-two of those twenty-four values were byte-identical to what was
//! already stored and the node still paid for them: a tip advance, a durable
//! molecule store, a protein sibling fold, and a `tv:` history node — per
//! unchanged field.
//!
//! The node already short-circuits the case where the **whole** mutation is a
//! repeat (`cas::filter_idempotent_mutations`, 140 ms — a 38x skip), but that
//! gate is content-hash-on-the-whole-mutation by construction: change one field
//! of twenty-four and the hash misses, and every field pays full freight. This
//! module makes that same "the stored value is already what you're asking for"
//! judgement **per molecule** instead of per mutation.
//!
//! ## What it does
//!
//! After molecule heads are restored (so tips are readable) and before the
//! in-memory apply, drop every prepared atom write whose content-addressed atom
//! uuid already equals the current tip at that key. Dropping the entry removes
//! it from the apply loop *and* the sibling fold — both iterate `atom_results`
//! — and it never reaches `modified_fields`, so the persist step does not write
//! that molecule either.
//!
//! Nothing else shrinks: mutation ids, idempotency records and secondary-index
//! updates all key off the untouched mutation list, so a fully-deduped write is
//! still acknowledged, still recorded as seen, and still re-indexed.
//!
//! ## Why it is opt-in (`LASTDB_WRITE_DEDUPE`)
//!
//! Skipping the tip advance means the slot keeps the `written_at` of the last
//! write that actually **changed** the value. Cross-device conflict resolution
//! is per-slot last-writer-wins on the author-clock key
//! `(written_at, logical_counter, device_id, mutation_uuid, atom_uuid)`
//! (`sync::engine::replay::records`), so this changes one observable behaviour:
//!
//! > Device A sets `V` at t1. Device C sets `W` at t2 > t1. Device A re-sends
//! > the identical `V` at t3 > t2.
//! >
//! > Today A's no-op re-send refreshes the slot to t3 and `V` beats `W`.
//! > With dedupe the slot stays at t1 and `W` — the only real change — wins.
//!
//! The dedupe outcome is arguably the more defensible rule (a write that
//! changed nothing should not win a conflict against one that did), but it is a
//! convergence-semantics change and not one to flip on unattended. Default off;
//! `LASTDB_WRITE_DEDUPE=1` enables. See
//! `brain get lastdb-unchanged-value-skip-is-whole-record-not-per-molecule`.
//!
//! ## What is never deduped
//!
//! [`mutation_is_dedupe_eligible`] holds back every mutation whose write
//! carries meaning beyond its value:
//!
//! - **`provenance` / `imported_written_at` / `imported_version`** — inbound
//!   `data_share` replay. The molecule's canonical bytes include the *signed*
//!   `written_at`; skipping the write would drop the original author's
//!   attribution and leave the signature unverifiable at rest.
//! - **`Delete`** — tombstones interact with the purge reachability snapshot
//!   and the Create→Delete→Create resurrect path. Not worth the blast radius
//!   for a verb that is not on the hot path.
//! - **`expected` (CAS)** — a CAS precondition is a statement about *when* to
//!   apply. A caller that passed a precondition and got success is entitled to
//!   assume a commit happened.

use crate::atom::Atom;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};

use super::helpers::current_atom_uuid;
use super::MutationManager;

/// Env: enable per-molecule write dedupe. Off unless truthy.
pub const WRITE_DEDUPE_ENV: &str = "LASTDB_WRITE_DEDUPE";

/// `LASTDB_WRITE_DEDUPE` truthy → drop unchanged field writes.
///
/// Same truthy vocabulary as [`env_flag`] (`1` / `true` / `yes` / `on`,
/// case-insensitive). Absent or unparseable → `false`, so the default write
/// path is byte-for-byte unchanged.
#[must_use]
pub fn parse_write_dedupe(raw: Option<String>) -> bool {
    raw.is_some_and(|s| env_flag::truthy(&s))
}

/// Whether this mutation's field writes may be skipped when their value already
/// matches the stored tip. See the module docs for why each exclusion exists.
#[must_use]
pub fn mutation_is_dedupe_eligible(mutation: &Mutation) -> bool {
    mutation.provenance.is_none()
        && mutation.imported_written_at.is_none()
        && mutation.imported_version.is_none()
        && mutation.expected.is_none()
        && mutation.mutation_type != MutationType::Delete
}

impl MutationManager {
    /// True when this process runs the write path with per-molecule dedupe.
    #[inline]
    pub(crate) fn write_dedupe_enabled(&self) -> bool {
        self.write_dedupe
    }

    /// Drop prepared atom writes whose value is already the current tip.
    ///
    /// Returns the retained entries. Must run **after**
    /// `restore_missing_molecules` — an unrestored molecule reads back no tip,
    /// which would make every field look changed and silently disable the
    /// optimisation rather than corrupt anything.
    ///
    /// A field that is not a schema runtime field (sibling partition keys ride
    /// along on the mutation payload) is always retained; the apply and fold
    /// loops already skip those on their own, and this pass has no business
    /// making that judgement a second time.
    pub(in crate::fold_db_core::mutation_manager) fn drop_unchanged_field_writes(
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: Vec<(usize, String, Atom)>,
    ) -> Vec<(usize, String, Atom)> {
        atom_results
            .into_iter()
            .filter(|(idx, field_name, atom)| {
                let Some(mutation) = schema_mutations.get(*idx) else {
                    return true;
                };
                if !mutation_is_dedupe_eligible(mutation) {
                    return true;
                }
                let Some(field) = schema.runtime_fields.get(field_name) else {
                    return true;
                };
                let Some(key_value) = mutation_key_values.get(*idx) else {
                    return true;
                };
                // Atom uuids are content-addressed over
                // `(schema_name, content, source_file_name, metadata)`, so uuid
                // equality here IS value equality — no separate compare.
                let unchanged = current_atom_uuid(field, key_value).as_deref() == Some(atom.uuid());
                if unchanged {
                    tracing::trace!(
                        target: "write_dedupe",
                        field = %field_name,
                        atom = %atom.uuid(),
                        "skipping unchanged field write"
                    );
                }
                !unchanged
            })
            .collect()
    }
}

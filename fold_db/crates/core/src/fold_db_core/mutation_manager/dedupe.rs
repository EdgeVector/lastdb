//! Optional per-field skip of writes whose atom identity matches the current tip.
//!
//! This runs after exact tips load and before logical preparation. Removing an
//! atom result also removes its tip and related-molecule work. Mutation IDs,
//! idempotency records, and Search delivery still describe the original request.
//!
//! The option is off by default. An unchanged field then retains its previous
//! tip time rather than advancing its conflict order. Writes with provenance,
//! imported clocks, or a compare condition always retain their tip updates.
//! Repeated writes to the same physical slot within a batch also stay in order;
//! comparing each one with the pre-batch tip could otherwise discard the final
//! value in an A -> B -> A sequence. Delete uses its separate tip-removal route.

use crate::atom::Atom;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};

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
        // Every occurrence of a repeated physical slot stays in order. A
        // baseline A followed by B -> A must not drop the final A merely
        // because it equals the baseline before B applies.
        let slot_for = |idx: usize, name: &str| {
            let field = schema.runtime_fields.get(name)?;
            let (hash, range) = field.slot_coordinates(mutation_key_values.get(idx)?)?;
            Some((field.common().molecule_uuid()?.clone(), hash, range))
        };
        let mut slot_counts = std::collections::HashMap::new();
        for (idx, name, _) in &atom_results {
            if let Some(slot) = slot_for(*idx, name) {
                *slot_counts.entry(slot).or_insert(0usize) += 1;
            }
        }
        atom_results
            .into_iter()
            .filter(|(idx, field_name, atom)| {
                if slot_for(*idx, field_name)
                    .and_then(|slot| slot_counts.get(&slot))
                    .is_some_and(|count| *count > 1)
                {
                    return true;
                }
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
                let unchanged = field.current_atom_uuid(key_value).as_deref() == Some(atom.uuid());
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

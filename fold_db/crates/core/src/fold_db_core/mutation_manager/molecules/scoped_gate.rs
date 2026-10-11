//! Scoped identities stay in their exact durable lane until resident scope exists.

use super::super::MutationManager;
use super::apply_gate::{ApplyGateOutcome, PurgeApplyOutcome};
use crate::atom::Atom;
use crate::schema::types::{KeyValue, Mutation, Schema};

pub(super) fn schema_is_scoped(schema: &Schema) -> bool {
    schema
        .runtime_fields
        .values()
        .any(|field| field.common().storage_prefix().is_some())
}

impl MutationManager {
    pub(super) fn scoped_apply_gate(
        &self,
        schema: &mut Schema,
        mutations: &[Mutation],
        key_values: &[KeyValue],
        atoms: Vec<(usize, String, Atom)>,
    ) -> ApplyGateOutcome {
        let (mutation_events, modified_fields) =
            self.apply_mutations_to_molecules(schema, mutations, key_values, atoms);
        ApplyGateOutcome {
            mutation_events,
            modified_fields,
            slot_revisions: Vec::new(),
        }
    }
}

impl PurgeApplyOutcome {
    pub(super) fn scoped() -> Self {
        Self {
            schema_tombstones: Vec::new(),
            slots: Vec::new(),
            slot_revisions: Vec::new(),
        }
    }
}

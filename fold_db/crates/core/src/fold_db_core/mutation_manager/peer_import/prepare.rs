//! Prepare original peer tips; the normal store owns winner and counter facts.

use super::*;
use crate::atom::MoleculeClock;
use crate::atom::{ImportedTip, MoleculeHashRange};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use std::collections::HashSet;

impl MutationManager {
    pub(super) async fn prepare_peer_job(
        &self,
        name: &str,
        outcomes: &[&AcceptedFieldOutcome],
        origin: &str,
    ) -> Result<DeferredPersistJob, SchemaError> {
        let mut schema = self.load_schema_for_delta(name, None).await?;
        let modified_fields = peer_changed_keys(&schema, outcomes)?;
        self.reload_changed_for_prepare(&mut schema, &modified_fields)
            .await?;
        for outcome in outcomes {
            install_peer_tip(&mut schema, outcome, origin)?;
        }
        let mut seen = HashSet::new();
        let keys: Vec<_> = outcomes
            .iter()
            .map(|outcome| &outcome.key)
            .filter(|key| seen.insert((**key).clone()))
            .cloned()
            .collect();
        let tracks_age = schema.schema_type == DeclarativeSchemaType::Hash;
        let tracks_hashes = schema.schema_type == DeclarativeSchemaType::HashRange;
        Ok(DeferredPersistJob {
            deferred_atoms: Some(outcomes.iter().map(|fact| fact.atom.clone()).collect()),
            storage_prefix: None,
            search_batch: None,
            share_prefixes: Vec::new(),
            schema,
            modified_fields,
            idempotency_entries: Vec::new(),
            retention_write: tracks_age.then(|| keys.clone()),
            retention_hash_partitions: tracks_hashes.then_some(keys),
            schema_name: name.to_string(),
        })
    }
}

fn peer_changed_keys(
    schema: &Schema,
    outcomes: &[&AcceptedFieldOutcome],
) -> Result<ModifiedFieldKeys, SchemaError> {
    let mut changed = ModifiedFieldKeys::new();
    for outcome in outcomes {
        let key = schema
            .runtime_fields
            .get(&outcome.field_name)
            .and_then(|field| field.changed_key_for(&outcome.key))
            .ok_or_else(|| unsupported("an outcome without an exact field slot"))?;
        changed
            .entry(outcome.field_name.clone())
            .or_default()
            .insert(key);
    }
    Ok(changed)
}

fn install_peer_tip(
    schema: &mut Schema,
    outcome: &AcceptedFieldOutcome,
    origin: &str,
) -> Result<(), SchemaError> {
    let field = schema
        .runtime_fields
        .get_mut(&outcome.field_name)
        .ok_or_else(|| unsupported("a missing prepared field"))?;
    let (hash, range) = field
        .slot_coordinates(&outcome.key)
        .ok_or_else(|| unsupported("missing prepared field coordinates"))?;
    if !field.has_molecule() {
        field.set_molecule_data(MoleculeHashRange::with_uuid(outcome.molecule_uuid.clone()));
    }
    let molecule = field
        .molecule_data_mut()
        .ok_or_else(|| unsupported("a missing prepared molecule"))?;
    let mut tip = outcome.tip.clone();
    // Some local authors predate a sync-router identity. The event's dedicated
    // authenticated device identity supplies only that missing conflict label;
    // it is deliberately absent from the LWW order.
    if tip.origin_device.is_empty() {
        tip.origin_device = origin.to_string();
    }
    let accepted = molecule.set_atom_uuid_from_values_imported_with_author(
        hash.clone(),
        range.clone(),
        ImportedTip {
            atom_uuid: tip.atom_uuid.clone(),
            device_id: tip.device_id.clone(),
            origin_device: tip.origin_device.clone(),
            writer_pubkey: tip.writer_pubkey().to_string(),
            signature: tip.signature().to_string(),
            signature_version: tip.signature_version(),
            written_at: Some(tip.written_at),
            logical_counter: tip.logical_counter,
            mutation_uuid: tip.mutation_uuid.clone(),
        },
    );
    if accepted {
        molecule.overlay_resident_tip(hash, range, tip, outcome.metadata.clone());
    }
    Ok(())
}

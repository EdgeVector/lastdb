//! Encode / decode the live mutation-log envelope: LastDB mutation intent,
//! not physical KV fanout.
//!
//! The serving write path uses `encode_mutations` and `LogOp::MutationIntent`.
//! Its payload contains the schema, key, fields, and signed write metadata.

use crate::atom::Atom;
use crate::schema::types::operations::MutationType;
use crate::schema::types::Mutation;
use crate::sync::log::MutationEnvelope;
use std::collections::HashMap;

/// Serialize mutations into the live log envelope. `storage_prefix` is the
/// org/db scope the write used (`None` = personal).
///
/// Stamps origin LWW identity: `imported_written_at` when the write already
/// carries one, otherwise now. Replay must reuse these fields, not mint now.
pub fn encode_mutations(
    mutations: &[Mutation],
    storage_prefix: Option<&str>,
) -> Vec<MutationEnvelope> {
    mutations
        .iter()
        .map(|mutation| MutationEnvelope {
            schema_name: mutation.schema_name.clone(),
            mutation_type: mutation_type_name(&mutation.mutation_type).to_string(),
            key_value: mutation.key_value.clone(),
            fields_and_values: mutation.fields_and_values.clone(),
            pub_key: mutation.pub_key.clone(),
            written_at: mutation
                .imported_written_at
                .unwrap_or_else(crate::clock::unix_nanos),
            writer_id: if mutation.author_clock_writer_id.is_empty() {
                mutation.pub_key.clone()
            } else {
                mutation.author_clock_writer_id.clone()
            },
            logical_counter: mutation.logical_counter,
            author_clock_signature: mutation.author_clock_signature.clone(),
            author_clock_signature_version: mutation.author_clock_signature_version,
            storage_prefix: storage_prefix
                .filter(|prefix| !prefix.is_empty())
                .map(str::to_string),
            provenance: mutation.provenance.clone(),
            imported_version: mutation.imported_version,
            mutation_uuid: mutation.uuid.clone(),
            source_file_name: mutation.source_file_name.clone(),
            metadata: mutation.metadata.clone(),
            field_atom_uuids: field_atom_uuids_for(mutation),
            aggregate_set: mutation.aggregate_set.clone(),
            erase_tips_before_nanos: mutation.erase_tips_before_nanos,
        })
        .collect()
}

/// Same content-addressed id `AtomStore::create_atom` will persist.
fn field_atom_uuids_for(mutation: &Mutation) -> HashMap<String, String> {
    mutation
        .fields_and_values
        .iter()
        .map(|(field, value)| {
            (
                field.clone(),
                Atom::new(mutation.schema_name.clone(), value.clone())
                    .uuid()
                    .to_string(),
            )
        })
        .collect()
}

/// Drop atom ids for fields the write pipeline will not persist an atom for.
///
/// `field_atom_uuids_for` mints a content-addressed id for **every** entry of
/// `fields_and_values`, but the write pipeline only creates an atom for a
/// field the schema declares (`prepare_atoms_and_key_values` skips anything
/// outside `runtime_fields` — extra keys ride along for fold field maps and
/// get no body). A hard erasure creates no atoms at all: `Delete` and `Purge`
/// are peeled out of the pipeline and hard-erased.
///
/// That gap is not cosmetic, because an id is what authorizes deletion of the
/// value. `MutationEnvelope::strip_sot_field_values` clears the inline bodies
/// once every field carries an id, and the seal then has to read each id back
/// out of the atom plane. An id nothing ever persisted makes the record
/// unsealable forever, and the upload path drops an unsealable record — so a
/// field the writer never intended to store takes the whole record's cloud
/// copy with it.
///
/// `persisted_fields` returns the schema's declared field set, or `None` when
/// the schema cannot be resolved. Both the `None` case and an erasure clear
/// the map entirely, which leaves `strip_sot_field_values` a no-op and keeps
/// the bodies inline: a larger log entry, never a lost value.
///
/// Measured on the primary before this shipped: 585 records dropped in 46 h
/// over 284 atom ids that no atom was ever written for. Record
/// `papercut-lastdb-capture-mints-atom-ids-for-fields-the-write-path-never-stores`.
pub fn retain_persisted_field_atom_uuids<F>(
    envelopes: &mut [MutationEnvelope],
    mutations: &[Mutation],
    persisted_fields: F,
) where
    F: Fn(&str) -> Option<std::collections::HashSet<String>>,
{
    for (envelope, mutation) in envelopes.iter_mut().zip(mutations) {
        if envelope.field_atom_uuids.is_empty() {
            continue;
        }
        if matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        ) {
            envelope.field_atom_uuids.clear();
            continue;
        }
        match persisted_fields(&mutation.schema_name) {
            Some(fields) => envelope
                .field_atom_uuids
                .retain(|field, _| fields.contains(field)),
            None => envelope.field_atom_uuids.clear(),
        }
    }
}

fn mutation_type_name(mutation_type: &MutationType) -> &'static str {
    match mutation_type {
        MutationType::Create => "create",
        MutationType::Update => "update",
        MutationType::Delete => "delete",
        MutationType::Purge => "purge",
    }
}

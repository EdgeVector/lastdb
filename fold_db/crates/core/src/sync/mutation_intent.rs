//! Encode / decode the live mutation-log envelope: LastDB mutation intent,
//! not physical KV fanout.
//!
//! Tests in this module drive the same encoder the serving write path uses
//! (`encode_mutations` → `LogOp::MutationIntent`) so payload size is a
//! property of schema + key + fields.

use crate::atom::Atom;
use crate::schema::types::operations::MutationType;
use crate::schema::types::Mutation;
use crate::sync::log::{LogOp, MutationEnvelope};
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

/// Rebuild mutations from a captured envelope. Returns the shared storage
/// prefix (first non-empty) so replay can apply under the same org scope.
///
/// Sets `imported_written_at` so apply uses the origin LWW clock.
pub fn decode_mutations(envelopes: &[MutationEnvelope]) -> (Vec<Mutation>, Option<String>) {
    let prefix = envelopes
        .iter()
        .find_map(|envelope| envelope.storage_prefix.clone());
    let mut mutations: Vec<Mutation> = envelopes
        .iter()
        .filter_map(|envelope| {
            let mutation_type = parse_mutation_type(&envelope.mutation_type)?;
            let mut mutation = Mutation::new(
                envelope.schema_name.clone(),
                envelope.fields_and_values.clone(),
                envelope.key_value.clone(),
                envelope.pub_key.clone(),
                mutation_type,
            );
            // Keep Some(0) for pre-clock #1556 rows so apply can last-write
            // among zeros and still LWW-skip against a real origin tip.
            mutation.imported_written_at = Some(envelope.written_at);
            // Keep original UUID absence for source history. The replay UUID
            // below remains unchanged for the existing legacy LWW rules.
            mutation.replayed_source_mutation_uuid = Some(envelope.mutation_uuid.clone());
            mutation.logical_counter = envelope.logical_counter;
            mutation.author_clock_writer_id = envelope.writer_id.clone();
            mutation.author_clock_signature = envelope.author_clock_signature.clone();
            mutation.author_clock_signature_version = envelope.author_clock_signature_version;
            mutation.imported_version = envelope.imported_version;
            mutation.provenance = envelope.provenance.clone();
            if !envelope.mutation_uuid.is_empty() {
                mutation.uuid = envelope.mutation_uuid.clone();
            } else if envelope.written_at == 0 && envelope.logical_counter == 0 {
                // Preserve the legacy zero-clock marker. A random UUID here
                // would turn ordered bootstrap replay into a random LWW tie.
                mutation.uuid.clear();
            }
            mutation.source_file_name = envelope.source_file_name.clone();
            mutation.metadata = envelope.metadata.clone();
            mutation.aggregate_set = envelope.aggregate_set.clone();
            Some(mutation)
        })
        .collect();
    // A cloud list can deliver independent writer segments in different
    // orders on two receivers. Sort by the stable molecule slot first and
    // the origin LWW identity second. The molecule merge decides the tip.
    mutations.sort_unstable_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| {
                left.key_value
                    .to_storage_key()
                    .cmp(&right.key_value.to_storage_key())
            })
            .then_with(|| {
                left.imported_written_at
                    .unwrap_or(0)
                    .cmp(&right.imported_written_at.unwrap_or(0))
            })
            .then_with(|| left.logical_counter.cmp(&right.logical_counter))
            .then_with(|| {
                left.author_clock_writer_id
                    .cmp(&right.author_clock_writer_id)
            })
            .then_with(|| left.uuid.cmp(&right.uuid))
            .then_with(|| left.content_hash().cmp(&right.content_hash()))
    });
    (mutations, prefix)
}

/// Resolve ref-only v2 envelopes from the atom plane using exact UUID reads.
/// Old inline envelopes and already-materialized sealed payloads are unchanged.
pub async fn materialize_field_values(
    mut envelopes: Vec<MutationEnvelope>,
    atoms: &crate::db_operations::atom_store::AtomStore,
) -> Result<Vec<MutationEnvelope>, String> {
    for envelope in &mut envelopes {
        // Sorted, and every unresolvable field collected before reporting.
        // `field_atom_uuids` is a `HashMap`, so reporting the first miss in
        // iteration order named an arbitrary member of the record — a field
        // whose own atom was usually fine. Two days of quarantine logs on the
        // primary blamed `title`/`created_at`/`slug` for a hole that was
        // never in any of them.
        let mut refs: Vec<(&String, &String)> = envelope
            .field_atom_uuids
            .iter()
            .filter(|(field, _)| !envelope.fields_and_values.contains_key(*field))
            .collect();
        refs.sort_unstable();
        let mut resolved = Vec::with_capacity(refs.len());
        let mut missing: Vec<String> = Vec::new();
        for (field, atom_uuid) in refs {
            let atom = atoms
                .get_atom_by_uuid(atom_uuid, envelope.storage_prefix.as_deref())
                .await
                .map_err(|error| format!("load atom {atom_uuid} for {field}: {error}"))?;
            match atom {
                Some(atom) => {
                    if atom.uuid() != atom_uuid.as_str() {
                        return Err(format!(
                            "atom identity mismatch for field {field}: expected {atom_uuid}, got {}",
                            atom.uuid()
                        ));
                    }
                    resolved.push((field.clone(), atom.content().clone()));
                }
                None => missing.push(format!("{field}={atom_uuid}")),
            }
        }
        if let Some(first) = missing.first() {
            // Keep the `missing atom <id> for field <field>` prefix:
            // `pin_log_record_cannot_seal` matches on it, and the quarantine
            // tombstone stores the whole string.
            let (field, atom_uuid) = first.split_once('=').unwrap_or((first, ""));
            let rest = if missing.len() > 1 {
                format!(
                    " ({} unresolved of {}: {})",
                    missing.len(),
                    envelope.field_atom_uuids.len(),
                    missing.join(", ")
                )
            } else {
                String::new()
            };
            return Err(format!("missing atom {atom_uuid} for field {field}{rest}"));
        }
        for (field, content) in resolved {
            envelope.fields_and_values.insert(field, content);
        }
    }
    Ok(envelopes)
}

/// A pin-log MutationIntent that cannot be sealed because its atoms are gone.
///
/// The upload cycle must skip that record and continue. Failing the whole
/// cycle on one missing atom blocks later envelopes forever (CoW DEV proof
/// 2026-08-20: `lgpc_cover_hash` poisoned every subsequent Sop write).
pub fn pin_log_record_cannot_seal(err: &str) -> bool {
    err.starts_with("missing atom ")
        || err.starts_with("load atom ")
        || err.starts_with("atom identity mismatch")
}

/// Live log record for one committed write batch.
pub fn mutation_intent_op(envelopes: Vec<MutationEnvelope>) -> LogOp {
    LogOp::MutationIntent {
        mutations: envelopes,
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

fn parse_mutation_type(name: &str) -> Option<MutationType> {
    match name {
        "create" => Some(MutationType::Create),
        "update" => Some(MutationType::Update),
        "delete" => Some(MutationType::Delete),
        "purge" => Some(MutationType::Purge),
        _ => None,
    }
}

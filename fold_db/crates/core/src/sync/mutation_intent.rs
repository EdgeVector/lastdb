//! Decode retained mutation-log envelopes: LastDB mutation intent, not
//! physical KV fanout.
//!
//! No write path encodes envelopes now. The retired-intent verify stack reads
//! old `LogOp::MutationIntent` payloads through this decoder.

use crate::schema::types::operations::MutationType;
use crate::schema::types::Mutation;
use crate::sync::log::MutationEnvelope;

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
            // Preserve the source time, including zero on unsigned input.
            // Every tip uses the same strict last-writer-wins comparison.
            mutation.imported_written_at = Some(envelope.written_at);
            mutation.erase_tips_before_nanos = envelope.erase_tips_before_nanos;
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

fn parse_mutation_type(name: &str) -> Option<MutationType> {
    match name {
        "create" => Some(MutationType::Create),
        "update" => Some(MutationType::Update),
        "delete" => Some(MutationType::Delete),
        "purge" => Some(MutationType::Purge),
        _ => None,
    }
}

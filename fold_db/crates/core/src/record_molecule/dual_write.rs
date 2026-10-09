//! Dual-write matches dual-read: envelope tip → merge; else today's per-field write.
//!
//! A missing or non-envelope R tip is never an empty document. That would hide
//! live field molecules.

use super::dual_read::load_record_tip_atom;
use super::envelope::{
    envelope_is_complete, merge_sent_fields, parse_document_fields, stamp_document_envelope_marked,
};
use super::RECORD_SENTINEL;
use crate::db_operations::DbOperations;
use crate::schema::types::Mutation;
use crate::schema::Schema;
use crate::schema::SchemaError;

/// Rewrite compacted-key mutations to one envelope atom on R.
///
/// - Mutation already targeting `RECORD_SENTINEL` (compact) is left as-is.
/// - R tip is an envelope → merge sent keys into `fields`, write R only.
/// - Missing / non-envelope tip → leave per-field mutation (today's path).
pub async fn fold_mutations_to_record_envelope(
    db_ops: &DbOperations,
    schema: &Schema,
    mutations: &mut [Mutation],
) -> Result<(), SchemaError> {
    let Some(record_uuid) = schema.molecule_uuid.as_deref() else {
        return Ok(());
    };

    for mutation in mutations.iter_mut() {
        if mutation.fields_and_values.len() == 1
            && mutation.fields_and_values.contains_key(RECORD_SENTINEL)
        {
            continue;
        }

        let Some(atom) = load_record_tip_atom(db_ops, record_uuid, &mutation.key_value).await?
        else {
            continue;
        };
        let Some(existing) = parse_document_fields(atom.content()) else {
            continue;
        };
        let mut fields = existing.clone();
        merge_sent_fields(&mut fields, &mutation.fields_and_values);
        // Preserve completeness. A merge does not drop unsent keys, so a
        // complete envelope stays complete. An incomplete envelope must not
        // flip to complete here — compact restamps after a full field zip.
        let complete = envelope_is_complete(atom.content());
        let envelope = stamp_document_envelope_marked(fields, complete);
        // Keep key-layout fields and undeclared extras for protein fold.
        // Prepare writes only the RECORD_SENTINEL atom on this path.
        mutation
            .fields_and_values
            .insert(RECORD_SENTINEL.to_string(), envelope);
    }
    Ok(())
}

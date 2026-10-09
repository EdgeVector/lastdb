use crate::schema::types::field::WriteContext;
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// Writes a mutation to the field.
    pub fn write_mutation(&mut self, key_value: &KeyValue, ctx: WriteContext) {
        self.ensure_molecule(&ctx.schema_name, &ctx.field_name);

        let meta = crate::atom::KeyMetadata {
            source_file_name: ctx.source_file_name,
            metadata: ctx.metadata,
            tombstoned: ctx.atom.is_tombstone(),
        };

        match (&self.kind, &mut self.molecule) {
            // Unified keyed write path: every field kind uses HashRange.
            // Single → ("",""); Hash → (h,""); Range → ("",r); HashRange → (h,r).
            (
                FieldKind::Single | FieldKind::Hash | FieldKind::Range | FieldKind::HashRange,
                Some(molecule),
            ) => {
                let (hash_key, range_key) = match self.kind {
                    FieldKind::Single => (String::new(), String::new()),
                    FieldKind::Hash => match &key_value.hash {
                        Some(h) => (h.clone(), String::new()),
                        None => return,
                    },
                    FieldKind::Range => match &key_value.range {
                        Some(r) => (String::new(), r.clone()),
                        None => return,
                    },
                    FieldKind::HashRange => {
                        if let (Some(h), Some(r)) = (&key_value.hash, &key_value.range) {
                            (h.clone(), r.clone())
                        } else {
                            tracing::warn!(
                                "HashRange write_mutation: atom {} not indexed — hash={:?}, range={:?}. \
                                 Both hash and range keys are required for HashRange fields.",
                                ctx.atom.uuid(),
                                key_value.hash,
                                key_value.range
                            );
                            return;
                        }
                    }
                };
                match ctx.writer_override {
                    Some(crate::atom::provenance::Provenance::User {
                        pubkey,
                        signature,
                        signature_version,
                    }) => {
                        let device_id = if ctx.author_clock_writer_id.is_empty() {
                            pubkey.clone()
                        } else {
                            ctx.author_clock_writer_id.clone()
                        };
                        molecule.set_atom_uuid_from_values_imported_with_author(
                            hash_key.clone(),
                            range_key.clone(),
                            ctx.atom.uuid().to_string(),
                            device_id,
                            pubkey,
                            signature,
                            signature_version,
                            ctx.imported_written_at,
                            ctx.logical_counter,
                            ctx.mutation_uuid.clone(),
                        );
                    }
                    _ if ctx.imported_written_at.is_some() => {
                        // Mutation-log replay: keep origin written_at / writer
                        // even when the envelope had no User provenance.
                        let device_id = if ctx.author_clock_writer_id.is_empty() {
                            ctx.pub_key.clone()
                        } else {
                            ctx.author_clock_writer_id.clone()
                        };
                        molecule.set_atom_uuid_from_values_imported_with_author(
                            hash_key.clone(),
                            range_key.clone(),
                            ctx.atom.uuid().to_string(),
                            device_id,
                            // The author-clock writer lives in `device_id`.
                            // Without signed per-tip provenance, keep the
                            // legacy crypto payload empty and the tip thin.
                            String::new(),
                            String::new(),
                            0,
                            ctx.imported_written_at,
                            ctx.logical_counter,
                            ctx.mutation_uuid.clone(),
                        );
                    }
                    _ => {
                        molecule.set_atom_uuid_from_values_with_author(
                            hash_key.clone(),
                            range_key.clone(),
                            ctx.atom.uuid().to_string(),
                            &ctx.signer,
                            ctx.logical_counter,
                            ctx.mutation_uuid.clone(),
                        );
                    }
                }
                molecule.set_key_metadata(hash_key, range_key, meta);
            }
            _ => {
                // kind/molecule mismatch should not happen after ensure_molecule
                tracing::error!(
                    kind = ?self.kind,
                    "write_mutation: field kind / molecule data mismatch"
                );
            }
        }
    }

    /// Materialize this field's in-memory molecule if it is not hydrated yet.
    ///
    /// `self.molecule` is `#[serde(skip)]`, so it is `None` after every schema
    /// load, clone, or node restart — not only for a field that has never been
    /// written. The durable binding is `inner.molecule_uuid`, which
    /// `populate_runtime_fields` has already resolved: a field carrying a
    /// `FieldMapper` resolves to its chain ROOT's `(schema, field)` pair, and
    /// `apply_field_mappers` copies the root's UUID for a multi-hop chain.
    ///
    /// So `(schema_name, field_name)` — the mutation's own names — is the
    /// correct source pair only for an UNMAPPED field. Deriving the UUID from
    /// it unconditionally re-anchored every mapped field onto
    /// `deterministic(mapped_schema, mapped_field)`, a second, empty molecule,
    /// and overwrote the resolved binding on the way out. Because the trigger
    /// is "molecule not hydrated at this particular write", the outcome is
    /// write-order dependent: a node that hydrates the mapped molecule before
    /// its first write keeps the root UUID, and a fresh node that writes first
    /// derives its own. Two nodes then hold two molecule UUIDs for one logical
    /// field, which is exactly what `design-org-alice-bob-simultaneous-writes`
    /// R1 forbids.
    ///
    /// An assigned `inner.molecule_uuid` is therefore authoritative here; the
    /// deterministic pair is only the fallback for a field that has none.
    fn ensure_molecule(&mut self, schema_name: &str, field_name: &str) {
        if self.molecule.is_some() {
            return;
        }
        // Clone out of `inner` first: the fallback branch mutates `inner`, so
        // holding a borrow of it across the branch would not borrowck.
        let assigned = self.inner.molecule_uuid().cloned();
        self.molecule = Some(if let Some(uuid) = assigned {
            crate::atom::MoleculeHashRange::with_uuid(uuid)
        } else {
            let data = crate::atom::MoleculeHashRange::new(schema_name, field_name);
            self.inner.set_molecule_uuid(data.uuid().to_string());
            data
        });
    }
}

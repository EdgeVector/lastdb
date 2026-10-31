use crate::schema::types::field::WriteContext;
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};
use crate::atom::ImportedTip;

impl FieldVariant {
    /// Writes a mutation to the field.
    pub fn write_mutation(&mut self, key_value: &KeyValue, ctx: WriteContext) -> bool {
        self.ensure_molecule(&ctx.schema_name, &ctx.field_name);

        let Some((hash, range)) = self.slot_coordinates_ref(key_value) else {
            if self.kind == FieldKind::HashRange {
                tracing::warn!(
                    "HashRange write_mutation: atom {} not indexed — hash={:?}, range={:?}. \
                     Both hash and range keys are required for HashRange fields.",
                    ctx.atom.uuid(),
                    key_value.hash,
                    key_value.range
                );
            }
            return false;
        };
        let Some(molecule) = self.molecule.as_mut() else {
            tracing::error!(
                kind = ?self.kind,
                "write_mutation: field kind / molecule data mismatch"
            );
            return false;
        };
        Self::write_tip(molecule, ctx, hash.to_string(), range.to_string())
    }

    /// Set the tip for one molecule key from the write context, then its metadata.
    fn write_tip(
        molecule: &mut crate::atom::MoleculeHashRange,
        ctx: WriteContext,
        hash_key: String,
        range_key: String,
    ) -> bool {
        let meta = crate::atom::KeyMetadata {
            source_file_name: ctx.source_file_name,
            metadata: ctx.metadata,
            tombstoned: ctx.atom.is_tombstone(),
        };
        let accepted = match ctx.writer_override {
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
                    ImportedTip {
                        atom_uuid: ctx.atom.uuid().to_string(),
                        device_id,
                        origin_device: ctx.origin_device,
                        writer_pubkey: pubkey,
                        signature,
                        signature_version,
                        written_at: ctx.imported_written_at,
                        logical_counter: ctx.logical_counter,
                        mutation_uuid: ctx.mutation_uuid.clone(),
                    },
                )
            }
            _ if ctx.imported_written_at.is_some() => {
                // Mutation-log replay: keep origin written_at / writer
                // even when the envelope had no User provenance.
                let device_id = if ctx.author_clock_writer_id.is_empty() {
                    ctx.pub_key.clone()
                } else {
                    ctx.author_clock_writer_id.clone()
                };
                // The author-clock writer lives in `device_id`. Without signed
                // per-tip provenance, the legacy crypto payload stays empty
                // and the tip stays thin.
                molecule.set_atom_uuid_from_values_imported_with_author(
                    hash_key.clone(),
                    range_key.clone(),
                    ImportedTip {
                        atom_uuid: ctx.atom.uuid().to_string(),
                        device_id,
                        origin_device: ctx.origin_device,
                        written_at: ctx.imported_written_at,
                        logical_counter: ctx.logical_counter,
                        mutation_uuid: ctx.mutation_uuid.clone(),
                        ..ImportedTip::default()
                    },
                )
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
                molecule.stamp_tip_origin_device(&hash_key, &range_key, &ctx.origin_device);
                true
            }
        };
        // A rejected imported atom must not alter the winning tip's metadata.
        if accepted {
            molecule.set_key_metadata(hash_key, range_key, meta);
        }
        accepted
    }

    /// Materialize this field's in-memory molecule if it is not hydrated yet.
    ///
    /// `self.molecule` is `#[serde(skip)]`, so it is `None` after every schema
    /// load, clone, or node restart — not only for a field that has never been
    /// written. The durable binding is `inner.molecule_uuid`, which
    /// `populate_runtime_fields` has already resolved from the persisted
    /// `field_molecule_uuids` entry, or derived from this schema and field.
    ///
    /// A persisted binding can differ from the mutation's own
    /// `(schema_name, field_name)` pair. Deriving the UUID from that pair
    /// unconditionally would re-anchor the field onto a second, empty molecule
    /// and overwrite the resolved binding on the way out. Two nodes could then
    /// hold two molecule UUIDs for one logical field, which
    /// `design-org-alice-bob-simultaneous-writes` R1 forbids.
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

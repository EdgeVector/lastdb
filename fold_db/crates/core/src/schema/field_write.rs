//! The field write path: sign, stamp, and set one molecule tip.
//!
//! `FieldVariant` is a model type and holds no signer and reads no clock.
//! This core module owns the write: it names the local device from the
//! signer, reads the clock through [`MoleculeClock`], and logs a refused key.

use crate::atom::{ImportedTip, MoleculeClock};
use crate::schema::types::field::{FieldKind, FieldVariant};
use crate::schema::types::key_value::KeyValue;

/// Bundles all write-time provenance for a field mutation.
/// Contains the atom plus optional metadata that should be stored
/// per-key on the molecule (surviving atom dedup).
pub struct WriteContext {
    pub atom: crate::atom::Atom,
    pub pub_key: String,
    pub source_file_name: Option<String>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub schema_name: String,
    pub field_name: String,
    /// The signing keypair for molecule signatures. Used when
    /// `writer_override` is `None`.
    pub signer: std::sync::Arc<crate::security::Ed25519KeyPair>,
    /// Replay/import override. When `Some(Provenance::User { .. })`, the
    /// molecule's per-key write path stamps the caller-supplied
    /// `(pubkey, signature, signature_version)` directly onto the
    /// `AtomEntry` instead of re-signing locally with `signer`. This is
    /// how an inbound `data_share` from another node preserves the
    /// original sender's `writer_pubkey` on the receiver's AtomEntry,
    /// so the assertion `record.author_pub_key == sender.pub_key` can
    /// pass on HashRange schemas. `None` (the default) keeps the local
    /// signing path used by every first-party mutation.
    pub writer_override: Option<crate::atom::provenance::Provenance>,
    /// The `written_at` the original author SIGNED, for the import path.
    /// Only meaningful when `writer_override` is `Some`: the molecule
    /// canonical bytes include `written_at`, so preserving the sender's
    /// value is what keeps the imported signature verifiable at rest
    /// (`verify` / `verify_key`). `None` falls back to stamping the local
    /// clock — attribution preserved, signature unverifiable (legacy
    /// behavior).
    pub imported_written_at: Option<u64>,
    /// Signed per-device logical author counter. Zero on legacy writes.
    pub logical_counter: u64,
    /// Device identity that signed the mutation author clock.
    pub author_clock_writer_id: String,
    /// Stable mutation identity used by the final LWW tie-break.
    pub mutation_uuid: String,
    /// Sync device that wrote the mutation: this node's sync device id for a
    /// local write, the log writer for a replayed one. Empty when unknown.
    pub origin_device: String,
    /// The `version` the original author SIGNED, for the import path.
    /// Only consumed by `SingleField` — the single-atom `Molecule` is the
    /// one variant whose canonical bytes include `version`. Ignored by the
    /// per-key variants (their entries do not sign a version).
    pub imported_version: Option<u64>,
}

/// Write one mutation into a field's hydrated molecule.
pub trait FieldWrite {
    /// Writes a mutation to the field. Returns false when the key does not
    /// address a slot of this field, or an imported tip loses to the tip.
    fn write_mutation(&mut self, key_value: &KeyValue, ctx: WriteContext) -> bool;
}

impl FieldWrite for FieldVariant {
    fn write_mutation(&mut self, key_value: &KeyValue, ctx: WriteContext) -> bool {
        ensure_molecule(self, &ctx.schema_name, &ctx.field_name);

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
        write_tip(molecule, ctx, hash.to_string(), range.to_string())
    }
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
/// `field.molecule` is `#[serde(skip)]`, so it is `None` after every schema
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
fn ensure_molecule(field: &mut FieldVariant, schema_name: &str, field_name: &str) {
    if field.molecule.is_some() {
        return;
    }
    // Clone out of `inner` first: the fallback branch mutates `inner`, so
    // holding a borrow of it across the branch would not borrowck.
    let assigned = field.inner.molecule_uuid().cloned();
    field.molecule = Some(if let Some(uuid) = assigned {
        crate::atom::MoleculeHashRange::with_uuid(uuid)
    } else {
        let data = crate::atom::MoleculeHashRange::new(schema_name, field_name);
        field.inner.set_molecule_uuid(data.uuid().to_string());
        data
    });
}

use super::*;
use crate::db_operations::atom_store::counter::TipCommit;

/// An atom to store, with the molecule whose key bundle seals its body in an
/// org scope. A personal scope seals every body with the home content key.
pub(crate) type SealedAtom = (Atom, Option<String>);

impl AtomStore {
    /// Creates an atom in memory without storing it.
    /// Used for batch operations where atoms are collected first then stored together.
    ///
    /// Enforces [`crate::atom::max_atom_content_bytes`] on `value` so oversized
    /// field payloads fail at the create boundary (not only on store).
    /// Default 256 KiB; see `LASTDB_MAX_ATOM_CONTENT_BYTES` and
    /// `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`.
    pub fn create_atom(
        schema_name: &str,
        value: Value,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Atom, SchemaError> {
        // Enforce and record in one call: a rejection names its schema in the
        // node log, and an accepted write that is running out of headroom says
        // so before the growth that would wedge it.
        crate::atom::enforce_atom_content_limit(schema_name, &value)?;
        let mut atom = Atom::new(schema_name.to_string(), value);
        if let Some(filename) = source_file_name {
            atom = atom.with_source_file_name(filename);
        }
        if let Some(meta) = metadata {
            atom = atom.with_metadata(meta);
        }
        Ok(atom)
    }

    /// Account the bodies that one planner commit wrote or reused in the
    /// keep-small meters, so the tip accounting that follows can bind each tip
    /// to its atom size and schema.
    pub(crate) fn account_stored_bodies(&self, bodies: &[SealedAtom], commit: &TipCommit) {
        let created: std::collections::HashSet<&str> =
            commit.created.iter().map(Atom::uuid).collect();
        let mut seen = std::collections::HashSet::new();
        for (atom, _) in bodies {
            if !seen.insert(atom.uuid()) {
                continue;
            }
            let logical = serde_json::to_vec(atom).map_or(0, |bytes| bytes.len() as u64);
            let blob_bytes = crate::atom::file_pointer::blob_logical_bytes_of_atom(
                atom.content(),
                atom.metadata(),
            );
            if created.contains(atom.uuid()) {
                self.keep_small.record_atom_put_with_blob(
                    atom.source_schema_name(),
                    atom.uuid(),
                    logical,
                    blob_bytes.unwrap_or_default(),
                    blob_bytes.is_some(),
                );
            } else {
                self.keep_small.remember_atom_contribution(
                    atom.source_schema_name(),
                    atom.uuid(),
                    logical,
                    blob_bytes.unwrap_or_default(),
                    blob_bytes.is_some(),
                    false,
                );
            }
        }
    }
}

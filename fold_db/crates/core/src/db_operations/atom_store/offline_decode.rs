//! Read-only atom decoding without database initialization or key creation.

use super::*;
use crate::atom::{atom_row_header, Atom};
use crate::schema::SchemaError;
use std::collections::BTreeSet;

impl AtomStore {
    /// Open the decoder and existing molecule wraps only. This constructor
    /// performs no ensure, mint, boot, marker, or flush operation.
    pub async fn for_offline_read(
        store: Arc<dyn NamespacedStore>,
        content_key: [u8; 32],
        molecule_wrap_key: [u8; 32],
    ) -> Result<Self, SchemaError> {
        let mut atoms = Self::from_namespaced_store(Arc::clone(&store)).await?;
        let entries = store
            .open_namespace("molecule_keys")
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("open existing molecule keys: {error}"))
            })?;
        atoms.content_key = Some(content_key);
        atoms.molecule_keys = Some(crate::db_operations::MoleculeKeyStore::new(
            Arc::new(TypedKvStore::new(entries)),
            Some(molecule_wrap_key),
        ));
        Ok(atoms)
    }

    /// Decode a bounded batch with the normal JSON/binary content codecs.
    /// Explicit bundle selectors must be valid and resolve existing wraps.
    /// One batched read warms all missing bundles before content decoding.
    /// Atoms without a selector retain the normal account-key legacy path.
    pub async fn decode_stored_atom_batch(
        &self,
        rows: &[Vec<u8>],
    ) -> Result<Vec<Atom>, SchemaError> {
        let mut bundles = BTreeSet::new();
        for row in rows {
            let header = atom_row_header(row).map_err(|error| {
                SchemaError::InvalidData(format!("read offline atom header: {error}"))
            })?;
            if let Some(selector) = header.get("molecule_key_bundle") {
                let name = selector
                    .as_str()
                    .filter(|name| !name.trim().is_empty())
                    .ok_or_else(|| {
                        SchemaError::InvalidData(
                            "offline atom has an invalid molecule bundle selector".into(),
                        )
                    })?;
                bundles.insert(name.to_string());
            }
        }
        if !bundles.is_empty() {
            let keys = self.molecule_keys.as_ref().ok_or_else(|| {
                SchemaError::InvalidData("offline decoder has no molecule key store".into())
            })?;
            keys.load_existing_many(&bundles.into_iter().collect::<Vec<_>>())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("load offline molecule bundles: {error}"))
                })?;
        }
        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            decoded.push(self.decode_atom_bytes(row).await?);
        }
        Ok(decoded)
    }
}

impl AtomStore {
    /// Pure production keys used after every physical source body is gone.
    /// This does not perform any lookup, mutation, marker or ledger operation.
    /// Blob edge keys precede the locator, as on the normal atom GC path.
    pub fn offline_reclaim_derived_keys(
        atom: &Atom,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        let mut references =
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
                .into_iter()
                .collect::<Vec<_>>();
        references.sort();
        let mut keys = Vec::with_capacity(references.len() + 1);
        for reference in references {
            let hash = reference.strip_prefix("sha256:").unwrap_or_default();
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(SchemaError::InvalidData(
                    "offline atom has an unsupported file blob identity".into(),
                ));
            }
            keys.push(
                super::blob_ref_edges::BlobRefEdge::atom(atom.uuid(), &reference)
                    .storage_key(storage_prefix),
            );
        }
        keys.push(crate::schema::types::field::build_storage_key(
            storage_prefix,
            &crate::atom::atom_locator_codec::locator_key(atom.uuid()),
        ));
        Ok(keys)
    }
}

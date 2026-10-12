//! Batched native dependency reads. Corrupt-present data never means absent.

use super::*;
use crate::atom::{atom_key_codec, atom_locator_codec, Atom, AtomKeyEncoding};
use crate::schema::types::field::build_storage_key;

impl AtomStore {
    pub(super) async fn add_referenced_atom_blobs(
        &self,
        roots: &mut SourceRoots,
        envelopes: &[MutationEnvelope],
    ) -> Result<(), SchemaError> {
        for scope in &roots.scopes {
            let ids: Vec<&str> = scope.atoms.iter().map(String::as_str).collect();
            let atoms = self
                .read_retired_atoms(&ids, scope.storage_prefix.as_deref())
                .await?;
            for (id, atom) in ids.into_iter().zip(atoms) {
                if let Some(atom) = atom {
                    if atom.uuid() != id {
                        return Err(refused("retired intent atom identity differs"));
                    }
                    roots
                        .blobs
                        .extend(crate::atom::file_pointer::blob_refs_of_atom(
                            atom.content(),
                            atom.metadata(),
                        ));
                } else if !inline_atom_is_complete(id, scope.storage_prefix.as_deref(), envelopes) {
                    return Err(refused("retired intent required atom is absent"));
                }
            }
        }
        Ok(())
    }

    /// Same exact flat/locator addressing as the native reader, with strict presence.
    async fn read_retired_atoms(
        &self,
        ids: &[&str],
        prefix: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        let main = self.raw().inner();
        let flat_keys: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| build_storage_key(prefix, &atom_key_codec::flat_key(id)).into_bytes())
            .collect();
        let raw = storage::read_many(main.as_ref(), &flat_keys).await?;
        let mut out = Vec::with_capacity(ids.len());
        for bytes in raw {
            out.push(match bytes {
                Some(bytes) => Some(
                    self.decode_atom_bytes(&bytes)
                        .await
                        .map_err(|_| refused("retired intent atom body is unreadable"))?,
                ),
                None => None,
            });
        }
        if !self.atom_key_encoding().writes_partition_prefix() {
            return Ok(out);
        }
        let missing: Vec<usize> = out
            .iter()
            .enumerate()
            .filter_map(|(i, atom)| atom.is_none().then_some(i))
            .collect();
        let locator_keys: Vec<Vec<u8>> = missing
            .iter()
            .map(|&i| {
                build_storage_key(prefix, &atom_locator_codec::locator_key(ids[i])).into_bytes()
            })
            .collect();
        let locators = storage::read_many(main.as_ref(), &locator_keys).await?;
        let mut located = Vec::new();
        let mut keys = Vec::new();
        for (&i, raw) in missing.iter().zip(locators) {
            let Some(raw) = raw else {
                continue;
            };
            let value = serde_json::from_slice(&raw)
                .map_err(|_| refused("retired intent atom locator is unreadable"))?;
            let partition = atom_locator_codec::decode_value(&value)
                .ok_or_else(|| refused("retired intent atom locator is malformed"))?;
            located.push(i);
            keys.push(
                build_storage_key(
                    prefix,
                    &atom_key_codec::storage_key(
                        AtomKeyEncoding::PartitionPrefix,
                        Some(&partition),
                        ids[i],
                    ),
                )
                .into_bytes(),
            );
        }
        let bodies = storage::read_many(main.as_ref(), &keys).await?;
        for (i, raw) in located.into_iter().zip(bodies) {
            if let Some(raw) = raw {
                out[i] = Some(
                    self.decode_atom_bytes(&raw)
                        .await
                        .map_err(|_| refused("retired intent atom body is unreadable"))?,
                );
            }
        }
        Ok(out)
    }

    pub(super) async fn require_retired_blob_presence(
        &self,
        blobs: &[String],
    ) -> Result<(), SchemaError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let namespaced = self
            .namespaced_store
            .as_ref()
            .ok_or_else(|| refused("retired intent retention requires a native store"))?;
        let cas = namespaced
            .open_namespace("cas_blobs")
            .await
            .map_err(|_| refused("retired intent blob store is unavailable"))?;
        let cas_keys = blobs.iter().map(|id| id.as_bytes().to_vec()).collect();
        let main_keys = blobs
            .iter()
            .map(|id| format!("cas_blob:{id}").into_bytes())
            .collect();
        let (cas_present, main_present) = tokio::try_join!(
            cas.exists_many(cas_keys),
            self.raw().inner().exists_many(main_keys),
        )
        .map_err(|_| refused("retired intent blob presence is unreadable"))?;
        if cas_present.len() != blobs.len() || main_present.len() != blobs.len() {
            return Err(refused("retired intent blob presence is incomplete"));
        }
        if cas_present
            .into_iter()
            .zip(main_present)
            .any(|(cas, main)| !cas && !main)
        {
            return Err(refused("retired intent required local blob is absent"));
        }
        Ok(())
    }
}

use super::*;
use futures::FutureExt;

impl AtomStore {
    /// Retrieve a single atom by its UUID.
    ///
    /// When `storage_prefix` is `Some`, the key is prefixed with `{storage_prefix}:`
    /// (also used for share namespaces `from:{sender}`). Exact prefix only —
    /// no bare dual-read.
    ///
    /// One point read of the body key `atom:{uuid}\0b`. A body exists if and
    /// only if its counter exists, so a missing body is the final answer.
    pub async fn get_atom_by_uuid(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<Atom>, SchemaError> {
        Ok(self
            .get_atoms(&[atom_uuid], storage_prefix)
            .await?
            .into_iter()
            .next()
            .flatten())
    }

    /// Batch counterpart of [`Self::get_atom_by_uuid`]: load many atoms in one
    /// read. Output slot `i` is the atom named by `uuids[i]`, or `None`.
    ///
    /// Every hot read path funnels its atom bodies through here
    /// (`variant/read.rs`, `filter_utils/fetch.rs`, `hash_range_query.rs` x2).
    pub async fn get_atoms(
        &self,
        uuids: &[&str],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        self.atom_read_scope(self.get_atoms_captured(uuids, storage_prefix).boxed())
            .await
    }

    async fn get_atoms_captured(
        &self,
        uuids: &[&str],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        let mut identities = HashMap::with_capacity(uuids.len());
        let mut keys = Vec::new();
        let mut uses = Vec::<usize>::new();
        let slots: Vec<_> = uuids
            .iter()
            .map(|uuid| {
                let slot = *identities.entry(*uuid).or_insert_with(|| {
                    keys.push(self.body_storage_key(uuid, storage_prefix));
                    uses.push(0);
                    keys.len() - 1
                });
                uses[slot] += 1;
                slot
            })
            .collect();
        let mut out = self.get_atoms_by_storage_keys_captured(&keys).await?;
        if out.len() != keys.len() {
            return Err(SchemaError::StorageRead {
                detail: "unique atom read returned a different number of results".into(),
            });
        }
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::logical_paths::source::bind_atoms(
            runtime,
            storage_prefix,
            &keys,
            &out,
        )?;
        Ok(slots
            .into_iter()
            .map(|slot| {
                uses[slot] -= 1;
                if uses[slot] == 0 {
                    out[slot].take()
                } else {
                    out[slot].clone()
                }
            })
            .collect())
    }

    /// The storage key of one atom body in one scope.
    pub(crate) fn body_storage_key(&self, atom: &str, scope: Option<&str>) -> String {
        build_storage_key(scope, &atom_key_codec::body_key(atom))
    }
}

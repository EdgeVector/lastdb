use super::*;

impl AtomStore {
    /// Retrieve a single atom by its UUID.
    ///
    /// When `storage_prefix` is `Some`, the key is prefixed with `{storage_prefix}:`
    /// (also used for share namespaces `from:{sender}`). Exact prefix only —
    /// no bare dual-read.
    ///
    /// This is the **uuid-only** surface: the caller names an atom without
    /// naming the slot that owns it. It is the bulk object read surface — how a
    /// lastgit pack object is read back — so it must resolve a
    /// partition-prefixed body too. It does that in a bounded two point reads:
    /// the flat key, then the [`atom_locator_codec`] row and the prefixed key
    /// that row names. A missing locator is not an error — it is the ordinary
    /// answer for an atom that does not exist, and for one written before the
    /// locator existed.
    ///
    /// A caller that knows the owning slot should pass it to
    /// [`Self::get_atom_by_uuid_in_partition`] instead and skip the locator hop;
    /// one that holds a whole slot's worth of keys should build them with
    /// [`atom_key_codec::storage_key`] and batch through
    /// [`super::AtomStore::get_atoms_by_storage_keys`].
    pub async fn get_atom_by_uuid(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<Atom>, SchemaError> {
        self.get_atom_by_uuid_in_partition(atom_uuid, None, storage_prefix)
            .await
    }

    /// [`Self::get_atom_by_uuid`] with the owning slot's partition as a **hint**.
    ///
    /// The hint is an optimization and never a correctness dependency: a
    /// caller that passes the wrong partition (or one derived from an API-form
    /// hash where the body was written under a blinded storage-form one) pays
    /// an extra point read and still gets the right atom. That is what makes
    /// threading the slot through callers safe to do one at a time — a caller
    /// cannot make an atom unreadable by guessing badly, only slower.
    ///
    /// Resolution order, each step skipped when it cannot apply:
    ///
    /// 1. the hinted prefixed key — one read, the common case for a caller that
    ///    knows its slot;
    /// 2. the flat key `atom:{uuid}` — every body on every shipped home, and
    ///    every body whose partition was unknown at write time;
    /// 3. the [`atom_locator_codec`] row, then the prefixed key it names — the
    ///    backstop that makes the uuid-only surface complete.
    ///
    /// Under [`crate::atom::AtomKeyEncoding::Flat`] step 1 and step 3 are both
    /// unreachable (no prefixed bodies, so no locator rows), leaving exactly the
    /// single flat read this path has always done.
    pub async fn get_atom_by_uuid_in_partition(
        &self,
        atom_uuid: &str,
        partition_hint: Option<&crate::atom::AtomPartition>,
        storage_prefix: Option<&str>,
    ) -> Result<Option<Atom>, SchemaError> {
        let prefixed = crate::atom::AtomKeyEncoding::PartitionPrefix;

        if let Some(hint) =
            partition_hint.filter(|_| self.atom_key_encoding().writes_partition_prefix())
        {
            let key = build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(prefixed, Some(hint), atom_uuid),
            );
            if let Some(raw) = self.get_atom_value(&key).await? {
                return Ok(Some(self.decode_atom_bytes(&raw).await?));
            }
        }

        let flat_key = build_storage_key(storage_prefix, &atom_key_codec::flat_key(atom_uuid));
        if let Some(raw) = self.get_atom_value(&flat_key).await? {
            return Ok(Some(self.decode_atom_bytes(&raw).await?));
        }

        let Some(partition) = self
            .lookup_atom_partition(atom_uuid, storage_prefix)
            .await?
        else {
            return Ok(None);
        };
        let located_key = build_storage_key(
            storage_prefix,
            &atom_key_codec::storage_key(prefixed, Some(&partition), atom_uuid),
        );
        match self.get_atom_value(&located_key).await? {
            Some(raw) => Ok(Some(self.decode_atom_bytes(&raw).await?)),
            None => Ok(None),
        }
    }

    /// Batch counterpart of [`Self::get_atom_by_uuid_in_partition`]: load many
    /// atoms, each with the partition of the slot that owns it when the caller
    /// knows it.
    ///
    /// Every hot read path funnels its atom bodies through here
    /// (`variant/read.rs`, `filter_utils/fetch.rs`, `hash_range_query.rs` ×2),
    /// which is what makes the partition a *hint* rather than a contract. The
    /// same three-step ladder the single-atom path uses, batched — one extra
    /// round trip per step, not per atom, and only for the atoms still missing:
    ///
    /// 1. the key `encoding` + hint name — the whole batch, one read;
    /// 2. the flat key, for the atoms that had a hint and missed — a body
    ///    written before the migration reached it, so a home mid-migration
    ///    reads correctly under either addressing;
    /// 3. the [`atom_locator_codec`] rows for whatever is *still* missing,
    ///    then the prefixed keys they name — the backstop that catches a
    ///    caller whose hint was wrong or absent.
    ///
    /// Under [`crate::atom::AtomKeyEncoding::Flat`] step 1 builds exactly the
    /// flat keys this path has always built and steps 2–3 are unreachable, so
    /// the shipped encoding pays nothing — not even an extra comparison per
    /// row.
    pub async fn get_atoms_located(
        &self,
        slots: &[(&str, Option<crate::atom::AtomPartition>)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<Atom>>, SchemaError> {
        let encoding = self.atom_key_encoding();
        let keys: Vec<String> = slots
            .iter()
            .map(|(uuid, partition)| {
                build_storage_key(
                    storage_prefix,
                    &atom_key_codec::storage_key(encoding, partition.as_ref(), uuid),
                )
            })
            .collect();
        let mut out = self.get_atoms_by_storage_keys(&keys).await?;

        if !encoding.writes_partition_prefix() {
            // The keys above ARE the flat keys. Nothing else can exist.
            return Ok(out);
        }

        // Step 2 — only the slots that named a partition got a non-flat key in
        // step 1, so only those have a flat key left to try.
        let unmigrated: Vec<usize> = out
            .iter()
            .enumerate()
            .filter(|(i, atom)| atom.is_none() && slots[*i].1.is_some())
            .map(|(i, _)| i)
            .collect();
        if !unmigrated.is_empty() {
            let flat_keys: Vec<String> = unmigrated
                .iter()
                .map(|&i| build_storage_key(storage_prefix, &atom_key_codec::flat_key(slots[i].0)))
                .collect();
            for (&i, atom) in unmigrated
                .iter()
                .zip(self.get_atoms_by_storage_keys(&flat_keys).await?)
            {
                out[i] = atom;
            }
        }

        // Step 3 — the backstop. Runs for every atom still missing, including
        // ones whose caller passed a partition: a wrong hint must cost a round
        // trip, never a `None` for an atom that exists.
        let unresolved: Vec<usize> = out
            .iter()
            .enumerate()
            .filter(|(_, atom)| atom.is_none())
            .map(|(i, _)| i)
            .collect();
        if unresolved.is_empty() {
            return Ok(out);
        }
        let locator_keys: Vec<String> = unresolved
            .iter()
            .map(|&i| {
                build_storage_key(storage_prefix, &atom_locator_codec::locator_key(slots[i].0))
            })
            .collect();
        let locators = self
            .main_store
            .get_items::<Value>(&locator_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to fetch atom locators: {e}")))?;

        // Only the rows that named a readable partition are worth a third read.
        let mut located_idx: Vec<usize> = Vec::new();
        let mut located_keys: Vec<String> = Vec::new();
        for (&i, raw) in unresolved.iter().zip(locators) {
            let Some(partition) = raw.as_ref().and_then(atom_locator_codec::decode_value) else {
                continue;
            };
            located_idx.push(i);
            located_keys.push(build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    crate::atom::AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    slots[i].0,
                ),
            ));
        }
        if !located_keys.is_empty() {
            for (&i, atom) in located_idx
                .iter()
                .zip(self.get_atoms_by_storage_keys(&located_keys).await?)
            {
                out[i] = atom;
            }
        }

        Ok(out)
    }

    pub(super) async fn get_atom_value(&self, key: &str) -> Result<Option<Vec<u8>>, SchemaError> {
        self.main_store
            .inner()
            .get(key.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to fetch atom: {e}")))
    }

    /// The partition a body was written under, from its locator row.
    ///
    /// `None` for every degraded case — no row, an unreadable value, a value
    /// that is not a well-formed partition prefix. Each of those means "read
    /// the flat key", which is the pre-locator behaviour, so a corrupt or
    /// partial locator collection can never make a readable atom unreadable.
    pub(crate) async fn lookup_atom_partition(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<crate::atom::AtomPartition>, SchemaError> {
        let key = build_storage_key(storage_prefix, &atom_locator_codec::locator_key(atom_uuid));
        let raw =
            self.main_store.get_item::<Value>(&key).await.map_err(|e| {
                SchemaError::InvalidData(format!("Failed to fetch atom locator: {e}"))
            })?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let partition = atom_locator_codec::decode_value(&raw);
        if partition.is_none() {
            // Not fatal — we fall back to the flat key — but it means a row we
            // wrote no longer round-trips, which is worth seeing.
            tracing::warn!(atom_uuid, "unreadable atom locator value; using flat key");
        }
        Ok(partition)
    }
}

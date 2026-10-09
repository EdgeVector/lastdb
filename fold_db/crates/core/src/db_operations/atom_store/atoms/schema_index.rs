use super::*;

impl AtomStore {
    /// Scan items under `base_prefix` for exactly `storage_prefix` / storage prefix.
    /// No bare dual-read. `err_label` prefixes storage errors.
    pub(crate) async fn scan_exact_storage_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        err_label: &str,
    ) -> Result<Vec<(String, T)>, SchemaError> {
        let prefix = build_storage_key(storage_prefix, base_prefix);
        self.main_store
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))
    }

    /// Enumerate every atom whose `source_schema_name` matches `schema_name`.
    ///
    /// Uses the schema-keyed secondary index for this storage scope: one
    /// bounded prefix key scan over this schema's atom UUID markers, followed
    /// by one batched canonical atom fetch. Older stores without a sentinel are
    /// lazily backfilled once per scope. Output is sorted by atom UUID for
    /// deterministic ordering (snapshot reproducibility).
    ///
    /// When `storage_prefix` is `Some`, only the `{storage_prefix}:…` storage prefix is
    /// scanned (share namespaces `from:{sender}` use the same mechanism).
    /// No bare dual-read.
    pub async fn list_atoms_by_schema(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Atom>, SchemaError> {
        // Exact storage prefix only (no bare dual-read). Prefix is an isolation
        // boundary for share `from:{sender}` namespaces and historical org keys.
        let mut atoms = self
            .list_atoms_by_schema_scope(schema_name, storage_prefix)
            .await?;
        // Sorted by atom UUID for deterministic ordering — required for
        // snapshot reproducibility (preserved from the pre-index full scan).
        atoms.sort_by(|a, b| a.uuid().cmp(b.uuid()));
        Ok(atoms)
    }

    /// Resolve one storage scope through the schema-keyed secondary index.
    pub(crate) async fn list_atoms_by_schema_scope(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Atom>, SchemaError> {
        let err_label = "Failed to scan atoms";

        let sentinel_key = build_storage_key(storage_prefix, schema_index_codec::BACKFILL_SENTINEL);
        let indexed = self
            .schema_index_store
            .exists_item(&sentinel_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))?;

        if !indexed {
            return self
                .backfill_schema_index(schema_name, storage_prefix)
                .await;
        }

        let scan_prefix = build_storage_key(
            storage_prefix,
            &schema_index_codec::schema_prefix(schema_name),
        );
        let keys: Vec<String> = self
            .schema_index_store
            .list_keys_with_prefix(&scan_prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))?;
        // The index stores uuids, not slots, so there is no partition to hint
        // with — `get_atoms_located` falls through to the locator backstop for
        // any prefixed body. Building flat keys directly here (as this did)
        // would silently return FEWER atoms than the schema has once bodies are
        // prefixed, and this is the path `list_atoms_by_schema` snapshots run
        // on: a short read here is a short backup.
        let uuids: Vec<String> = keys
            .into_iter()
            .map(|key| {
                crate::db_operations::atom_store::helpers::strip_record_prefix(&key, &scan_prefix)
            })
            .collect();
        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> =
            uuids.iter().map(|uuid| (uuid.as_str(), None)).collect();
        Ok(self
            .get_atoms_located(&slots, storage_prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))?
            .into_iter()
            .flatten()
            .collect())
    }

    /// One-time lazy backfill of the schema index for a storage scope.
    ///
    /// The sentinel is written in the same batch as every discovered index row,
    /// so a partial backfill is never trusted as complete.
    pub(crate) async fn backfill_schema_index(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Atom>, SchemaError> {
        let err_label = "Failed to scan atoms";
        let scan_prefix = build_storage_key(storage_prefix, atom_key_codec::ATOM_PREFIX);
        let mut scanned = self
            .main_store
            .inner()
            .scan_prefix(scan_prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))?;
        // Flat writes now land at `atom\0{uuid}`. `ATOM_PREFIX` is still the
        // colon catalog (`atom:`), so a one-form scan misses every new body
        // (and every `{org}:atom\0…` row). Walk the kind twin inside this
        // storage prefix only — never the unprefixed personal tree.
        if let Some(twin) = crate::kind_partition::form_twin(&scan_prefix) {
            let extra = self
                .main_store
                .inner()
                .scan_prefix(twin.as_bytes())
                .await
                .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))?;
            scanned = crate::kind_partition::merge_scan_rows(&scan_prefix, scanned, extra);
        }

        let mut index_items: Vec<(String, Value)> = Vec::with_capacity(scanned.len() + 1);
        let mut matching: Vec<Atom> = Vec::new();
        for (_, raw) in scanned {
            let atom = self.decode_atom_bytes(&raw).await?;
            let index_key = build_storage_key(
                storage_prefix,
                &schema_index_codec::record_key(atom.source_schema_name(), atom.uuid()),
            );
            if atom.source_schema_name() == schema_name {
                matching.push(atom);
            }
            index_items.push((index_key, Value::Bool(true)));
        }
        index_items.push((
            build_storage_key(storage_prefix, schema_index_codec::BACKFILL_SENTINEL),
            Value::Bool(true),
        ));

        self.schema_index_store
            .batch_put_items(index_items)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("Failed to back-fill schema index: {e}"))
            })?;

        Ok(matching)
    }

    /// Delete all `schemaidx:` keys (and the backfill sentinel) in a storage
    /// scope. Canonical `atom:` rows are left alone; a later list call will
    /// rebuild the index for that scope.
    pub async fn purge_schema_secondary_index(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(u64, u64), SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &crate::kind_partition::anchored("schemaidx", ""),
        );
        let rows = self
            .schema_index_store
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan schemaidx: {e}")))?;
        let mut keys: Vec<String> = Vec::with_capacity(rows.len() + 1);
        let mut bytes = 0u64;
        for (k, v) in rows {
            bytes += k.len() as u64 + v.len() as u64;
            keys.push(String::from_utf8_lossy(&k).into_owned());
        }
        // Sentinel may sit outside the `schemaidx:` prefix name shape.
        let sentinel = build_storage_key(storage_prefix, schema_index_codec::BACKFILL_SENTINEL);
        if self
            .schema_index_store
            .exists_item(&sentinel)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("exists schemaidx sentinel: {e}")))?
        {
            keys.push(sentinel);
        }
        let count = keys.len() as u64;
        if !keys.is_empty() {
            self.schema_index_store
                .batch_delete_keys(keys)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("delete schemaidx: {e}")))?;
            let _ = self.flush().await;
        }
        Ok((count, bytes))
    }
}

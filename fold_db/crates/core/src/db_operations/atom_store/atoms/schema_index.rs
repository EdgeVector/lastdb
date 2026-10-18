use super::*;

impl AtomStore {
    /// Read items under exactly one storage scope; no bare fallback.
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

    /// List the atoms that current writes mark for one schema and scope.
    /// A read never walks other atom bodies or repairs an old format.
    pub async fn list_atoms_by_schema(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Atom>, SchemaError> {
        let mut atoms = self
            .list_atoms_by_schema_scope(schema_name, storage_prefix)
            .await?;
        atoms.sort_by(|a, b| a.uuid().cmp(b.uuid()));
        Ok(atoms)
    }

    pub(crate) async fn list_atoms_by_schema_scope(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Atom>, SchemaError> {
        let scan_prefix = build_storage_key(
            storage_prefix,
            &schema_index_codec::schema_prefix(schema_name),
        );
        let keys = self
            .schema_index_store
            .list_keys_with_prefix(&scan_prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("list schema atom markers: {e}")))?;
        // Markers name UUIDs. The ordinary locator path finds a body in its
        // actual location; it must not assume a flat physical copy exists.
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
            .map_err(|e| SchemaError::InvalidData(format!("fetch schema atoms: {e}")))?
            .into_iter()
            .flatten()
            .collect())
    }
}

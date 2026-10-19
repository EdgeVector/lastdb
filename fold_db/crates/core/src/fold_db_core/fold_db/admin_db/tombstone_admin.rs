//! Legacy tombstone flag audit over the owner socket.

use super::*;

impl FoldDB {
    /// Audit `KeyMetadata.tombstoned` against atom content, optionally stamping
    /// the flag onto legacy `mk:` records whose content is already a tombstone.
    ///
    /// `schema` narrows the walk to one schema's field molecules; `None` audits
    /// the whole store. A named schema that resolves to no molecules is an error
    /// rather than a silent whole-store scan — the difference between "that
    /// schema has nothing" and "I just read your entire database" is not one to
    /// discover from the runtime.
    ///
    /// The name may be either the stored schema name (a hash on a real home) or
    /// a `descriptive_name`. A descriptive name can match several registered
    /// schemas — successive versions of the same product schema — and all of
    /// their molecules are audited together, because a page over that data is
    /// served from all of them.
    ///
    /// `max_keys` / `after_key` bound one call and resume the next; see
    /// [`crate::db_operations::AtomStore::audit_key_tombstone_flags`].
    pub async fn audit_tombstone_flags(
        &self,
        schema: Option<&str>,
        stamp: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, FoldDbError> {
        let molecules = match schema {
            None => None,
            Some(name) => {
                let mut matched: Vec<(String, Schema)> = Vec::new();
                if let Ok(Some(schema)) = self.schema_manager().get_schema_metadata(name) {
                    matched.push((name.to_string(), schema));
                } else {
                    let all = self
                        .db_ops()
                        .get_all_schemas()
                        .await
                        .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
                    matched
                        .extend(all.into_iter().filter(|(_, schema)| {
                            schema.descriptive_name.as_deref() == Some(name)
                        }));
                }
                if matched.is_empty() {
                    // The operator named a schema; nothing resolves to it. That
                    // is a 404 about their argument, not a server fault.
                    return Err(FoldDbError::Schema(crate::schema::SchemaError::NotFound(
                        format!("unknown schema: {name}"),
                    )));
                }
                let mut map = std::collections::HashMap::new();
                for (stored_name, schema) in &matched {
                    // A stored name is a hash on a real home; the descriptive
                    // name is what the operator asked about, so label with it.
                    let label = schema.descriptive_name.as_deref().unwrap_or(stored_name);
                    for (field_name, field) in &schema.runtime_fields {
                        if let Some(uuid) = field.common().molecule_uuid() {
                            map.insert(uuid.clone(), format!("{label}.{field_name}"));
                        }
                    }
                }
                if map.is_empty() {
                    // Resolvable but not auditable: still a fact about the
                    // caller's selector, so 400 rather than 500.
                    return Err(FoldDbError::Schema(
                        crate::schema::SchemaError::InvalidField(format!(
                            "schema {name} has no field molecules to audit"
                        )),
                    ));
                }
                Some(map)
            }
        };
        self.db_ops()
            .atoms()
            .audit_key_tombstone_flags(molecules.as_ref(), stamp, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_key_tombstone_flags", e))
    }
}

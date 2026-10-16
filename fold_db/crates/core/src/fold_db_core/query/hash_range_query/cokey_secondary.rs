//! Secondary-field steps of the co-key plan: tip matching, then atom bodies.

use super::*;

impl HashRangeQueryProcessor {
    /// Fields to query: every declared field when `fields` is empty, otherwise
    /// the requested runtime fields minus record-molecule fields.
    pub(super) fn cokey_selected_fields(schema: &Schema, fields: &[String]) -> Vec<String> {
        if fields.is_empty() {
            crate::record_molecule::declared_runtime_field_names(schema)
        } else {
            fields
                .iter()
                .filter(|f| {
                    schema.runtime_fields.contains_key(f.as_str())
                        && !crate::record_molecule::is_record_molecule_field(f)
                })
                .cloned()
                .collect()
        }
    }

    /// Match every secondary field against the page keys, collecting the atom
    /// bodies still to resolve.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the co-key plan inputs; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn collect_cokey_secondary_pending(
        &self,
        schema: &mut Schema,
        secondary: Vec<String>,
        fanout_keys: &[KeyValue],
        fanout_sources: &HashMap<KeyValue, KeySource>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<Vec<SecondaryPending>, SchemaError> {
        let mut pending: Vec<SecondaryPending> = Vec::new();
        if as_of.is_some() {
            for fname in &secondary {
                // History: hydrate + rewind per storage prefix, keep page_keys.
                self.secondary_matches_as_of(
                    schema,
                    fname,
                    fanout_keys,
                    fanout_sources,
                    as_of,
                    include_tombstones,
                    &mut pending,
                )
                .await?;
            }
        } else {
            // Each field has a separate molecule. Overlap its slot load, but
            // preserve field order when collecting pending atom bodies.
            let configured = std::env::var("LASTDB_QUERY_SECONDARY_CONCURRENCY").ok();
            let concurrency =
                secondary_field_concurrency(configured.as_deref(), request_concurrency);
            let schema: &Schema = schema;
            let mut fields = stream::iter(secondary.into_iter().map(|fname| async move {
                let mut field_pending = Vec::new();
                self.secondary_matches_current(
                    schema,
                    &fname,
                    fanout_keys,
                    fanout_sources,
                    include_tombstones,
                    &mut field_pending,
                )
                .await?;
                Ok::<_, SchemaError>(field_pending)
            }))
            .buffered(concurrency);
            while let Some(field_pending) = fields.next().await {
                pending.extend(field_pending?);
            }
        }
        Ok(pending)
    }

    /// Resolve the pending atom bodies in one batch and file each value under
    /// its field and key in `result`.
    pub(super) async fn fill_cokey_secondaries(
        &self,
        schema: &Schema,
        pending: Vec<SecondaryPending>,
        include_tombstones: bool,
        result: &mut HashMap<String, HashMap<KeyValue, FieldValue>>,
    ) -> Result<(), SchemaError> {
        let resolved = self.resolve_pending_atoms(&pending).await?;

        for ((fname, kv, atom_uuid, key_meta, writer_pubkey, prefix, partition), atom) in
            pending.into_iter().zip(resolved)
        {
            let Some(atom) = atom else {
                // A present personal tip with no body is an integrity
                // skip, just as it is for the primary field. Scoped
                // reads can legitimately omit bodies never shared.
                if prefix.is_none() {
                    self.db_ops.record_unresolved_atom_skip(
                        &atom_uuid,
                        &kv,
                        crate::db_operations::core::UnresolvedAtomContext {
                            molecule_uuid: schema
                                .runtime_fields
                                .get(&fname)
                                .and_then(|field| field.common().molecule_uuid())
                                .map(String::as_str),
                            schema: Some(schema.name.as_str()),
                            field: Some(fname.as_str()),
                            atom_partition: partition.as_ref(),
                            tip_storage_key: None,
                        },
                    );
                }
                continue;
            };
            if !include_tombstones && crate::atom::is_tombstone_value(atom.content()) {
                continue;
            }
            let fv =
                Self::cokey_field_value(schema, &fname, &atom, atom_uuid, key_meta, writer_pubkey);
            result.entry(fname).or_default().insert(kv, fv);
        }
        Ok(())
    }

    /// Build the response value for one resolved secondary atom.
    fn cokey_field_value(
        schema: &Schema,
        fname: &str,
        atom: &crate::atom::Atom,
        atom_uuid: String,
        key_meta: Option<crate::atom::KeyMetadata>,
        writer_pubkey: Option<String>,
    ) -> FieldValue {
        let (source_file_name, metadata) = match key_meta {
            Some(km) => (
                km.source_file_name
                    .or_else(|| atom.source_file_name().cloned()),
                km.metadata.or_else(|| atom.metadata().cloned()),
            ),
            None => (atom.source_file_name().cloned(), atom.metadata().cloned()),
        };
        let written_at = atom
            .created_at()
            .timestamp_nanos_opt()
            .and_then(|ns| u64::try_from(ns).ok());
        let mut fv = FieldValue {
            value: atom.content().clone(),
            atom_uuid,
            source_file_name,
            metadata,
            molecule_uuid: None,
            molecule_version: None,
            writer_pubkey: writer_pubkey.filter(|s| !s.is_empty()),
            written_at,
        };
        if let Some(field) = schema.runtime_fields.get(fname) {
            fv.molecule_uuid = field.common().molecule_uuid().cloned();
            fv.molecule_version = field.molecule_version();
            if let Some(pk) = field.molecule_writer_pubkey() {
                fv.writer_pubkey = Some(pk);
            }
        }
        fv
    }
}

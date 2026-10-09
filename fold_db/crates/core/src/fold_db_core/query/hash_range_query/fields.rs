//! Field scans, predicate key matching, key ordering and exact-key queries.

use super::*;

impl HashRangeQueryProcessor {
    pub(super) fn scan_fields(
        &self,
        schema: &Schema,
        predicates: &[FieldPredicate],
        order_by: Option<&QueryOrderBy>,
    ) -> Result<Vec<String>, SchemaError> {
        let mut fields = Vec::new();
        for predicate in predicates {
            fields.push(predicate.field_name().to_string());
        }
        if let Some(order_by) = order_by {
            fields.push(order_by.field.clone());
        }
        // On a blinded home, pass A must carry the schema's hash key field even
        // when no predicate names it: the key field is what makes the co-key
        // primary recoverable to API form
        // ([`Self::recover_api_page_keys`]). Without it the pass-A primary is a
        // predicate field, whose value is not the key, so pass B would blind an
        // already-blinded token for every field and the query would return an
        // empty result set rather than the matching rows.
        if self.db_ops.atoms().key_codec().encoding() != crate::atom::HashKeyEncoding::Plain {
            if let Some(hash_field) = schema.key.as_ref().and_then(|k| k.hash_field.as_ref()) {
                if schema.runtime_fields.contains_key(hash_field.as_str()) {
                    fields.push(hash_field.clone());
                }
            }
        }
        fields.sort();
        fields.dedup();

        if fields.is_empty() {
            let Some(key_field) = schema
                .key
                .as_ref()
                .and_then(|k| k.hash_field.as_ref().or(k.range_field.as_ref()))
                .cloned()
                .or_else(|| {
                    crate::record_molecule::declared_runtime_field_names(schema)
                        .into_iter()
                        .next()
                })
            else {
                return Ok(Vec::new());
            };
            fields.push(key_field);
        }

        for field in &fields {
            if !schema.runtime_fields.contains_key(field) {
                return Err(SchemaError::InvalidField(format!(
                    "field predicate references unknown field '{field}' on schema '{}'",
                    schema.name
                )));
            }
        }
        Ok(fields)
    }

    pub(super) fn matching_field_predicate_keys(
        &self,
        pass_a: &CokeyRows,
        predicates: &[FieldPredicate],
    ) -> Result<Vec<KeyValue>, SchemaError> {
        let records = records_from_field_map(&pass_a.fields);
        let mut keys: Vec<KeyValue> = pass_a
            .page_keys
            .iter()
            .filter(|key| {
                let Some(record) = records.get(*key) else {
                    return false;
                };
                predicates
                    .iter()
                    .all(|predicate| field_predicate_matches(predicate, &record.fields))
            })
            .cloned()
            .collect();
        keys.sort_by_key(KeyValue::to_storage_key);
        Ok(keys)
    }

    pub(super) fn sort_keys_by_field(
        &self,
        keys: &mut [KeyValue],
        fields: &HashMap<String, HashMap<KeyValue, FieldValue>>,
        order_by: &QueryOrderBy,
    ) {
        let field_values = fields.get(&order_by.field);
        keys.sort_by(|a, b| {
            let av = field_values
                .and_then(|values| values.get(a))
                .map(|fv| &fv.value);
            let bv = field_values
                .and_then(|values| values.get(b))
                .map(|fv| &fv.value);
            let primary = compare_json_values(av, bv);
            let ordered = match order_by.order.as_ref() {
                Some(SortOrder::Desc) => primary.reverse(),
                _ => primary,
            };
            ordered.then_with(|| a.to_storage_key().cmp(&b.to_storage_key()))
        });
    }

    pub(super) async fn query_exact_keys(
        &self,
        schema: &mut Schema,
        fields: &[String],
        page_keys: &[KeyValue],
        key_sources: &HashMap<KeyValue, KeySource>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        // lint:fn-size-ok verbatim move from hash_range_query.rs; splitting this function is separate work
        let mut selected: Vec<String> = if fields.is_empty() {
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
        };
        // Pass B formats records from the field maps it hydrates. Carry the key
        // field so a kept row with none of the projected values still has an
        // identity map entry and cannot disappear after satisfying `Absent`.
        if let Some(key_field) = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
        {
            if schema.runtime_fields.contains_key(key_field.as_str())
                && !selected.iter().any(|field| field == key_field)
            {
                selected.push(key_field.clone());
            }
        }
        let mut result: HashMap<String, HashMap<KeyValue, FieldValue>> = selected
            .iter()
            .map(|field| (field.clone(), HashMap::new()))
            .collect();
        if selected.is_empty() || page_keys.is_empty() {
            return Ok(result);
        }

        let mut pending: Vec<SecondaryPending> = Vec::new();
        for fname in &selected {
            if as_of.is_some() {
                self.secondary_matches_as_of(
                    schema,
                    fname,
                    page_keys,
                    key_sources,
                    as_of,
                    include_tombstones,
                    &mut pending,
                )
                .await?;
            } else {
                self.secondary_matches_current(
                    schema,
                    fname,
                    page_keys,
                    key_sources,
                    include_tombstones,
                    &mut pending,
                )
                .await?;
            }
        }

        if pending.is_empty() {
            return Ok(result);
        }

        let atoms = self
            .resolve_pending_atoms(&pending)
            .await
            .map_err(|e| SchemaError::InvalidField(format!("exact-key atom batch: {e}")))?;

        for ((fname, kv, atom_uuid, key_meta, writer_pubkey, prefix, partition), atom) in
            pending.into_iter().zip(atoms)
        {
            let Some(atom) = atom else {
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
            if let Some(field) = schema.runtime_fields.get(&fname) {
                fv.molecule_uuid = field.common().molecule_uuid().cloned();
                fv.molecule_version = field.molecule_version();
                if let Some(pk) = field.molecule_writer_pubkey() {
                    fv.writer_pubkey = Some(pk);
                }
            }
            result.entry(fname).or_default().insert(kv, fv);
        }

        Ok(result)
    }
}

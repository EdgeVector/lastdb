use super::*;

impl SchemaServiceState {
    /// Find schemas similar to the given schema using Jaccard index on field name sets
    pub fn find_similar_schemas(
        &self,
        name: &str,
        threshold: f64,
    ) -> FoldDbResult<SimilarSchemasResponse> {
        let schemas = read_lock(&self.schemas, "schemas")?;

        let target = schemas
            .get(name)
            .ok_or_else(|| FoldDbError::Config(format!("Schema '{name}' not found")))?;

        let target_fields = collect_field_names(target);

        let system_set: HashSet<String> = self
            .system_schema_hashes
            .read()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let mut similar: Vec<SimilarSchemaEntry> = schemas
            .iter()
            .filter(|(k, _)| k.as_str() != name)
            .filter_map(|(schema_name, schema)| {
                let other_fields = collect_field_names(schema);
                let similarity = jaccard_index(&target_fields, &other_fields);
                if similarity >= threshold {
                    Some(SimilarSchemaEntry {
                        schema: SchemaEnvelope {
                            schema: schema.clone(),
                            system: system_set.contains(schema_name),
                        },
                        similarity,
                    })
                } else {
                    None
                }
            })
            .collect();

        similar.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(SimilarSchemasResponse {
            query_schema: name.to_string(),
            threshold,
            similar_schemas: similar,
        })
    }

    /// Batch check whether proposed schemas can reuse existing ones.
    ///
    /// For each entry, finds a matching descriptive name (exact or semantic),
    /// resolves to the active (non-deprecated) schema, computes field rename
    /// maps, and determines if the existing schema is a superset.
    ///
    /// Read-only operation — only acquires read locks.
    pub fn batch_check_schema_reuse(
        &self,
        entries: &[SchemaLookupEntry],
    ) -> FoldDbResult<HashMap<String, SchemaReuseMatch>> {
        let mut results = HashMap::new();

        let schemas = read_lock(&self.schemas, "schemas_cache")?;

        for entry in entries {
            // 1. Find matching descriptive name (exact or semantic).
            // `SchemaLookupEntry` doesn't carry an `owner_app_id` (batch reuse
            // operates on the un-owned/legacy namespace), so the search is
            // scoped to `owner_app_id == None`.
            let (matched_desc, matched_hash, is_exact) =
                match self.find_matching_descriptive_name(&entry.descriptive_name, None) {
                    Ok((Some(desc), Some(hash), exact)) => (desc, hash, exact),
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(
                        target: "schema_service::schema",
                                        "batch_check_schema_reuse: error matching '{}': {}",
                                        entry.descriptive_name,
                                        e
                                    );
                        continue;
                    }
                };

            // 2. Resolve to active (non-deprecated) schema
            let Some(existing) = schemas.get(&matched_hash) else {
                continue;
            };
            let (active_schema, _active_name) =
                match self.resolve_active_schema(existing, &matched_hash, &schemas) {
                    Some(pair) => pair,
                    None => (existing.clone(), matched_hash.clone()),
                };

            // 3. Get the active schema's fields
            let existing_fields: Vec<String> =
                active_schema.fields.as_ref().cloned().unwrap_or_default();

            // 4. Compute semantic field rename map
            let field_rename_map = self.semantic_field_rename_map(
                &entry.fields,
                &existing_fields,
                &entry.descriptive_name,
                &HashMap::new(),
                &active_schema.field_descriptions,
            );

            // 5. Determine superset status and unmapped fields
            let existing_set: HashSet<&String> = existing_fields.iter().collect();
            let mut unmapped = Vec::new();
            for f in &entry.fields {
                if !existing_set.contains(f) && !field_rename_map.contains_key(f) {
                    unmapped.push(f.clone());
                }
            }
            let is_superset = unmapped.is_empty();

            let system = self.is_system_schema(&active_schema.name);
            results.insert(
                entry.descriptive_name.clone(),
                SchemaReuseMatch {
                    schema: SchemaEnvelope {
                        schema: active_schema,
                        system,
                    },
                    matched_descriptive_name: matched_desc,
                    is_exact_match: is_exact,
                    field_rename_map,
                    is_superset,
                    unmapped_fields: unmapped,
                },
            );
        }

        Ok(results)
    }
}

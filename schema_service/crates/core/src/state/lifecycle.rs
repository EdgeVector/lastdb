use super::*;

impl SchemaServiceState {
    /// Repair existing duplicate `descriptive_name` groups in the registry by
    /// keeping the active schema with the largest field set as the survivor
    /// and marking the rest as `superseded_by` the survivor. Updates the
    /// in-memory state and persists the survivors' siblings via the storage
    /// backend so the next cold start sees a consistent registry.
    ///
    /// Returns a per-group summary the caller can echo back to an operator.
    /// Idempotent — re-running on a clean registry is a no-op.
    ///
    /// This is the one-shot dev cleanup for the duplicate pile produced by
    /// the pre-409 cross-schema_type fall-through (see
    /// `state_expansion::is_cross_schema_type_expansion`) and by concurrent
    /// Lambda races. It does NOT delete data — superseded schemas are
    /// preserved so any in-flight writes that still address them keep
    /// resolving via the active-redirect path.
    pub async fn dedupe_descriptive_names(&self) -> FoldDbResult<Vec<DescriptiveNameDedupeGroup>> {
        // Collect groups of ((owner_app_id, descriptive_name), [(hash,
        // field_count)]) for all active schemas — sort descending by field
        // count + lexically by hash so the survivor pick is deterministic
        // across runs. Grouping by `(owner_app_id, descriptive_name)` means a
        // seed `Project` and an `fbrain/Project` are NOT deduped against each
        // other (app_identity v3.1, Lane B2b) — only same-owner duplicates
        // collapse.
        // (owner_app_id, descriptive_name) → [(identity_hash, field_count)].
        // Aliased for readability — clippy::type_complexity flags the raw form.
        type DedupeGroupKey = (Option<String>, String);
        type DedupeEntries = Vec<(String, usize)>;
        let groups: Vec<(Option<String>, String, DedupeEntries)> = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            let mut by_desc: HashMap<DedupeGroupKey, DedupeEntries> = HashMap::new();
            for (name, schema) in schemas.iter() {
                if schema.superseded_by.is_some() {
                    continue;
                }
                if let Some(ref desc) = schema.descriptive_name {
                    let field_count = schema.fields.as_ref().map_or(0, std::vec::Vec::len);
                    by_desc
                        .entry((schema.owner_app_id.clone(), desc.clone()))
                        .or_default()
                        .push((name.clone(), field_count));
                }
            }
            by_desc
                .into_iter()
                .filter(|(_, entries)| entries.len() > 1)
                .map(|((owner, desc), entries)| (owner, desc, entries))
                .collect()
        };

        let mut report = Vec::with_capacity(groups.len());
        for (owner_app_id, descriptive_name, mut entries) in groups {
            // Survivor: largest field count, tiebreak by lexical hash for
            // determinism.
            entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let (survivor_hash, survivor_field_count) = entries[0].clone();
            let losers: Vec<String> = entries[1..].iter().map(|(h, _)| h.clone()).collect();

            // Mark losers superseded_by survivor in memory; collect the
            // post-mutation clones for persistence outside the lock.
            let to_persist: Vec<Schema> = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                losers
                    .iter()
                    .filter_map(|loser_hash| {
                        let s = schemas.get_mut(loser_hash)?;
                        s.superseded_by = Some(survivor_hash.clone());
                        Some(s.clone())
                    })
                    .collect()
            };

            self.persist_schemas(&to_persist).await?;

            // Keep the descriptive_name_index pointing at the survivor under
            // the namespaced key so app-owned and un-owned same-name groups
            // stay independent.
            {
                let index_key = descriptive_name_key(owner_app_id.as_deref(), &descriptive_name);
                let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
                index.insert(index_key, survivor_hash.clone());
            }

            tracing::info!(
                target: "schema_service::dedupe",
                descriptive_name = %descriptive_name,
                owner_app_id = ?owner_app_id,
                survivor = %survivor_hash,
                survivor_field_count,
                losers_count = losers.len(),
                "Deduped descriptive_name group: kept largest field set as survivor",
            );

            report.push(DescriptiveNameDedupeGroup {
                descriptive_name,
                survivor: survivor_hash,
                survivor_field_count,
                superseded: losers,
            });
        }

        // Advance the snapshot version when the cleanup actually flipped
        // `superseded_by` markers and rewrote `descriptive_name_index`
        // entries. Clients poll `state_version` via `GET /v1/snapshot` to
        // decide whether to re-fetch; without the bump a pre-dedupe cache
        // would silently keep showing loser hashes as active and resolve
        // the descriptive_name against the stale index slot. An empty
        // report means the registry was already clean, so we skip the
        // bump there to avoid churning the version on idempotent re-runs.
        if !report.is_empty() {
            self.bump_state_version();
        }

        Ok(report)
    }

    /// Mark selected schemas inactive without deleting their immutable records.
    ///
    /// This is the surgical cleanup path for bad registry entries. It uses the
    /// same `superseded_by.is_some()` inactive convention as schema expansion
    /// and dedupe, but self-references the deprecated schema when there is no
    /// replacement survivor. The descriptive-name index entry is removed only
    /// when it still points at the deprecated schema, so newer active schemas
    /// in the same namespace are not disturbed.
    pub async fn deprecate_schemas(
        &self,
        request: DeprecateSchemasRequest,
    ) -> FoldDbResult<DeprecateSchemasResponse> {
        let owner_app_id = request
            .owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|owner| !owner.is_empty())
            .map(str::to_string);

        let mut targets: Vec<(String, String)> = Vec::new();
        for name in request.schema_names {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                targets.push((trimmed.to_string(), trimmed.to_string()));
            }
        }

        let mut not_found = Vec::new();
        {
            let index = read_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            for desc in request.descriptive_names {
                let trimmed = desc.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let key = descriptive_name_key(owner_app_id.as_deref(), trimmed);
                if let Some(hash) = index.get(&key) {
                    targets.push((hash.clone(), trimmed.to_string()));
                } else {
                    let schemas = read_lock(&self.schemas, "schemas")?;
                    let fallback = schemas
                        .iter()
                        .find(|(_, schema)| {
                            schema.superseded_by.is_none()
                                && schema.descriptive_name.as_deref() == Some(trimmed)
                                && normalize_owner(schema.owner_app_id.as_deref())
                                    == normalize_owner(owner_app_id.as_deref())
                        })
                        .map(|(hash, _)| hash.clone());
                    match fallback {
                        Some(hash) => targets.push((hash, trimmed.to_string())),
                        None => not_found.push(trimmed.to_string()),
                    }
                }
            }
        }

        let mut seen = HashSet::new();
        targets.retain(|(hash, _)| seen.insert(hash.clone()));

        let mut deprecated = Vec::new();
        let mut changed = Vec::new();
        for (hash, label) in targets {
            let (entry, maybe_changed): (DeprecatedSchemaEntry, Option<Schema>) = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                let Some(schema) = schemas.get_mut(&hash) else {
                    not_found.push(label);
                    continue;
                };
                let already_deprecated = schema.superseded_by.is_some();
                if !already_deprecated {
                    schema.superseded_by = Some(hash.clone());
                }
                (
                    DeprecatedSchemaEntry {
                        schema_name: hash.clone(),
                        descriptive_name: schema.descriptive_name.clone(),
                        already_deprecated,
                    },
                    (!already_deprecated).then(|| schema.clone()),
                )
            };

            if let Some(schema) = maybe_changed {
                changed.push(schema);
            }
            deprecated.push(entry);
        }

        if !changed.is_empty() {
            self.persist_schemas(&changed).await?;
            let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            for schema in &changed {
                if let Some(desc) = schema.descriptive_name.as_deref() {
                    let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc);
                    if index.get(&key) == Some(&schema.name) {
                        index.remove(&key);
                    }
                }
            }
            self.bump_state_version();
        }

        Ok(DeprecateSchemasResponse {
            deprecated,
            not_found,
        })
    }
}

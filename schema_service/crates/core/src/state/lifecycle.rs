use super::*;
use serde_json::{json, Value};

/// The descriptive-name dedupe both HTTP routers answer.
#[derive(Debug)]
pub enum DedupeNamesReply {
    /// The cleanup finished. HTTP 200. The value is `{ "groups": [...] }`.
    Ready(Value),
    /// The cleanup failed. HTTP 500. `message` is the log line. `body` is
    /// `{ "error": message }`.
    Failed { message: String, body: Value },
}

impl DedupeNamesReply {
    /// The HTTP status, body, and optional log line.
    ///
    /// Ready is 200 and has no log line. Failed is 500. The log line is the
    /// actix error text. The Lambda router ignores that line.
    #[must_use]
    pub fn into_http(self) -> (u16, Value, Option<String>) {
        match self {
            Self::Ready(body) => (200, body, None),
            Self::Failed { message, body } => (500, body, Some(message)),
        }
    }
}

/// The schema deprecation both HTTP routers answer.
#[derive(Debug)]
pub enum DeprecateSchemasReply {
    /// The deprecation finished. HTTP 200. The value is the report.
    Ready(Value),
    /// The deprecation failed. HTTP 500. `message` is the log line. `body` is
    /// `{ "error": message }`.
    Failed { message: String, body: Value },
}

impl DeprecateSchemasReply {
    /// The HTTP status, body, and optional log line.
    ///
    /// Ready is 200 and has no log line. Failed is 500. The log line is the
    /// actix error text. The Lambda router ignores that line.
    #[must_use]
    pub fn into_http(self) -> (u16, Value, Option<String>) {
        match self {
            Self::Ready(body) => (200, body, None),
            Self::Failed { message, body } => (500, body, Some(message)),
        }
    }
}

/// `(owner_app_id, descriptive_name, [(identity_hash, field_count)])`.
type DuplicateGroup = (Option<String>, String, Vec<(String, usize)>);
/// `(schema_name, label)` of a schema selected for deprecation.
type DeprecationTarget = (String, String);

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
    /// the old cross-schema_type fall-through and by concurrent Lambda races.
    /// It does NOT delete data — superseded schemas are
    /// preserved so any in-flight writes that still address them keep
    /// resolving via the active-redirect path.
    pub async fn dedupe_descriptive_names(&self) -> FoldDbResult<Vec<DescriptiveNameDedupeGroup>> {
        let groups = self.duplicate_descriptive_name_groups()?;

        let mut report = Vec::with_capacity(groups.len());
        for (owner_app_id, descriptive_name, entries) in groups {
            report.push(
                self.dedupe_group(owner_app_id, descriptive_name, entries)
                    .await?,
            );
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

    /// Classify [`Self::dedupe_descriptive_names`] for the HTTP cleanup.
    ///
    /// `Ready` is `{ "groups": [...] }`. `Failed` keeps the same error text
    /// both routers return. The Actix router still writes the log.
    #[must_use]
    pub async fn dedupe_names_reply(&self) -> DedupeNamesReply {
        match self.dedupe_descriptive_names().await {
            Ok(groups) => DedupeNamesReply::Ready(json!({ "groups": groups })),
            Err(error) => {
                let message = format!("dedupe-descriptive-names failed: {error}");
                let body = crate::error_only_body(&message);
                DedupeNamesReply::Failed { message, body }
            }
        }
    }

    /// Active schemas that share `(owner_app_id, descriptive_name)`, with
    /// `(identity_hash, field_count)` for each member.
    fn duplicate_descriptive_name_groups(&self) -> FoldDbResult<Vec<DuplicateGroup>> {
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
        let groups: Vec<DuplicateGroup> = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            let mut by_desc: HashMap<DedupeGroupKey, Vec<(String, usize)>> = HashMap::new();
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
        Ok(groups)
    }

    /// Keep the largest schema of one duplicate group and supersede the rest.
    async fn dedupe_group(
        &self,
        owner_app_id: Option<String>,
        descriptive_name: String,
        mut entries: Vec<(String, usize)>,
    ) -> FoldDbResult<DescriptiveNameDedupeGroup> {
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

        Ok(DescriptiveNameDedupeGroup {
            descriptive_name,
            survivor: survivor_hash,
            survivor_field_count,
            superseded: losers,
        })
    }

    /// Map a deprecate request to unique `(schema_name, label)` targets plus
    /// the labels that matched no active schema.
    fn resolve_deprecation_targets(
        &self,
        request: DeprecateSchemasRequest,
    ) -> FoldDbResult<(Vec<DeprecationTarget>, Vec<String>)> {
        let owner_app_id = request
            .owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|owner| !owner.is_empty())
            .map(str::to_string);

        let mut targets: Vec<DeprecationTarget> = Vec::new();
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
        Ok((targets, not_found))
    }

    /// Persist newly deprecated schemas and drop index entries that still
    /// point at them.
    async fn retire_deprecated_index_entries(&self, changed: &[Schema]) -> FoldDbResult<()> {
        if changed.is_empty() {
            return Ok(());
        }
        self.persist_schemas(changed).await?;
        let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
        for schema in changed {
            if let Some(desc) = schema.descriptive_name.as_deref() {
                let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc);
                if index.get(&key) == Some(&schema.name) {
                    index.remove(&key);
                }
            }
        }
        self.bump_state_version();
        Ok(())
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
        let (targets, mut not_found) = self.resolve_deprecation_targets(request)?;

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

        self.retire_deprecated_index_entries(&changed).await?;

        Ok(DeprecateSchemasResponse {
            deprecated,
            not_found,
        })
    }

    /// Classify [`Self::deprecate_schemas`] for the HTTP cleanup.
    ///
    /// `Ready` is the report. `Failed` keeps the same error text both routers
    /// return. The Actix router still writes the log.
    #[must_use]
    pub async fn deprecate_schemas_reply(
        &self,
        request: DeprecateSchemasRequest,
    ) -> DeprecateSchemasReply {
        match self.deprecate_schemas(request).await {
            Ok(report) => DeprecateSchemasReply::Ready(json!(report)),
            Err(error) => {
                let message = format!("deprecate-schemas failed: {error}");
                let body = crate::error_only_body(&message);
                DeprecateSchemasReply::Failed { message, body }
            }
        }
    }
}

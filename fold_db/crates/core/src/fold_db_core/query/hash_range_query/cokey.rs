//! The single co-key query plan: primary key set, then secondary field resolution.

use super::*;

impl HashRangeQueryProcessor {
    /// Co-key plan: primary defines keys; secondaries fill those keys only.
    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn query_cokey(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_cokey_with_sources(
            schema,
            fields,
            filter,
            window,
            as_of,
            include_tombstones,
            received_from_namespaces,
            request_concurrency,
        )
        .await
        .map(|rows| rows.fields)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn query_cokey_with_sources(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
        request_concurrency: Option<usize>,
    ) -> Result<CokeyRows, SchemaError> {
        // lint:fn-size-ok verbatim move from hash_range_query.rs; splitting this function is separate work
        let selected: Vec<String> = if fields.is_empty() {
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
        if selected.is_empty() {
            return Ok(CokeyRows {
                fields: HashMap::new(),
                page_keys: Vec::new(),
                key_sources: HashMap::new(),
            });
        }

        let mut primary_name = Self::cokey_primary_field(schema, &selected);
        let mut result: HashMap<String, HashMap<KeyValue, FieldValue>> = HashMap::new();

        // --- 1. Primary: full resolve under filter / as_of / namespaces ---
        // The primary establishes the page of keys; secondaries only fill those
        // keys. So windowing the primary windows the whole row set, and the
        // secondary fan-out below shrinks with it for free.
        let (primary_vals, key_sources) = self
            .resolve_primary(
                schema,
                &primary_name,
                filter.clone(),
                window.clone(),
                as_of,
                include_tombstones,
                received_from_namespaces,
            )
            .await?;
        // The walk hands back storage-form keys. Rename the page here, at the one
        // boundary that holds the mapping, so the rest of the query — and the
        // caller — only ever sees API form. Two reasons it has to happen here and
        // not per-consumer: secondaries blind again, so they must be fanned out on
        // API keys or every one of them misses; and the primary's key becomes the
        // response's `key.hash`, so a storage-form key that escapes is a key the
        // caller cannot point-read back.
        let (mut primary_vals, mut page_keys, mut key_sources) =
            self.rename_page_to_api_keys(schema, &primary_name, primary_vals, key_sources);

        // Shared-molecule multi-key (graph-edge planes): one molecule holds
        // tips for both key layouts. HashKey(slug) therefore lists every tip
        // whose storage hash is slug, inbound and outbound. Keep only tips
        // whose *named* hash-field value equals that slug so schema S returns
        // S's layout. Stage 0 dump 2026-09-02: all GRAPH_EDGE_FIELDS share
        // molecule_uuid across BySource/ByDestination; HashKey and empty
        // Prefix both returned the union (12 = 5 out + 7 in).
        let had_named_spine_keys = !page_keys.is_empty();
        Self::retain_named_layout_primary(
            &filter,
            &mut primary_vals,
            &mut page_keys,
            &mut key_sources,
        );

        // A schema key field is the dense row spine for rows written through
        // that layout. Multi-key expand is the exception: a sibling layout can
        // share projected field hashes without ever having atoms under its own
        // key field. Prefer the key spine, then fall back to the projection only
        // when that spine is empty so sibling-key reads remain addressable.
        // Do not fall back after dropping sibling-layout tips: that re-opens
        // the union when the caller omitted the hash field from `selected`.
        let (primary_vals, page_keys, key_sources) = if page_keys.is_empty()
            && !had_named_spine_keys
            && !selected.iter().any(|field| field == &primary_name)
        {
            let fallback = selected[0].clone();
            let (values, sources) = self
                .resolve_primary(
                    schema,
                    &fallback,
                    filter,
                    window,
                    as_of,
                    include_tombstones,
                    received_from_namespaces,
                )
                .await?;
            primary_name = fallback;
            self.rename_page_to_api_keys(schema, &primary_name, values, sources)
        } else {
            (primary_vals, page_keys, key_sources)
        };
        result.insert(primary_name.clone(), primary_vals);

        // F=1: done. Empty secondaries still get empty maps when multi-field
        // and page is empty so clients see a complete field set.
        if selected.len() == 1 && selected[0] == primary_name {
            tracing::debug!(
                primary = %primary_name,
                keys = page_keys.len(),
                "HashRangeQueryProcessor::query_cokey: single-field done"
            );
            return Ok(CokeyRows {
                fields: result,
                page_keys,
                key_sources,
            });
        }

        if page_keys.is_empty() {
            for fname in &selected {
                if fname != &primary_name {
                    result.entry(fname.clone()).or_default();
                }
            }
            return Ok(CokeyRows {
                fields: result,
                page_keys,
                key_sources,
            });
        }

        // --- 2. Secondary fields for page_keys only ---
        // The page arrived already renamed, so the fan-out keys are just the page
        // keys: a secondary lands on the very `KeyValue` the primary is filed
        // under. That identity is what keeps a row whole — land a secondary on a
        // different key and the row splits in two (one fully populated, one blank
        // shell, the shape the board scan showed).
        let fanout_keys: &[KeyValue] = &page_keys;
        let fanout_sources: &HashMap<KeyValue, KeySource> = &key_sources;

        let mut pending: Vec<SecondaryPending> = Vec::new();

        let secondary: Vec<String> = selected
            .iter()
            .filter(|fname| *fname != &primary_name)
            .cloned()
            .collect();
        for fname in &secondary {
            result.entry(fname.clone()).or_default();
        }
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
            let schema_ref: &Schema = schema;
            let mut fields = stream::iter(secondary.into_iter().map(|fname| async move {
                let mut field_pending = Vec::new();
                self.secondary_matches_current(
                    schema_ref,
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

        // --- 3. Atom batches for secondary bodies (grouped by storage prefix) ---
        if !pending.is_empty() {
            let resolved = self.resolve_pending_atoms(&pending).await.map_err(|e| {
                SchemaError::InvalidField(format!("co-key secondary atom batch: {e}"))
            })?;

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
        }

        tracing::debug!(
            fields = result.len(),
            page_keys = page_keys.len(),
            "HashRangeQueryProcessor::query_cokey: done"
        );
        Ok(CokeyRows {
            fields: result,
            page_keys,
            key_sources,
        })
    }
}

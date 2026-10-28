//! Resolver input construction and the embedding memo.

use super::*;

impl SchemaServiceState {
    /// Embed `text` through the process-local memo. Registry texts repeat on
    /// every resolve, so only the first request on a warm process pays the
    /// model; later requests read the vector back.
    pub(super) fn memo_embed_text(
        &self,
        text: &str,
        memo: &mut MemoStats,
    ) -> Result<Vec<f32>, String> {
        if let Ok(cache) = self.native_resolve_embeddings.read() {
            if let Some(vec) = cache.get(text) {
                memo.hits += 1;
                return Ok(vec.clone());
            }
        }
        let vec = self.embedder.embed_text(text).map_err(|e| e.to_string())?;
        memo.misses += 1;
        if let Ok(mut cache) = self.native_resolve_embeddings.write() {
            cache.insert(text.to_string(), vec.clone());
        }
        Ok(vec)
    }

    /// Build the full resolver input for `proposal`: embed the active
    /// registry (within the time budget), the proposal, and the canonical
    /// field catalog.
    pub(super) fn build_native_resolver_input(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> Result<BuiltNativeInput, String> {
        let config = ResolverConfig::embedding_beam_shadow_defaults();
        let schemas = read_lock(&self.schemas, "schemas_cache").map_err(|e| e.to_string())?;
        let budget = native_resolve_budget();
        let started = Instant::now();
        let mut memo = MemoStats::default();

        let registry = self.embed_registry_schemas(
            &schemas,
            config.limits.max_registry_schemas as usize,
            started,
            budget,
            &mut memo,
        )?;
        if registry.schemas.is_empty() {
            return Err("empty registry".into());
        }

        let proposal_part = self.embed_proposal(proposal)?;
        let (canonical_meta, canonical_emb) =
            self.embed_canonical_fields(started, budget, &mut memo)?;

        tracing::debug!(
            target: "schema_service::schema",
            descriptive_name = %proposal.descriptive_name,
            elapsed_ms = started.elapsed().as_millis() as u64,
            model_calls = memo.misses,
            memo_hits = memo.hits,
            "native_component_cover: resolver input built"
        );

        let input = NativeResolverInput {
            resolver_contract_version: SCHEMA_RESOLVER_ABI_VERSION,
            proposal_metadata: ProposalMetadata {
                proposal_id: proposal_part.proposal_id,
                descriptive_name: proposal.descriptive_name.clone(),
                fields: proposal_part.fields,
            },
            proposal_embeddings: ProposalEmbeddings {
                descriptive_name: proposal_part.name_embedding,
                field_contexts: proposal_part.field_embeddings,
            },
            registry_metadata: RegistryMetadata {
                schemas: registry.schemas,
                fields: registry.fields,
                canonical_fields: canonical_meta,
            },
            registry_embeddings: RegistryEmbeddings {
                descriptive_names: registry.name_embeddings,
                schema_field_contexts: registry.field_embeddings,
                canonical_fields: canonical_emb,
            },
            config,
        };

        Ok(BuiltNativeInput { input })
    }

    /// Embed every active (non-superseded) registry schema name and field
    /// context, up to `max_schemas` schemas, checking the budget after each
    /// model call.
    fn embed_registry_schemas(
        &self,
        schemas: &HashMap<String, Schema>,
        max_schemas: usize,
        started: Instant,
        budget: Duration,
        memo: &mut MemoStats,
    ) -> Result<RegistryEmbedded, String> {
        let mut out = RegistryEmbedded::default();
        let mut count = 0usize;

        for (identity, schema) in schemas {
            if schema.superseded_by.is_some() {
                continue;
            }
            if count >= max_schemas {
                break;
            }
            let schema_id = schema
                .identity_hash
                .clone()
                .unwrap_or_else(|| identity.clone());
            let descriptive = schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema_id.clone());

            // Optional owner scoping: when proposal has owner_app_id, prefer
            // same-owner + unowned schemas for the registry shortlist. Still
            // include others so component cover can reuse global shapes.
            let _owner = schema.owner_app_id.as_deref();

            let name_vec = self
                .memo_embed_text(&descriptive, memo)
                .map_err(|e| format!("embed schema name: {e}"))?;
            ensure_within_budget(started, budget, memo)?;
            out.name_embeddings.push(EmbeddingVectorRecord {
                target_id: schema_id.clone(),
                vector: name_vec,
            });

            self.embed_schema_fields(
                schema,
                &schema_id,
                &descriptive,
                started,
                budget,
                memo,
                &mut out,
            )?;

            out.schemas.push(RegistrySchemaHandle {
                schema_id,
                descriptive_name: descriptive,
                lifecycle: "active".to_string(),
            });
            count += 1;
        }
        Ok(out)
    }

    /// Embed the field contexts of one registry schema into `out`.
    #[allow(clippy::too_many_arguments)]
    fn embed_schema_fields(
        &self,
        schema: &Schema,
        schema_id: &str,
        descriptive: &str,
        started: Instant,
        budget: Duration,
        memo: &mut MemoStats,
        out: &mut RegistryEmbedded,
    ) -> Result<(), String> {
        let field_names = schema.fields.clone().unwrap_or_default();
        for field_name in &field_names {
            let field_id = format!("{schema_id}#{field_name}");
            let ftype = schema.get_field_type(field_name).to_string();
            out.fields.push(RegistryFieldHandle {
                field_id: field_id.clone(),
                schema_id: schema_id.to_string(),
                field_name: field_name.clone(),
                field_type: ftype,
                canonical_field_ids: Vec::new(),
            });
            let desc = schema
                .field_descriptions
                .get(field_name)
                .map(String::as_str);
            let text = field_context_text(descriptive, field_name, desc);
            let fvec = self
                .memo_embed_text(&text, memo)
                .map_err(|e| format!("embed field context: {e}"))?;
            ensure_within_budget(started, budget, memo)?;
            out.field_embeddings.push(EmbeddingVectorRecord {
                target_id: field_id,
                vector: fvec,
            });
        }
        Ok(())
    }

    /// Build the proposal id, field metadata and embeddings. Proposal text
    /// bypasses the memo: it is unique per request.
    fn embed_proposal(&self, proposal: &SchemaResolveProposal) -> Result<ProposalEmbedded, String> {
        let proposal_id = proposal
            .identity_hash
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("prop:{}", proposal.descriptive_name));

        if proposal.fields.is_empty() {
            return Err("proposal has no fields".into());
        }

        let mut fields = Vec::with_capacity(proposal.fields.len());
        let mut field_embeddings = Vec::with_capacity(proposal.fields.len());
        for (i, name) in proposal.fields.iter().enumerate() {
            let pfid = proposal_field_id(i, name);
            fields.push(ProposalFieldMetadata {
                proposal_field_id: pfid.clone(),
                field_name: name.clone(),
                field_type: "String".to_string(),
                required: true,
            });
            let desc = proposal.field_descriptions.get(name).map(String::as_str);
            let text = field_context_text(&proposal.descriptive_name, name, desc);
            let fvec = self
                .embedder
                .embed_text(&text)
                .map_err(|e| format!("embed proposal field: {e}"))?;
            field_embeddings.push(EmbeddingVectorRecord {
                target_id: pfid,
                vector: fvec,
            });
        }

        let name_embedding = self
            .embedder
            .embed_text(proposal.descriptive_name.trim())
            .map_err(|e| format!("embed proposal name: {e}"))?;

        Ok(ProposalEmbedded {
            proposal_id,
            fields,
            field_embeddings,
            name_embedding,
        })
    }

    /// Canonical fields from the in-memory catalog (types only; embeddings
    /// optional).
    fn embed_canonical_fields(
        &self,
        started: Instant,
        budget: Duration,
        memo: &mut MemoStats,
    ) -> Result<
        (
            Vec<RegistryCanonicalFieldHandle>,
            Vec<EmbeddingVectorRecord>,
        ),
        String,
    > {
        let mut canonical_meta = Vec::new();
        let mut canonical_emb = Vec::new();
        if let Ok(cf) = self.canonical_fields.read() {
            for (name, field) in cf.iter() {
                if crate::field_name_policy::invalid_canonical_name(name) {
                    continue;
                }
                let cid = format!("field:{name}");
                canonical_meta.push(RegistryCanonicalFieldHandle {
                    canonical_field_id: cid.clone(),
                    field_type: field.field_type.to_string(),
                });
                if let Ok(vec) = self.memo_embed_text(name, memo) {
                    canonical_emb.push(EmbeddingVectorRecord {
                        target_id: cid,
                        vector: vec,
                    });
                }
                ensure_within_budget(started, budget, memo)?;
            }
        }
        Ok((canonical_meta, canonical_emb))
    }
}

/// Registry handles and embeddings produced by `embed_registry_schemas`.
#[derive(Default)]
struct RegistryEmbedded {
    schemas: Vec<RegistrySchemaHandle>,
    fields: Vec<RegistryFieldHandle>,
    name_embeddings: Vec<EmbeddingVectorRecord>,
    field_embeddings: Vec<EmbeddingVectorRecord>,
}

/// Proposal metadata and embeddings produced by `embed_proposal`.
struct ProposalEmbedded {
    proposal_id: String,
    fields: Vec<ProposalFieldMetadata>,
    field_embeddings: Vec<EmbeddingVectorRecord>,
    name_embedding: Vec<f32>,
}

/// Checked after every model call that missed the memo. A hit costs a map
/// read, so the check only matters on the slow path.
fn ensure_within_budget(
    started: Instant,
    budget: Duration,
    memo: &MemoStats,
) -> Result<(), String> {
    let elapsed = started.elapsed();
    if elapsed > budget {
        return Err(format!(
            "registry embedding exceeded budget: {}ms > {}ms after {} model calls ({} memo hits)",
            elapsed.as_millis(),
            budget.as_millis(),
            memo.misses,
            memo.hits
        ));
    }
    Ok(())
}

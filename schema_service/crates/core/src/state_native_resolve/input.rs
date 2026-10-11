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

    pub(super) fn build_native_resolver_input(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> Result<BuiltNativeInput, String> {
        let config = ResolverConfig::embedding_beam_shadow_defaults();
        let schemas = read_lock(&self.schemas, "schemas_cache").map_err(|e| e.to_string())?;
        let budget = native_resolve_budget();
        let started = Instant::now();
        let mut memo = MemoStats::default();
        // Checked after every model call that missed the memo. A hit costs
        // a map read, so the check only matters on the slow path.
        let over_budget = |memo: &MemoStats| -> Result<(), String> {
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
        };

        let mut registry_schemas = Vec::new();
        let mut registry_fields = Vec::new();
        let mut name_embeddings = Vec::new();
        let mut field_embeddings = Vec::new();

        let max_schemas = config.limits.max_registry_schemas as usize;
        let mut count = 0usize;

        for (identity, schema) in schemas.iter() {
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
                .memo_embed_text(&descriptive, &mut memo)
                .map_err(|e| format!("embed schema name: {e}"))?;
            over_budget(&memo)?;
            name_embeddings.push(EmbeddingVectorRecord {
                target_id: schema_id.clone(),
                vector: name_vec,
            });

            let field_names = schema.fields.clone().unwrap_or_default();
            for field_name in &field_names {
                let field_id = format!("{schema_id}#{field_name}");
                let ftype = schema.get_field_type(field_name).to_string();
                registry_fields.push(RegistryFieldHandle {
                    field_id: field_id.clone(),
                    schema_id: schema_id.clone(),
                    field_name: field_name.clone(),
                    field_type: ftype,
                    canonical_field_ids: Vec::new(),
                });
                let desc = schema
                    .field_descriptions
                    .get(field_name)
                    .map(String::as_str);
                let text = field_context_text(&descriptive, field_name, desc);
                let fvec = self
                    .memo_embed_text(&text, &mut memo)
                    .map_err(|e| format!("embed field context: {e}"))?;
                over_budget(&memo)?;
                field_embeddings.push(EmbeddingVectorRecord {
                    target_id: field_id,
                    vector: fvec,
                });
            }

            registry_schemas.push(RegistrySchemaHandle {
                schema_id,
                descriptive_name: descriptive,
                lifecycle: "active".to_string(),
            });
            count += 1;
        }

        if registry_schemas.is_empty() {
            return Err("empty registry".into());
        }

        let proposal_id = proposal
            .identity_hash
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("prop:{}", proposal.descriptive_name));

        if proposal.fields.is_empty() {
            return Err("proposal has no fields".into());
        }

        let mut p_fields = Vec::with_capacity(proposal.fields.len());
        let mut p_field_emb = Vec::with_capacity(proposal.fields.len());
        for (i, name) in proposal.fields.iter().enumerate() {
            let pfid = proposal_field_id(i, name);
            let ftype = "String".to_string();
            p_fields.push(ProposalFieldMetadata {
                proposal_field_id: pfid.clone(),
                field_name: name.clone(),
                field_type: ftype,
                required: true,
            });
            let desc = proposal.field_descriptions.get(name).map(String::as_str);
            let text = field_context_text(&proposal.descriptive_name, name, desc);
            let fvec = self
                .embedder
                .embed_text(&text)
                .map_err(|e| format!("embed proposal field: {e}"))?;
            p_field_emb.push(EmbeddingVectorRecord {
                target_id: pfid,
                vector: fvec,
            });
        }

        let p_name = self
            .embedder
            .embed_text(proposal.descriptive_name.trim())
            .map_err(|e| format!("embed proposal name: {e}"))?;

        // Canonical fields from in-memory catalog (types only; embeddings optional).
        let mut canonical_meta = Vec::new();
        let mut canonical_emb = Vec::new();
        if let Ok(cf) = self.canonical_fields.read() {
            for (name, field) in cf.iter() {
                let cid = format!("field:{name}");
                canonical_meta.push(RegistryCanonicalFieldHandle {
                    canonical_field_id: cid.clone(),
                    field_type: field.field_type.to_string(),
                });
                if let Ok(vec) = self.memo_embed_text(name, &mut memo) {
                    canonical_emb.push(EmbeddingVectorRecord {
                        target_id: cid,
                        vector: vec,
                    });
                }
                over_budget(&memo)?;
            }
        }

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
                proposal_id,
                descriptive_name: proposal.descriptive_name.clone(),
                fields: p_fields,
            },
            proposal_embeddings: ProposalEmbeddings {
                descriptive_name: p_name,
                field_contexts: p_field_emb,
            },
            registry_metadata: RegistryMetadata {
                schemas: registry_schemas,
                fields: registry_fields,
                canonical_fields: canonical_meta,
            },
            registry_embeddings: RegistryEmbeddings {
                descriptive_names: name_embeddings,
                schema_field_contexts: field_embeddings,
                canonical_fields: canonical_emb,
            },
            config,
        };

        Ok(BuiltNativeInput { input })
    }
}

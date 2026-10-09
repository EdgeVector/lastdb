//! Fallback, verification and telemetry helpers of the resolver pack consumer.

use super::*;

impl<S> ResolverPackConsumer<S>
where
    S: ResolverPackObjectStore,
{
    pub(super) fn fallback_or_last_known_good(
        &self,
        primary_reason: ResolverPackFallbackReason,
    ) -> Result<(ResolverPackLoadOutcome, Option<LoadedResolverPack>), ResolverPackConsumerError>
    {
        match self.try_load_last_known_good() {
            Ok(Some(loaded)) => {
                self.record_fetch("lkg_loaded");
                self.record_verification("ok_lkg");
                self.record_import("ok");
                Ok((
                    ResolverPackLoadOutcome::Loaded {
                        source: ResolverPackLoadSource::LastKnownGood,
                    },
                    Some(loaded),
                ))
            }
            Ok(None) => {
                self.record_fallback(primary_reason);
                Ok((
                    ResolverPackLoadOutcome::LiveServiceFallback {
                        reason: primary_reason,
                    },
                    None,
                ))
            }
            Err(err) => {
                // Prefer the LKG-specific failure (e.g. stale) when LKG exists
                // but is no longer usable; otherwise keep the primary reason.
                let reason = fallback_reason_for_error(&err);
                self.record_verification(reason.as_str());
                self.record_fallback(reason);
                Ok((
                    ResolverPackLoadOutcome::LiveServiceFallback { reason },
                    None,
                ))
            }
        }
    }

    /// Load last-known-good from staged cache files and re-verify hashes/signature.
    pub(super) fn try_load_last_known_good(
        &self,
    ) -> Result<Option<LoadedResolverPack>, ResolverPackConsumerError> {
        let Some(manifest_bytes) = self.cache.last_known_good_manifest_bytes()? else {
            return Ok(None);
        };
        let Some(resolver_config_bytes) =
            self.cache.last_known_good_file("resolver_config.json")?
        else {
            return Ok(None);
        };
        let Some(schema_snapshot_bytes) =
            self.cache.last_known_good_file("schema_snapshot.json")?
        else {
            return Ok(None);
        };
        let Some(embedding_artifact_bytes) =
            self.cache.last_known_good_file("embedding_artifact.json")?
        else {
            return Ok(None);
        };

        let loaded = self.finish_verified_pack(
            &manifest_bytes,
            &resolver_config_bytes,
            &schema_snapshot_bytes,
            &embedding_artifact_bytes,
            /* write_content_cache */ false,
        )?;
        Ok(Some(loaded))
    }

    pub(super) async fn load_from_manifest_bytes(
        &self,
        manifest_bytes: &[u8],
    ) -> Result<StagedLoadedPack, ResolverPackConsumerError> {
        let manifest = parse_manifest(manifest_bytes)?;
        self.reject_stale_manifest(&manifest)?;

        let resolver_config_bytes = self
            .read_or_fetch_artifact(
                ResolverPackArtifactKind::ResolverConfig,
                &manifest.resolver_config_hash,
            )
            .await?;
        let schema_snapshot_bytes = self
            .read_or_fetch_artifact(
                ResolverPackArtifactKind::SchemaSnapshot,
                &manifest.schema_snapshot_hash,
            )
            .await?;
        let embedding_artifact_bytes = self
            .read_or_fetch_artifact(
                ResolverPackArtifactKind::EmbeddingArtifact,
                &manifest.embedding_artifact_hash,
            )
            .await?;

        let loaded = self.finish_verified_pack(
            manifest_bytes,
            &resolver_config_bytes,
            &schema_snapshot_bytes,
            &embedding_artifact_bytes,
            /* write_content_cache */ true,
        )?;

        Ok(StagedLoadedPack {
            loaded,
            resolver_config_bytes,
            schema_snapshot_bytes,
            embedding_artifact_bytes,
        })
    }

    pub(super) fn finish_verified_pack(
        &self,
        manifest_bytes: &[u8],
        resolver_config_bytes: &[u8],
        schema_snapshot_bytes: &[u8],
        embedding_artifact_bytes: &[u8],
        write_content_cache: bool,
    ) -> Result<LoadedResolverPack, ResolverPackConsumerError> {
        let manifest = parse_manifest(manifest_bytes)?;
        self.reject_stale_manifest(&manifest)?;

        verify_resolver_pack_manifest(
            &manifest,
            resolver_config_bytes,
            schema_snapshot_bytes,
            embedding_artifact_bytes,
            &self.config.trusted_keys,
            self.config.env,
            &self.config.expected_embedder_id,
        )?;

        let schema_snapshot = parse_schema_snapshot_artifact(schema_snapshot_bytes)
            .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;
        let embedding_artifact = parse_embedding_artifact(embedding_artifact_bytes)
            .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;
        let registry_embeddings = import_pack_embeddings(&embedding_artifact)?;
        let resolver_config = parse_resolver_config(resolver_config_bytes)
            .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;

        if write_content_cache {
            self.cache.put_artifact(
                ResolverPackArtifactKind::ResolverConfig,
                &manifest.resolver_config_hash,
                resolver_config_bytes,
            )?;
            self.cache.put_artifact(
                ResolverPackArtifactKind::SchemaSnapshot,
                &manifest.schema_snapshot_hash,
                schema_snapshot_bytes,
            )?;
            self.cache.put_artifact(
                ResolverPackArtifactKind::EmbeddingArtifact,
                &manifest.embedding_artifact_hash,
                embedding_artifact_bytes,
            )?;
        }

        Ok(LoadedResolverPack {
            manifest,
            resolver_config,
            schema_snapshot,
            embedding_artifact,
            registry_embeddings,
        })
    }

    pub(super) async fn read_or_fetch_artifact(
        &self,
        kind: ResolverPackArtifactKind,
        hash: &str,
    ) -> Result<Vec<u8>, ResolverPackConsumerError> {
        if let Some(bytes) = self.cache.get_artifact(kind, hash)? {
            self.record_fetch("artifact_cache_hit");
            return Ok(bytes);
        }

        let key = resolver_pack_artifact_key(self.config.env, kind, hash);
        let Some(bytes) = self.store.get_object(&key).await? else {
            return Err(ResolverPackConsumerError::MissingArtifact(key));
        };
        self.record_fetch("artifact_downloaded");
        Ok(bytes)
    }

    pub(super) fn reject_stale_manifest(
        &self,
        manifest: &ResolverPackManifest,
    ) -> Result<(), ResolverPackConsumerError> {
        let Some(max_age_seconds) = self.config.max_pack_age_seconds else {
            return Ok(());
        };
        let generated_at = DateTime::parse_from_rfc3339(&manifest.generated_at)
            .map_err(|e| ResolverPackConsumerError::BadGeneratedAt(e.to_string()))?
            .with_timezone(&Utc);
        let now = self.config.now.unwrap_or_else(Utc::now);
        if now.signed_duration_since(generated_at).num_seconds() > max_age_seconds {
            return Err(ResolverPackConsumerError::StaleManifest);
        }
        Ok(())
    }

    pub(super) fn record_fetch(&self, outcome: &'static str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .fetch_outcomes
                .entry(outcome.to_string())
                .or_insert(0) += 1;
        }
    }

    pub(super) fn record_verification(&self, outcome: &str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .verification_outcomes
                .entry(outcome.to_string())
                .or_insert(0) += 1;
        }
    }

    pub(super) fn record_import(&self, outcome: &'static str) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .import_outcomes
                .entry(outcome.to_string())
                .or_insert(0) += 1;
        }
    }

    pub(super) fn record_fallback(&self, reason: ResolverPackFallbackReason) {
        if let Ok(mut snapshot) = self.telemetry.lock() {
            *snapshot
                .fallback_reasons
                .entry(reason.as_str().to_string())
                .or_insert(0) += 1;
        }
    }
}

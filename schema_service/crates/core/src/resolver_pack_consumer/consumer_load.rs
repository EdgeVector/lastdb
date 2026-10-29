//! Public load entry points of the resolver pack consumer.

use super::*;

impl<S> ResolverPackConsumer<S>
where
    S: ResolverPackObjectStore,
{
    pub fn new(
        store: S,
        cache: Arc<dyn ResolverPackCache>,
        config: ResolverPackConsumerConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            cache,
            config,
            clock,
            telemetry: Arc::new(Mutex::new(ResolverPackTelemetrySnapshot::default())),
        }
    }

    /// Load the latest resolver pack.
    ///
    /// `if_none_match` supports conditional GET via `If-None-Match`.
    ///
    /// On success with a newly fetched pack, the optional etag is returned for
    /// the next refresh. On last-known-good fallback the etag is cleared
    /// (`None`) so a later refresh revalidates the latest pointer.
    pub async fn load_latest_conditional(
        &self,
        if_none_match: Option<&str>,
    ) -> Result<
        (
            ResolverPackLoadOutcome,
            Option<LoadedResolverPack>,
            Option<String>,
        ),
        ResolverPackConsumerError,
    > {
        if !self.config.enabled {
            self.record_fallback(ResolverPackFallbackReason::Disabled);
            return Ok((
                ResolverPackLoadOutcome::LiveServiceFallback {
                    reason: ResolverPackFallbackReason::Disabled,
                },
                None,
                None,
            ));
        }

        // Probe the default algorithm path for the supported contract. The
        // manifest itself still carries the authoritative algorithm claim.
        let pointer_key = latest_compatible_manifest_pointer_key(
            self.config.env,
            SUPPORTED_RESOLVER_CONTRACT_VERSION,
            NATIVE_COMPONENT_COVER_ALGORITHM_ID,
            NATIVE_COMPONENT_COVER_ALGORITHM_VERSION,
            &self.config.expected_embedder_id,
        );

        let fetch = match self
            .store
            .get_object_conditional(&pointer_key, if_none_match)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                self.record_fetch(fetch_error_telemetry(&err));
                return self
                    .fallback_or_last_known_good(fallback_reason_for_fetch_error(&err))
                    .map(|(o, l)| (o, l, None));
            }
        };

        let (manifest_bytes, etag) = match fetch {
            ObjectFetchResult::NotFound => {
                self.record_fetch("missing_latest_manifest");
                return self
                    .fallback_or_last_known_good(ResolverPackFallbackReason::MissingPack)
                    .map(|(o, l)| (o, l, None));
            }
            ObjectFetchResult::NotModified { etag } => {
                self.record_fetch("latest_manifest_not_modified");
                return Ok(self.load_not_modified(etag));
            }
            ObjectFetchResult::Found { bytes, etag } => {
                self.record_fetch("latest_manifest_fetched");
                (bytes, etag)
            }
        };

        let staged = match self.load_from_manifest_bytes(&manifest_bytes).await {
            Ok(staged) => staged,
            Err(err) => {
                let reason = fallback_reason_for_error(&err);
                self.record_verification(reason.as_str());
                // Never overwrite LKG with a failed latest release.
                return self
                    .fallback_or_last_known_good(reason)
                    .map(|(o, l)| (o, l, None));
            }
        };

        self.cache.put_last_known_good(
            &manifest_bytes,
            &staged.resolver_config_bytes,
            &staged.schema_snapshot_bytes,
            &staged.embedding_artifact_bytes,
        )?;
        self.record_verification("ok");
        self.record_import("ok");
        Ok((
            ResolverPackLoadOutcome::Loaded {
                source: ResolverPackLoadSource::Latest,
            },
            Some(staged.loaded),
            etag,
        ))
    }
}

impl<S> ResolverPackConsumer<S>
where
    S: ResolverPackObjectStore,
{
    /// Prefer still-valid LKG (or previously staged cache) without network
    /// artifact fetches. If LKG is missing, fall back live.
    fn load_not_modified(
        &self,
        etag: Option<String>,
    ) -> (
        ResolverPackLoadOutcome,
        Option<LoadedResolverPack>,
        Option<String>,
    ) {
        match self.try_load_last_known_good() {
            Ok(Some(loaded)) => {
                self.record_verification("ok_not_modified");
                self.record_import("ok");
                (
                    ResolverPackLoadOutcome::Loaded {
                        source: ResolverPackLoadSource::NotModified,
                    },
                    Some(loaded),
                    etag,
                )
            }
            Ok(None) => {
                self.record_fallback(ResolverPackFallbackReason::MissingPack);
                (
                    ResolverPackLoadOutcome::LiveServiceFallback {
                        reason: ResolverPackFallbackReason::MissingPack,
                    },
                    None,
                    etag,
                )
            }
            Err(err) => {
                let reason = fallback_reason_for_error(&err);
                self.record_verification(reason.as_str());
                self.record_fallback(reason);
                (
                    ResolverPackLoadOutcome::LiveServiceFallback { reason },
                    None,
                    None,
                )
            }
        }
    }
}

//! State entry points for the release registry.

use super::*;

impl SchemaServiceState {
    /// Point get of one release by its exact key.
    #[must_use]
    pub fn get_release(&self, release_id: &str) -> Option<ReleaseRecord> {
        read_lock(&self.app_releases, "app_releases")
            .ok()
            .and_then(|releases| releases.get(release_id).cloned())
    }

    /// Point get of one channel by its exact key.
    #[must_use]
    pub fn get_channel(&self, app_id: &str, channel: &str) -> Option<ChannelRecord> {
        read_lock(&self.app_channels, "app_channels")
            .ok()
            .and_then(|channels| channels.get(&channel_key(app_id, channel)).cloned())
    }

    /// `POST /v2/apps/{app_id}/releases` — publish an immutable release.
    ///
    /// `body` is the parsed request JSON `{ "manifest": … }`, and it is the
    /// exact object the `app_release_publish` envelope signed. The release id
    /// is the digest of `body["manifest"]`, so what the developer signed and
    /// what the registry keys on are the same bytes.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn publish_release(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleasePublishOutcome, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .publish_release_inner(&config, app_id, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "release_publish",
            match &result {
                Ok(ReleasePublishOutcome::Created(_)) => "created",
                Ok(ReleasePublishOutcome::Idempotent(_)) => "idempotent",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn publish_release_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleasePublishOutcome, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        let manifest_value = body
            .get("manifest")
            .cloned()
            .ok_or_else(|| ReleaseError::InvalidManifest("body has no 'manifest'".to_string()))?;
        let manifest: ReleaseManifest = serde_json::from_value(manifest_value.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed manifest: {e}")))?;
        validate_manifest(&manifest).map_err(ReleaseError::InvalidManifest)?;
        if manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppReleasePublish,
        )
        .map_err(|f| map_auth_failure(&f))?;

        // Ownership: only the registered owner of the namespace publishes
        // releases into it. Read from durable storage, not memory, so a
        // concurrent app registration cannot be bypassed.
        let app = self
            .storage
            .backend()
            .load_app(app_id)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        // Every locked identity must already resolve. This is the gate that
        // keeps the release path read-only against the catalog: an
        // unresolved schema fails the publish, it does not get registered.
        for (name, identity) in &manifest.schemas {
            let resolved = self
                .get_schema_by_name(identity)
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if resolved.is_none() {
                return Err(ReleaseError::UnresolvedSchema(name.clone()));
            }
        }

        // The id is the digest of the manifest the developer signed, not of
        // a re-serialization of our parsed struct.
        let release_id =
            compute_release_id_from_value(&manifest_value).map_err(ReleaseError::Internal)?;

        {
            let releases = read_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if let Some(existing) = releases.get(&release_id) {
                return Ok(ReleasePublishOutcome::Idempotent(existing.clone()));
            }
        }

        let record = ReleaseRecord {
            release_id: release_id.clone(),
            manifest,
            publisher_dev_pubkey: verified.dev_pubkey,
            published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            revoked_at: None,
            revocation_reason: None,
        };

        // Insert-if-absent under one write lock: a concurrent publish of the
        // same manifest converges on the first winner rather than racing.
        let stored = {
            let mut releases = write_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            if let Some(existing) = releases.get(&release_id) {
                return Ok(ReleasePublishOutcome::Idempotent(existing.clone()));
            }
            releases.insert(release_id, record.clone());
            record
        };
        self.persist_release(&stored).await?;
        Ok(ReleasePublishOutcome::Created(stored))
    }

    /// `PUT /v2/apps/{app_id}/channels/{channel}` — point a channel at a
    /// release under a generation check.
    ///
    /// `body` is `{ app_id, channel, release_id, generation }`, signed with
    /// purpose `app_channel_set`. `generation` is what the writer read; a
    /// stale value fails with [`ReleaseError::GenerationConflict`] instead of
    /// clobbering a concurrent write.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn set_channel(
        &self,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ChannelRecord, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .set_channel_inner(&config, app_id, channel, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "channel_set",
            match &result {
                Ok(_) => "updated",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn set_channel_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ChannelRecord, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        if !is_valid_app_id(app_id) {
            return Err(ReleaseError::InvalidManifest(
                "app_id does not match ^[a-z][a-z0-9-]{0,39}$".to_string(),
            ));
        }
        if !is_valid_channel(channel) {
            return Err(ReleaseError::InvalidManifest(
                "channel does not match ^[a-z][a-z0-9-]{0,31}$".to_string(),
            ));
        }

        #[derive(Deserialize)]
        struct ChannelSetRequest {
            app_id: String,
            channel: String,
            release_id: String,
            generation: u64,
        }
        let request: ChannelSetRequest = serde_json::from_value(body.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed body: {e}")))?;
        if request.app_id != app_id || request.channel != channel {
            return Err(ReleaseError::AppIdMismatch);
        }
        if !is_sha256_hex(&request.release_id) {
            return Err(ReleaseError::InvalidManifest(
                "release_id must be 64 lowercase hex characters".to_string(),
            ));
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppChannelSet,
        )
        .map_err(|f| map_auth_failure(&f))?;
        let app = self
            .storage
            .backend()
            .load_app(app_id)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        // A channel can only point at a release that exists, in this app.
        let release = self
            .get_release(&request.release_id)
            .ok_or_else(|| ReleaseError::UnknownRelease(request.release_id.clone()))?;
        if release.manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        let key = channel_key(app_id, channel);
        let record = {
            let mut channels = write_lock(&self.app_channels, "app_channels")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            let current = channels.get(&key).map_or(0, |c| c.generation);
            if current != request.generation {
                return Err(ReleaseError::GenerationConflict {
                    expected: current,
                    supplied: request.generation,
                });
            }
            let record = ChannelRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                release_id: request.release_id,
                generation: current + 1,
                updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            };
            channels.insert(key, record.clone());
            record
        };
        self.persist_channel(&record).await?;
        Ok(record)
    }

    /// `POST /v2/apps/{app_id}/revocations` — revoke a published release.
    ///
    /// The manifest stays immutable; revocation annotates the record, and
    /// an installed host reacts on its next drift check.
    ///
    /// # Errors
    /// See [`ReleaseError`].
    pub async fn revoke_release(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleaseRecord, ReleaseError> {
        let config = self.app_identity_config();
        let result = self
            .revoke_release_inner(&config, app_id, cert_header, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "release_revoke",
            match &result {
                Ok(_) => "revoked",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn revoke_release_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<ReleaseRecord, ReleaseError> {
        if !config.is_active() {
            return Err(ReleaseError::NotConfigured);
        }
        #[derive(Deserialize)]
        struct RevokeRequest {
            app_id: String,
            release_id: String,
            #[serde(default)]
            reason: Option<String>,
        }
        let request: RevokeRequest = serde_json::from_value(body.clone())
            .map_err(|e| ReleaseError::InvalidManifest(format!("malformed body: {e}")))?;
        if request.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }
        if !is_sha256_hex(&request.release_id) {
            return Err(ReleaseError::InvalidManifest(
                "release_id must be 64 lowercase hex characters".to_string(),
            ));
        }
        if let Some(reason) = &request.reason {
            if reason.len() > REVOCATION_REASON_MAX_LEN {
                return Err(ReleaseError::InvalidManifest(format!(
                    "reason must be at most {REVOCATION_REASON_MAX_LEN} characters"
                )));
            }
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppReleaseRevoke,
        )
        .map_err(|f| map_auth_failure(&f))?;
        let app = self
            .storage
            .backend()
            .load_app(app_id)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .ok_or_else(|| ReleaseError::UnknownApp(app_id.to_string()))?;
        if app.owner_dev_pubkey != verified.dev_pubkey {
            return Err(ReleaseError::NotAppOwner);
        }

        let record = {
            let mut releases = write_lock(&self.app_releases, "app_releases")
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            let existing = releases
                .get_mut(&request.release_id)
                .ok_or_else(|| ReleaseError::UnknownRelease(request.release_id.clone()))?;
            if existing.manifest.app_id != app_id {
                return Err(ReleaseError::AppIdMismatch);
            }
            if existing.revoked_at.is_none() {
                existing.revoked_at =
                    Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
                existing.revocation_reason = request.reason;
            }
            existing.clone()
        };
        self.persist_release(&record).await?;
        Ok(record)
    }
    /// `GET /v2/releases/{release_id}/compat` — list all compat rows for a release.
    ///
    /// Reads from storage (not an in-memory cache) because compat rows are
    /// written infrequently by a separate verifier identity, not by the app
    /// publisher.
    ///
    /// # Errors
    /// Returns storage errors.
    pub async fn list_compat_rows(
        &self,
        release_id: &str,
    ) -> Result<Vec<CompatRow>, CompatRowError> {
        self.storage
            .backend()
            .load_compat_rows_for_release(release_id)
            .await
            .map_err(|e| CompatRowError::Internal(e.to_string()))
    }

    /// `POST /v2/releases/{release_id}/compat` — write a verifier-signed compat row.
    ///
    /// The envelope must be signed by the configured `verifier_pubkey` in
    /// `AppIdentityConfig`. App owner DevCerts are rejected.
    ///
    /// # Errors
    /// See [`CompatRowError`].
    pub async fn write_compat_row(
        &self,
        release_id: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<CompatRow, CompatRowError> {
        let config = self.app_identity_config();
        let result = self
            .write_compat_row_inner(&config, release_id, sig_header, body)
            .await;
        record_release_op(
            env_label(config.deployment_env),
            "compat_row_write",
            match &result {
                Ok(_) => "created",
                Err(e) => e.metric_status(),
            },
        );
        result
    }

    async fn write_compat_row_inner(
        &self,
        config: &AppIdentityConfig,
        release_id: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<CompatRow, CompatRowError> {
        let Some(verifier_pubkey) = &config.verifier_pubkey else {
            return Err(CompatRowError::VerifierNotConfigured);
        };

        #[derive(Deserialize)]
        struct CompatRowRequest {
            release_id: String,
            contract_version: String,
            #[serde(default)]
            verification_url: Option<String>,
        }
        let request: CompatRowRequest = serde_json::from_value(body.clone())
            .map_err(|e| CompatRowError::InvalidBody(format!("malformed body: {e}")))?;

        if request.release_id != release_id {
            return Err(CompatRowError::InvalidBody(
                "release_id in body does not match path".to_string(),
            ));
        }

        if request.contract_version.is_empty()
            || request.contract_version.len() > 64
            || !request
                .contract_version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == 'x')
        {
            return Err(CompatRowError::InvalidBody(
                "contract_version must be 1-64 chars matching [a-zA-Z0-9.-x]+".to_string(),
            ));
        }

        if self.get_release(release_id).is_none() {
            return Err(CompatRowError::UnknownRelease(release_id.to_string()));
        }

        use app_identity_crypto::{compute_payload_hash, verifying_key_from_base64};
        use base64::engine::general_purpose::STANDARD as BASE64;
        use base64::Engine as _;

        let envelope: app_identity_crypto::SignatureEnvelope =
            serde_json::from_str(sig_header).map_err(|_| CompatRowError::SignatureInvalid)?;

        let verifying_key = verifying_key_from_base64(verifier_pubkey)
            .map_err(|_| CompatRowError::VerifierNotConfigured)?;

        let envelope_payload_hash = envelope.payload_hash.as_str();
        let computed_hash =
            compute_payload_hash(body).map_err(|_| CompatRowError::SignatureInvalid)?;

        if envelope_payload_hash != computed_hash {
            return Err(CompatRowError::SignatureInvalid);
        }

        let sig_b64 = envelope
            .sig
            .as_ref()
            .ok_or(CompatRowError::SignatureInvalid)?;
        let sig_bytes = BASE64
            .decode(sig_b64.as_bytes())
            .map_err(|_| CompatRowError::SignatureInvalid)?;
        let signature = ::ed25519_dalek::Signature::from_slice(&sig_bytes)
            .map_err(|_| CompatRowError::SignatureInvalid)?;

        use ::ed25519_dalek::Verifier;
        let body_canonical = app_identity_crypto::canonicalize(body)
            .map_err(|_| CompatRowError::SignatureInvalid)?;
        verifying_key
            .verify(&body_canonical, &signature)
            .map_err(|_| CompatRowError::SignatureInvalid)?;

        let record = CompatRow {
            release_id: release_id.to_string(),
            contract_version: request.contract_version,
            verifier_pubkey: verifier_pubkey.clone(),
            verified_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            verification_url: request.verification_url,
        };

        self.storage
            .backend()
            .save_compat_row(&record)
            .await
            .map_err(|e| {
                if e.to_string().contains("409") || e.to_string().contains("already exists") {
                    CompatRowError::AlreadyExists
                } else {
                    CompatRowError::Internal(e.to_string())
                }
            })?;

        Ok(record)
    }

    /// `GET /v2/apps` (anonymous) — list all apps from storage.
    ///
    /// Reads from storage to include legacy apps and new per-record apps.
    /// Ordered by app_id for stable pagination.
    ///
    /// # Errors
    /// Returns storage errors.
    pub async fn list_all_apps(&self) -> Result<Vec<crate::snapshot::AppRecord>, String> {
        let apps = self
            .storage
            .backend()
            .load_all_apps()
            .await
            .map_err(|e| e.to_string())?;
        let mut sorted: Vec<_> = apps.into_values().collect();
        sorted.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        Ok(sorted)
    }

    /// `GET /v2/apps/{app_id}/releases` — list all releases for an app.
    ///
    /// Reads from the in-memory release store and filters by app_id.
    /// Ordered by published_at descending (newest first).
    #[must_use]
    pub fn list_releases_for_app(&self, app_id: &str) -> Vec<ReleaseRecord> {
        let Ok(releases) = read_lock(&self.app_releases, "app_releases") else {
            return Vec::new();
        };
        let mut filtered: Vec<_> = releases
            .values()
            .filter(|r| r.manifest.app_id == app_id)
            .cloned()
            .collect();
        filtered.sort_by(|a, b| b.published_at.cmp(&a.published_at));
        filtered
    }
}

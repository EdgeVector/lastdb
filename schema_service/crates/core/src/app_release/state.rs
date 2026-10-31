//! State entry points for the release registry.

use super::*;

/// The channel write both HTTP routers answer.
#[derive(Debug)]
pub enum SetChannelReply {
    /// The channel points at the release. HTTP 200. The value is the public channel.
    Ready(Value),
    /// The write failed. `status` and `body` come from the release error.
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Verify the cert and require the signer to own `app_id`.
    ///
    /// The app comes from durable storage, not memory, so a concurrent
    /// registration cannot be bypassed. Returns the owner pubkey.
    async fn require_app_owner(
        &self,
        app_id: &str,
        config: &AppIdentityConfig,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
        purpose: Purpose,
    ) -> Result<String, ReleaseError> {
        let verified = verify_cert_and_signature(config, cert_header, sig_header, body, purpose)
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
        Ok(verified.dev_pubkey)
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
        validate_manifest_round_trip(&manifest, &manifest_value)
            .map_err(ReleaseError::InvalidManifest)?;
        if manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        let publisher_dev_pubkey = self
            .require_app_owner(
                app_id,
                config,
                cert_header,
                sig_header,
                body,
                Purpose::AppReleasePublish,
            )
            .await?;

        // Every locked identity must already resolve. This is the gate that
        // keeps the release path read-only against the catalog: an
        // unresolved schema fails the publish, it does not get registered.
        for (name, identity) in &manifest.schemas {
            let resolved = self
                .get_schema_by_name(identity)
                .map_err(|e| ReleaseError::Internal(e.to_string()))?;
            let schema = resolved.ok_or_else(|| ReleaseError::UnresolvedSchema(name.clone()))?;
            let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")
                .map_err(|error| ReleaseError::Internal(error.to_string()))?;
            crate::field_name_policy::validate_schema_vocabulary(&schema, &vocabulary)
                .map_err(|error| ReleaseError::InvalidManifest(error.to_string()))?;
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
            publisher_dev_pubkey,
            published_at: self.clock.rfc3339_secs(),
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
            // Counted under the same lock as the insert, after the
            // idempotent return, so a repeat publish never counts.
            check_publish_rate(&releases, &record.publisher_dev_pubkey, self.clock.as_ref())?;
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

    /// Classify [`Self::set_channel`] for the HTTP routers.
    ///
    /// `Ready` is the public channel. `Failed` keeps the status and body from
    /// [`ReleaseError::to_http`]. Each router still checks the path, the body,
    /// and the signature headers. The metric stays inside `set_channel`.
    #[must_use]
    pub async fn set_channel_reply(
        &self,
        app_id: &str,
        channel: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> SetChannelReply {
        match self
            .set_channel(app_id, channel, cert_header, sig_header, body)
            .await
        {
            Ok(record) => SetChannelReply::Ready(record.to_public_json()),
            Err(error) => {
                let (status, error_body) = error.to_http();
                SetChannelReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
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

        self.require_app_owner(
            app_id,
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppChannelSet,
        )
        .await?;

        // A channel can only point at a release that exists, in this app.
        let release = self
            .get_release(&request.release_id)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .ok_or_else(|| ReleaseError::UnknownRelease(request.release_id.clone()))?;
        if release.manifest.app_id != app_id {
            return Err(ReleaseError::AppIdMismatch);
        }

        // The generation comes from durable storage, and the backend's
        // conditional write rejects a concurrent move. Memory changes only
        // after the write commits.
        let current = self
            .get_channel(app_id, channel)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .map_or(0, |c| c.generation);
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
            updated_at: self.clock.rfc3339_secs(),
        };
        self.persist_channel(&record).await?;
        write_lock(&self.app_channels, "app_channels")
            .map_err(|e| ReleaseError::Internal(e.to_string()))?
            .insert(channel_key(app_id, channel), record.clone());
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

        self.require_app_owner(
            app_id,
            config,
            cert_header,
            sig_header,
            body,
            Purpose::AppReleaseRevoke,
        )
        .await?;

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
                existing.revoked_at = Some(self.clock.rfc3339_secs());
                existing.revocation_reason = request.reason;
            }
            existing.clone()
        };
        self.persist_release(&record).await?;
        Ok(record)
    }
}

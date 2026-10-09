//! App update (`PUT /v1/apps/{app_id}`).

use super::*;

impl SchemaServiceState {
    /// Apply an owner-authenticated metadata update to a registered app
    /// (app_identity v3.1, `PUT /v1/apps/{app_id}`). Verifies the cert +
    /// `app_update` signature, requires the signer pubkey match the
    /// registered owner, and rejects any change to `display_name`. The
    /// commit is local to this deployment env — dev and prod are
    /// independent registries (the cross-env mirror was decommissioned
    /// in #517), so an owner updating their dev record does NOT touch
    /// the prod record.
    ///
    /// `body` is the parsed `{ metadata }` request JSON, which the handler
    /// folds into `{ app_id, metadata }` (the same shape `POST /v1/apps`
    /// signs) before passing here so the signed payload bytes match across
    /// the register + update paths.
    pub async fn update_app(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<AppUpdateOutcome, AppUpdateError> {
        let started = Instant::now();
        let config = self.app_identity_config();
        let label = env_label(config.deployment_env);

        let result = self
            .update_app_inner(&config, app_id, cert_header, sig_header, body)
            .await;

        let status = match &result {
            Ok(AppUpdateOutcome::Updated(_)) => "updated",
            Ok(AppUpdateOutcome::NoChange(_)) => "no_change",
            Err(e) => e.metric_status(),
        };
        record_app_update(label, status, started.elapsed().as_secs_f64());
        result
    }

    async fn update_app_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<AppUpdateOutcome, AppUpdateError> {
        // lint:fn-size-ok verbatim move from app_identity.rs; splitting this function is separate work
        if !is_valid_app_id(app_id) {
            return Err(AppUpdateError::InvalidAppId);
        }

        // Parse + validate the metadata blob before any crypto work.
        let request: AppUpdateRequest = serde_json::from_value(body.clone())
            .map_err(|e| AppUpdateError::InvalidMetadata(format!("malformed body: {e}")))?;
        validate_metadata(&request.metadata).map_err(AppUpdateError::InvalidMetadata)?;
        if let Some(cs) = &request.code_signature {
            validate_code_signature(cs).map_err(AppUpdateError::InvalidCodeSignature)?;
        }
        if let Some(source) = &request.source {
            validate_app_source(source).map_err(AppUpdateError::InvalidMetadata)?;
        }
        if let Some(artifact) = &request.artifact {
            validate_app_artifact(artifact).map_err(AppUpdateError::InvalidMetadata)?;
        }
        validate_uses(&request.uses).map_err(AppUpdateError::InvalidUses)?;

        // The signed payload is `{ app_id, metadata }` — the same shape the
        // register path signs. Folding the path's app_id in here means the
        // signature binds to the target namespace; a signature for app A
        // cannot be replayed against app B. `code_signature` and `uses` each
        // join the signed payload ONLY when present/non-empty so an old
        // client's signature (which never saw the field) still verifies
        // byte-for-byte.
        let mut signed_payload = json!({ "app_id": app_id, "metadata": request.metadata });
        if let Some(cs) = &request.code_signature {
            signed_payload["code_signature"] =
                serde_json::to_value(cs).map_err(|e| AppUpdateError::Internal(e.to_string()))?;
        }
        if let Some(source) = &request.source {
            signed_payload["source"] = serde_json::to_value(source)
                .map_err(|e| AppUpdateError::Internal(e.to_string()))?;
        }
        if let Some(artifact) = &request.artifact {
            signed_payload["artifact"] = serde_json::to_value(artifact)
                .map_err(|e| AppUpdateError::Internal(e.to_string()))?;
        }
        if !request.uses.is_empty() {
            signed_payload["uses"] = serde_json::to_value(&request.uses)
                .map_err(|e| AppUpdateError::Internal(e.to_string()))?;
        }

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            &signed_payload,
            Purpose::AppUpdate,
        )
        .map_err(|f| match f {
            AuthFailure::CertExpired => AppUpdateError::CertExpired,
            AuthFailure::EnvelopeInvalid => AppUpdateError::EnvelopeInvalid,
            AuthFailure::CertInvalid | AuthFailure::DevRevoked => AppUpdateError::CertInvalid,
        })?;

        let (previous_record, updated_record) = {
            let mut apps = self
                .apps
                .write()
                .map_err(|_| AppUpdateError::Internal("apps write lock poisoned".to_string()))?;
            let existing = apps
                .get(app_id)
                .ok_or(AppUpdateError::AppNotRegistered)?
                .clone();

            // Owner check: the signer's pubkey MUST equal the registered
            // owner. A valid DevCert alone is not sufficient — a different
            // developer holding their own cert cannot mutate this app's
            // metadata.
            if existing.owner_dev_pubkey != verified.dev_pubkey {
                return Err(AppUpdateError::NotOwner);
            }

            // display_name is set at registration and immutable. Other
            // fields (description / homepage_url / icon_url) can change.
            if existing.metadata.display_name != request.metadata.display_name {
                return Err(AppUpdateError::DisplayNameImmutable {
                    current_display_name: existing.metadata.display_name,
                });
            }

            // `code_signature`: absent = leave as-is; present = set/replace
            // (the owner's add/rotate path). See [`AppUpdateRequest`].
            let next_code_signature = request
                .code_signature
                .clone()
                .or_else(|| existing.code_signature.clone());
            let next_source = request.source.clone().or_else(|| existing.source.clone());
            let next_artifact = request
                .artifact
                .clone()
                .or_else(|| existing.artifact.clone());

            // `uses`: same "absent = leave as-is, present = replace" rule.
            // An update body that omits the field (empty after `serde(default)`)
            // keeps the registered declaration; a non-empty list replaces it
            // wholesale (a re-publish re-asserts the whole intent set). This
            // mirrors how a manifest `[uses]` change flows through `push`.
            let next_uses = if request.uses.is_empty() {
                existing.uses.clone()
            } else {
                request.uses.clone()
            };

            if existing.metadata == request.metadata
                && existing.code_signature == next_code_signature
                && existing.source == next_source
                && existing.artifact == next_artifact
                && existing.uses == next_uses
            {
                return Ok(AppUpdateOutcome::NoChange(existing));
            }

            let updated = AppRecord {
                app_id: existing.app_id.clone(),
                owner_dev_pubkey: existing.owner_dev_pubkey.clone(),
                metadata: request.metadata,
                version: existing.version.clone(),
                registered_at: existing.registered_at.clone(),
                // Metadata update preserves the lifecycle tier; only
                // `promote_app` changes it.
                tier: existing.tier,
                code_signature: next_code_signature,
                source: next_source,
                artifact: next_artifact,
                uses: next_uses,
            };
            apps.insert(updated.app_id.clone(), updated.clone());
            (existing, updated)
        };

        if let Err(e) = self.persist_app_update(&updated_record).await {
            // Roll the in-memory swap back to the previous record so a
            // persistence failure can't leave the registry one version
            // ahead of the backend.
            if let Ok(mut apps) = self.apps.write() {
                apps.insert(previous_record.app_id.clone(), previous_record);
            }
            return Err(AppUpdateError::Internal(e.to_string()));
        }
        // Clients re-fetch the snapshot when the state version moves; the
        // metadata they consent against must reflect the new copy.
        self.bump_state_version();
        Ok(AppUpdateOutcome::Updated(updated_record))
    }
}

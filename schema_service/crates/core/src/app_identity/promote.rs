//! App promotion between tiers.

use super::*;

impl SchemaServiceState {
    /// Promote a sandbox app to live (`POST /v1/apps/{app_id}/promote`).
    /// Verifies the cert + `app_promote` signature, requires the signer
    /// pubkey match the registered owner, and requires the cert to carry
    /// `authorized_publisher = true`. Idempotent: promoting an already-live
    /// app is a no-op success.
    ///
    /// The signed payload is `{ app_id, action: "promote" }` — the path's
    /// `app_id` is folded into the signed bytes so a promote signature for
    /// app A cannot be replayed against app B, and the dedicated
    /// `Purpose::AppPromote` keeps it from being replayed as a metadata
    /// update.
    pub async fn promote_app(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
    ) -> Result<AppPromoteOutcome, AppPromoteError> {
        let started = Instant::now();
        let config = self.app_identity_config();
        let label = env_label(config.deployment_env);

        let result = self
            .promote_app_inner(&config, app_id, cert_header, sig_header)
            .await;

        let status = match &result {
            Ok(AppPromoteOutcome::Promoted(_)) => "promoted",
            Ok(AppPromoteOutcome::AlreadyLive(_)) => "already_live",
            Err(e) => e.metric_status(),
        };
        record_app_promote(label, status, started.elapsed().as_secs_f64());
        result
    }

    async fn promote_app_inner(
        &self,
        config: &AppIdentityConfig,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
    ) -> Result<AppPromoteOutcome, AppPromoteError> {
        if !is_valid_app_id(app_id) {
            return Err(AppPromoteError::InvalidAppId);
        }

        let signed_payload = json!({ "app_id": app_id, "action": "promote" });

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            &signed_payload,
            Purpose::AppPromote,
        )
        .map_err(|f| match f {
            AuthFailure::CertExpired => AppPromoteError::CertExpired,
            AuthFailure::EnvelopeInvalid => AppPromoteError::EnvelopeInvalid,
            AuthFailure::CertInvalid | AuthFailure::DevRevoked => AppPromoteError::CertInvalid,
        })?;

        // Promotion to live requires an authorized publisher (paid OR
        // developer_access), stamped on the cert at mint. A sandbox app can
        // be reserved + iterated by any authorized dev, but going live keeps
        // the same gate as the rest of the publish surface.
        if !verified.authorized_publisher {
            return Err(AppPromoteError::NotAuthorizedPublisher);
        }

        let (previous_record, promoted_record) = {
            let mut apps = self
                .apps
                .write()
                .map_err(|_| AppPromoteError::Internal("apps write lock poisoned".to_string()))?;
            let existing = apps
                .get(app_id)
                .ok_or(AppPromoteError::AppNotRegistered)?
                .clone();

            // Owner check: the signer's pubkey MUST equal the registered
            // owner. A valid DevCert alone is not sufficient.
            if existing.owner_dev_pubkey != verified.dev_pubkey {
                return Err(AppPromoteError::NotOwner);
            }

            if existing.tier == AppTier::Live {
                return Ok(AppPromoteOutcome::AlreadyLive(existing));
            }

            // Live shelf = installable. Source-first install clones
            // `source`; artifact install is the Phase-2 tarball path.
            // Either pointer must be on the *registry row* (publish body
            // or owner PUT) before sandbox→live. Already-live rows are
            // grandfathered via the AlreadyLive branch above.
            let has_source = existing
                .source
                .as_ref()
                .is_some_and(|s| !s.trim().is_empty());
            let has_artifact = existing.artifact.is_some();
            if !has_source && !has_artifact {
                return Err(AppPromoteError::MissingInstallPointer);
            }

            let promoted = AppRecord {
                tier: AppTier::Live,
                ..existing.clone()
            };
            apps.insert(promoted.app_id.clone(), promoted.clone());
            (existing, promoted)
        };

        if let Err(e) = self.persist_app_update(&promoted_record).await {
            // Roll the in-memory flip back so a persistence failure can't
            // leave the registry one tier ahead of the backend.
            if let Ok(mut apps) = self.apps.write() {
                apps.insert(previous_record.app_id.clone(), previous_record);
            }
            return Err(AppPromoteError::Internal(e.to_string()));
        }
        // Clients re-fetch the snapshot when the state version moves.
        self.bump_state_version();
        Ok(AppPromoteOutcome::Promoted(promoted_record))
    }
}

//! App registration (`POST /v1/apps`): cert + signature checks, quota, first-write-wins commit.

use super::*;

impl SchemaServiceState {
    /// Register an app (`POST /v1/apps`). Verifies the cert + signature,
    /// validates the app_id and metadata, and applies first-write-wins.
    ///
    /// `body` is the parsed request JSON `{ app_id, metadata }` — the same
    /// object the `app_register` envelope signed.
    pub async fn register_app(
        &self,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<AppRegisterOutcome, AppRegisterError> {
        let started = Instant::now();
        let config = self.app_identity_config();
        let label = env_label(config.deployment_env);

        let result = self
            .register_app_inner(&config, cert_header, sig_header, body)
            .await;

        let status = match &result {
            Ok(AppRegisterOutcome::Created(_)) => "created",
            Ok(AppRegisterOutcome::Updated(_)) => "updated",
            Ok(AppRegisterOutcome::Idempotent(_)) => "idempotent",
            Err(e) => e.metric_status(),
        };
        record_app_register(label, status, started.elapsed().as_secs_f64());
        if matches!(result, Ok(AppRegisterOutcome::Created(_))) {
            record_apps_registry_size(self.apps_count());
        }
        result
    }

    async fn register_app_inner(
        &self,
        config: &AppIdentityConfig,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> Result<AppRegisterOutcome, AppRegisterError> {
        // 1. Parse + validate the request body (cheap; before crypto).
        let request: AppRegisterRequest = serde_json::from_value(body.clone())
            .map_err(|e| AppRegisterError::InvalidMetadata(format!("malformed body: {e}")))?;
        // Charset guard first so a non-ASCII / control-char id gets the
        // discriminated `invalid_app_id_charset` reason rather than the
        // generic grammar rejection (see [`validate_app_id_charset`]).
        validate_app_id_charset(&request.app_id).map_err(AppRegisterError::InvalidAppIdCharset)?;
        if !is_valid_app_id(&request.app_id) {
            return Err(AppRegisterError::InvalidAppId);
        }
        validate_metadata(&request.metadata).map_err(AppRegisterError::InvalidMetadata)?;
        validate_app_version(&request.version).map_err(AppRegisterError::InvalidVersion)?;
        if let Some(cs) = &request.code_signature {
            validate_code_signature(cs).map_err(AppRegisterError::InvalidCodeSignature)?;
        }
        if let Some(source) = &request.source {
            validate_app_source(source).map_err(AppRegisterError::InvalidMetadata)?;
        }
        if let Some(artifact) = &request.artifact {
            validate_app_artifact(artifact).map_err(AppRegisterError::InvalidMetadata)?;
        }
        validate_uses(&request.uses).map_err(AppRegisterError::InvalidUses)?;

        // 2. Verify cert + signature over the whole body.
        let verified =
            verify_cert_and_signature(config, cert_header, sig_header, body, Purpose::AppRegister)
                .map_err(|f| match f {
                    AuthFailure::CertExpired => AppRegisterError::CertExpired,
                    AuthFailure::EnvelopeInvalid => AppRegisterError::EnvelopeInvalid,
                    // A revoked dev's cert is simply not honored at registration;
                    // the design's /v1/apps response set has no 403, so collapse
                    // to cert_invalid here (schema_claim keeps the 403).
                    AuthFailure::CertInvalid | AuthFailure::DevRevoked => {
                        AppRegisterError::CertInvalid
                    }
                })?;

        // 2b. Authorization gate — reserving an app namespace requires an
        // authorized publisher (paid OR developer_access), stamped on the
        // cert at mint. An unauthorized free account cannot reserve a name.
        if !verified.authorized_publisher {
            return Err(AppRegisterError::NotAuthorizedPublisher);
        }

        // 3. First-write-wins commit. Every new registration starts in the
        // Sandbox tier — the developer owns the name and can publish schemas
        // under it immediately, but the app is not production until an
        // owner-authenticated `POST /v1/apps/{id}/promote` flips it to Live.
        let record = AppRecord {
            app_id: request.app_id.clone(),
            owner_dev_pubkey: verified.dev_pubkey,
            metadata: request.metadata,
            version: request.version,
            registered_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            tier: AppTier::Sandbox,
            code_signature: request.code_signature,
            source: request.source,
            artifact: request.artifact,
            uses: request.uses,
        };
        self.commit_app_record(record).await
    }

    /// First-write-wins commit of an [`AppRecord`] into the registry.
    /// The check + insert happen under one write lock so concurrent
    /// claims for the same app_id can't both win; persistence happens
    /// after the lock is dropped (can't await while holding a std
    /// RwLock guard), with rollback on failure.
    ///
    /// Called by `register_app` (the public publish path, after cert
    /// verification). Idempotent: a second commit with the same owner +
    /// metadata returns [`AppRegisterOutcome::Idempotent`]; a different
    /// owner returns [`AppRegisterError::Conflict`].
    pub(crate) async fn commit_app_record(
        &self,
        record: AppRecord,
    ) -> Result<AppRegisterOutcome, AppRegisterError> {
        // lint:fn-size-ok verbatim move from app_identity.rs; splitting this function is separate work
        enum CommitPlan {
            Created(Box<AppRecord>),
            Updated {
                updated: Box<AppRecord>,
                previous: Box<AppRecord>,
            },
        }

        let plan = 'commit: {
            let mut apps = self
                .apps
                .write()
                .map_err(|_| AppRegisterError::Internal("apps write lock poisoned".to_string()))?;
            if let Some(existing) = apps.get(&record.app_id) {
                if existing.owner_dev_pubkey == record.owner_dev_pubkey {
                    let version_is_greater =
                        app_version_is_greater(&record.version, &existing.version)
                            .map_err(AppRegisterError::InvalidVersion)?;
                    if !version_is_greater {
                        return Err(AppRegisterError::NonMonotonicVersion {
                            current_version: existing.version.clone(),
                            requested_version: record.version.clone(),
                        });
                    }
                    let updated = AppRecord {
                        app_id: existing.app_id.clone(),
                        owner_dev_pubkey: existing.owner_dev_pubkey.clone(),
                        metadata: record.metadata,
                        version: record.version,
                        registered_at: existing.registered_at.clone(),
                        // First publish starts sandbox. A later version publish
                        // updates the latest row and preserves visibility, so
                        // already-live apps stay discoverable for upgrade.
                        tier: existing.tier,
                        code_signature: record
                            .code_signature
                            .or_else(|| existing.code_signature.clone()),
                        source: record.source.or_else(|| existing.source.clone()),
                        artifact: record.artifact.or_else(|| existing.artifact.clone()),
                        uses: if record.uses.is_empty() {
                            existing.uses.clone()
                        } else {
                            record.uses
                        },
                    };
                    let previous = existing.clone();
                    apps.insert(updated.app_id.clone(), updated.clone());
                    break 'commit CommitPlan::Updated {
                        updated: Box::new(updated),
                        previous: Box::new(previous),
                    };
                }
                // Idempotent only when the re-post matches what's stored.
                // `code_signature` follows the update path's "absent = no
                // claim" semantics: a re-post WITHOUT the field matches any
                // stored signing identity (so a legacy client's re-push stays
                // a 200 no-op), while a re-post with a DIFFERENT one is a 409
                // — the owner's add/rotate path is `PUT /v1/apps/{app_id}`,
                // which clients fall back to off that 409.
                let code_signature_matches = record.code_signature.is_none()
                    || existing.code_signature == record.code_signature;
                let source_matches = record.source.is_none() || existing.source == record.source;
                let artifact_matches =
                    record.artifact.is_none() || existing.artifact == record.artifact;
                if existing.owner_dev_pubkey == record.owner_dev_pubkey
                    && existing.metadata == record.metadata
                    && code_signature_matches
                    && source_matches
                    && artifact_matches
                {
                    return Ok(AppRegisterOutcome::Idempotent(existing.clone()));
                }
                return Err(AppRegisterError::Conflict {
                    current_owner_dev_pubkey: existing.owner_dev_pubkey.clone(),
                });
            }
            // Per-developer quota (checked only on a genuinely NEW app_id:
            // the idempotent-re-post and conflict arms above return first,
            // and `PUT /v1/apps/{app_id}` never reaches this commit path —
            // so an at-cap developer can keep updating their existing apps).
            let owned_by_dev = apps
                .values()
                .filter(|r| r.owner_dev_pubkey == record.owner_dev_pubkey)
                .count();
            if owned_by_dev >= MAX_APPS_PER_DEVELOPER {
                return Err(AppRegisterError::QuotaExceeded);
            }
            apps.insert(record.app_id.clone(), record.clone());
            CommitPlan::Created(Box::new(record))
        };

        match plan {
            CommitPlan::Created(record) => {
                let record = *record;
                if let Err(e) = self.persist_app(&record).await {
                    // Roll the in-memory insert back so a persistence failure
                    // doesn't leave a phantom registration that vanishes on restart.
                    if let Ok(mut apps) = self.apps.write() {
                        apps.remove(&record.app_id);
                    }
                    return Err(AppRegisterError::Internal(e.to_string()));
                }
                self.bump_state_version();
                Ok(AppRegisterOutcome::Created(record))
            }
            CommitPlan::Updated { updated, previous } => {
                let updated = *updated;
                let previous = *previous;
                if let Err(e) = self.persist_app_update(&updated).await {
                    if let Ok(mut apps) = self.apps.write() {
                        apps.insert(previous.app_id.clone(), previous);
                    }
                    return Err(AppRegisterError::Internal(e.to_string()));
                }
                self.bump_state_version();
                Ok(AppRegisterOutcome::Updated(updated))
            }
        }
    }
}

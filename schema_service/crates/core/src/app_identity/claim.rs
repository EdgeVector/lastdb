//! Schema-claim and field-declare authorization against the registered app owner.

use super::*;

impl SchemaServiceState {
    /// Gate a `POST /v1/schemas` registration on the `owner_app_id` ↔ dev
    /// cert binding. Call this BEFORE `add_schema`.
    ///
    /// `schema_payload` is the raw `schema` JSON object — the payload the
    /// `schema_claim` envelope signed.
    ///
    /// Namespacing is the discriminator, not gatekeeping. Submissions
    /// WITHOUT `owner_app_id` are un-namespaced user proposals: the
    /// registry accepts them and canonicalizes (identity-hash dedup,
    /// descriptive-name correction, similarity dedup).
    ///
    /// Submissions WITH `owner_app_id` split on intent
    /// (`offer_to_shared_discovery`), per the local-first app-namespacing
    /// design (`designs-local-first-app-namespacing`):
    ///
    /// - **Local namespace claim** (`offer_to_shared_discovery = false`, the
    ///   default — what a fresh `fbrain init` / `fkanban init` and every
    ///   node-side registration send) is CERT-FREE. The node already computes
    ///   the schema's deterministic `identity_hash` locally
    ///   (`compute_identity_hash`, content-addressed: it embeds
    ///   `"app:{owner_app_id}:"` so namespaces are isolated in the hash with no
    ///   registry round-trip). Claiming a namespace therefore needs neither the
    ///   shared registry nor a DevCert — it only records ownership of a
    ///   content-addressed namespace, so we accept it unconditionally. This
    ///   removes the `cert_required` 401 from the build/run hot path that was
    ///   the #1 onboarding dead-end (it superseded the client-side
    ///   resolve-not-publish workaround).
    /// - **Offer into shared discovery** (`offer_to_shared_discovery = true`)
    ///   is the act of publishing a schema into OTHERS' discovery — writing to
    ///   the shared commons. That MUST present a valid dev-cert + signature
    ///   whose `dev_pubkey` matches the app's registered owner; apps cannot
    ///   forge ownership of a shared namespace they don't control. When
    ///   app-identity is not configured (no trusted roots) a shared-discovery
    ///   offer fails loud with `app_identity_not_configured` so the
    ///   misconfigured stage is visible at first publish.
    ///
    /// The DevCert gate did not move in spirit — it moved OFF the local claim
    /// path and stays ON the shared-discovery offer, which is the honest place
    /// for it.
    pub fn authorize_schema_claim(
        &self,
        owner_app_id: Option<&str>,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        schema_payload: &Value,
        offer_to_shared_discovery: bool,
    ) -> Result<(), SchemaClaimError> {
        let config = self.app_identity_config();
        let result = self.authorize_schema_claim_inner(
            &config,
            owner_app_id,
            cert_header,
            sig_header,
            schema_payload,
            offer_to_shared_discovery,
        );
        let status = match &result {
            Ok(()) => "ok",
            Err(e) => e.metric_status(),
        };
        record_schema_claim(status, owner_app_id);
        result
    }

    fn authorize_schema_claim_inner(
        &self,
        config: &AppIdentityConfig,
        owner_app_id: Option<&str>,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        schema_payload: &Value,
        offer_to_shared_discovery: bool,
    ) -> Result<(), SchemaClaimError> {
        let Some(app_id) = owner_app_id.filter(|s| !s.is_empty()) else {
            // Un-namespaced proposal: accept and let the registry
            // canonicalize. Users running fold_db nodes propose freely;
            // only namespace reservation requires cert verification.
            return Ok(());
        };

        // Local namespace claim (the default). The deterministic
        // `identity_hash` is computed locally and already isolates the
        // namespace via its `"app:{id}:"` prefix, so claiming needs no
        // registry ownership check and no DevCert. Accept it before any
        // cert / config gate fires — this is what keeps `cert_required` off
        // the build/run hot path. Only an explicit offer INTO shared
        // discovery falls through to the cert gate below.
        if !offer_to_shared_discovery {
            return Ok(());
        }

        // An `owner_app_id`-tagged publish against a stage with no
        // `APP_IDENTITY_ROOT_PUBKEYS` configured can't have its cert
        // verified — silently accepting it was the 2026-05-30 fbrain
        // dogfood false-green (publish returns 200 with a bare canonical).
        // Fail loud so a misconfigured deploy is visible at the first
        // publish, not in a snapshot diff days later.
        if !config.is_active() {
            return Err(SchemaClaimError::AppIdentityNotConfigured);
        }

        let (Some(cert_header), Some(sig_header)) = (cert_header, sig_header) else {
            return Err(SchemaClaimError::CertRequired);
        };

        let verified = verify_cert_and_signature(
            config,
            cert_header,
            sig_header,
            schema_payload,
            Purpose::SchemaClaim,
        )
        .map_err(|f| match f {
            AuthFailure::CertExpired => SchemaClaimError::CertExpired,
            AuthFailure::EnvelopeInvalid => SchemaClaimError::EnvelopeInvalid,
            AuthFailure::DevRevoked => SchemaClaimError::DevRevoked,
            AuthFailure::CertInvalid => SchemaClaimError::CertInvalid,
        })?;

        let apps =
            read_lock(&self.apps, "apps").map_err(|e| SchemaClaimError::Internal(e.to_string()))?;
        match apps.get(app_id) {
            None => Err(SchemaClaimError::AppNotRegistered),
            Some(rec) if rec.owner_dev_pubkey == verified.dev_pubkey => Ok(()),
            Some(_) => Err(SchemaClaimError::CertForWrongApp),
        }
    }

    /// Gate `POST /v1/fields/declare` on the `owner_app_id` ↔ dev-cert binding.
    ///
    /// **Always cert-gated**, and that is the difference from
    /// [`Self::authorize_schema_claim`]. A local schema claim is cert-free
    /// because the node computes a content-addressed `identity_hash` locally
    /// and is merely recording ownership of a namespace it already owns by
    /// construction. Declaring a field is not that: it claims a slot that other
    /// schemas' writes will fold into, on a service shared by everyone. There
    /// is no content-addressed fallback that makes it safe to accept
    /// unverified.
    ///
    /// The corollary, which is the whole rule: **no verified identity → no
    /// owned field.** A submission without a verified `owner_app_id` is an
    /// un-namespaced proposal, which is precisely *not yours*. Such
    /// submissions keep working exactly as they do today; they simply cannot
    /// declare fields, and coherence requires declaring.
    ///
    /// Reuses [`SchemaClaimError`] so the 401/403 body shapes stay in lockstep
    /// with the app-register / schema-claim handlers rather than inventing a
    /// second vocabulary for the same failures.
    pub fn authorize_field_declare(
        &self,
        owner_app_id: &str,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        payload: &Value,
    ) -> Result<(), SchemaClaimError> {
        let config = self.app_identity_config();

        // Fail loud on a misconfigured stage. A declare accepted with no
        // trusted roots configured cannot have had its cert verified, and
        // silently accepting it is the 2026-05-30 dogfood false-green: publish
        // returns 200, the problem surfaces days later in a diff.
        if !config.is_active() {
            return Err(SchemaClaimError::AppIdentityNotConfigured);
        }

        let app_id = owner_app_id.trim();
        if app_id.is_empty() {
            return Err(SchemaClaimError::CertRequired);
        }

        let (Some(cert_header), Some(sig_header)) = (cert_header, sig_header) else {
            return Err(SchemaClaimError::CertRequired);
        };

        // A distinct purpose, so a `schema_claim` signature can never be
        // replayed as a field declaration.
        let verified = verify_cert_and_signature(
            &config,
            cert_header,
            sig_header,
            payload,
            Purpose::FieldDeclare,
        )
        .map_err(|f| match f {
            AuthFailure::CertExpired => SchemaClaimError::CertExpired,
            AuthFailure::EnvelopeInvalid => SchemaClaimError::EnvelopeInvalid,
            AuthFailure::DevRevoked => SchemaClaimError::DevRevoked,
            AuthFailure::CertInvalid => SchemaClaimError::CertInvalid,
        })?;

        let apps =
            read_lock(&self.apps, "apps").map_err(|e| SchemaClaimError::Internal(e.to_string()))?;
        match apps.get(app_id) {
            None => Err(SchemaClaimError::AppNotRegistered),
            Some(rec) if rec.owner_dev_pubkey == verified.dev_pubkey => Ok(()),
            Some(_) => Err(SchemaClaimError::CertForWrongApp),
        }
    }
}

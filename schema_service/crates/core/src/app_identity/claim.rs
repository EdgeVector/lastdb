//! New schemas and declared fields require a verified Exemem developer owner.

use super::*;

impl SchemaServiceState {
    /// Authorize a new schema or expansion before `add_schema`.
    /// Local namespace claims and shared offers have the same account requirement.
    /// An exact public repost is a read and is handled before this gate.
    pub fn authorize_schema_claim(
        &self,
        owner_app_id: Option<&str>,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        schema_payload: &Value,
        _offer_to_shared_discovery: bool,
    ) -> Result<(), SchemaClaimError> {
        let result = self.authorize_owned_registration(
            owner_app_id.unwrap_or_default(),
            cert_header,
            sig_header,
            schema_payload,
            Purpose::SchemaClaim,
        );
        let status = match &result {
            Ok(()) => "ok",
            Err(error) => error.metric_status(),
        };
        record_schema_claim(status, owner_app_id);
        result
    }

    /// Authorize field declarations with their own signed purpose and body.
    pub fn authorize_field_declare(
        &self,
        owner_app_id: &str,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        payload: &Value,
    ) -> Result<(), SchemaClaimError> {
        self.authorize_owned_registration(
            owner_app_id,
            cert_header,
            sig_header,
            payload,
            Purpose::FieldDeclare,
        )
    }

    fn authorize_owned_registration(
        &self,
        owner_app_id: &str,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        payload: &Value,
        purpose: Purpose,
    ) -> Result<(), SchemaClaimError> {
        let app_id = owner_app_id.trim();
        if app_id.is_empty() {
            return Err(SchemaClaimError::CertRequired);
        }
        let config = self.app_identity_config();
        if !config.is_active() {
            return Err(SchemaClaimError::AppIdentityNotConfigured);
        }
        let (Some(cert_header), Some(sig_header)) = (cert_header, sig_header) else {
            return Err(SchemaClaimError::CertRequired);
        };
        let verified =
            verify_cert_and_signature(&config, cert_header, sig_header, payload, purpose).map_err(
                |failure| match failure {
                    AuthFailure::CertExpired => SchemaClaimError::CertExpired,
                    AuthFailure::EnvelopeInvalid => SchemaClaimError::EnvelopeInvalid,
                    AuthFailure::DevRevoked => SchemaClaimError::DevRevoked,
                    AuthFailure::CertInvalid => SchemaClaimError::CertInvalid,
                },
            )?;
        if !verified.authorized_publisher {
            return Err(SchemaClaimError::NotAuthorizedPublisher);
        }
        let apps = read_lock(&self.apps, "apps")
            .map_err(|error| SchemaClaimError::Internal(error.to_string()))?;
        match apps.get(app_id) {
            None => Err(SchemaClaimError::AppNotRegistered),
            Some(record) if record.owner_dev_pubkey == verified.dev_pubkey => Ok(()),
            Some(_) => Err(SchemaClaimError::CertForWrongApp),
        }
    }
}

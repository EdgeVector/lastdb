use super::*;
use crate::state::SchemaServiceState;

impl SchemaServiceState {
    /// Declare (or re-declare) a field handle and persist the result.
    ///
    /// The caller must already have passed
    /// [`SchemaServiceState::authorize_field_declare`] — this method trusts
    /// `request.owner_app_id` and does not re-verify the cert. Keeping the two
    /// separate is deliberate: the HTTP layer owns credential extraction, and
    /// this owns the registry invariants.
    pub async fn declare_field(
        &self,
        request: &DeclareFieldRequest,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        let (owner, handle) = declare_steps::validate_declare_request(request)?;
        let _write = self.declared_field_writes.lock().await;
        let backend = self.storage.backend();
        let names: Vec<_> = request
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let (persisted, ()) = tokio::try_join!(
            backend.load_all_declared_fields(),
            self.refresh_vocabulary_for_names(&names),
        )
        .map_err(|error| DeclaredFieldError::Internal(error.to_string()))?;
        let mut staged = DeclaredFieldRegistry::new();
        staged.load(persisted);
        let expected = staged
            .by_owner_handle
            .get(&(owner.to_owned(), handle.to_owned()))
            .and_then(|id| staged.by_id.get(id))
            .cloned();
        let fields = self
            .checked_declared_vocabulary(&request.fields)
            .map_err(|error| DeclaredFieldError::InvalidField(error.to_string()))?;
        let mut record = staged.declare(request, mint_declaration_id)?;
        if record.declared_at.is_empty() {
            record.declared_at = self.clock.rfc3339_secs();
        }
        self.record_schema_write();
        self.commit_vocabulary(
            &fields,
            crate::vocabulary_commit::VocabularyCommit::Declaration {
                record: &record,
                expected: expected.as_ref(),
            },
        )
        .await
        .map_err(|error| DeclaredFieldError::Internal(error.to_string()))?;
        self.declared_fields
            .write()
            .map_err(|error| DeclaredFieldError::Internal(error.to_string()))?
            .load([record.clone()]);
        self.bump_state_version();
        tracing::info!(
            target: "schema_service::declared_fields",
            declaration_id = %record.declaration_id,
            handle = %record.handle,
            owner_app_id = %record.owner_app_id,
            fields = record.fields.len(),
            "declared field handle",
        );
        Ok(record)
    }

    /// Look up one declaration.
    pub fn get_declared_field(
        &self,
        declaration_id: &str,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        let registry = self
            .declared_fields
            .read()
            .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
        registry
            .get(declaration_id)
            .cloned()
            .ok_or_else(|| DeclaredFieldError::UnknownDeclaration(declaration_id.to_string()))
    }

    /// Refuse a schema registration that stamps a declared identity the
    /// submitting app has no permission to reference.
    ///
    /// Called from `add_schema`. Identities belonging to no declaration pass
    /// straight through — every one of the 1,141 live schemas carries
    /// locally-minted v1 field hashes, and this feature is forward-only.
    pub fn authorize_declared_field_references(
        &self,
        owner_app_id: Option<&str>,
        field_hashes: &HashMap<String, String>,
    ) -> Result<(), DeclaredFieldError> {
        if field_hashes.is_empty() {
            return Ok(());
        }
        let registry = self
            .declared_fields
            .read()
            .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
        if registry.is_empty() {
            return Ok(());
        }
        registry.authorize_references(owner_app_id, field_hashes.values().map(String::as_str))
    }
}

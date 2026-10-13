//! Pure steps of [`super::DeclaredFieldRegistry::declare`]: validate the request,
//! merge fields, and grant readers.

use super::*;

/// Reject a malformed declare request; returns the trimmed owner and handle.
pub(super) fn validate_declare_request(
    request: &DeclareFieldRequest,
) -> Result<(&str, &str), DeclaredFieldError> {
    let owner = request.owner_app_id.trim();
    if owner.is_empty() {
        return Err(DeclaredFieldError::OwnerRequired(
            "owner_app_id is required — a field with no verified owner is a \
             proposal, not a declaration"
                .to_string(),
        ));
    }
    let handle = request.handle.trim();
    if handle.is_empty() {
        return Err(DeclaredFieldError::InvalidHandle(
            "handle must be non-empty".to_string(),
        ));
    }
    if request.fields.is_empty() {
        return Err(DeclaredFieldError::InvalidField(
            "declare at least one field".to_string(),
        ));
    }
    if request.fields.len() > MAX_FIELDS_PER_DECLARATION {
        return Err(DeclaredFieldError::InvalidField(format!(
            "at most {MAX_FIELDS_PER_DECLARATION} fields per declaration"
        )));
    }
    let mut seen = HashSet::new();
    for field in &request.fields {
        let name = field.name.trim();
        if name.is_empty() {
            return Err(DeclaredFieldError::InvalidField(
                "field name must be non-empty".to_string(),
            ));
        }
        if !seen.insert(name) {
            return Err(DeclaredFieldError::InvalidField(format!(
                "duplicate field name `{name}` in one declaration"
            )));
        }
    }
    Ok((owner, handle))
}

/// Insert or replace each requested field, keeping the record sorted by name.
pub(super) fn merge_declared_fields(
    record: &mut DeclaredFieldRecord,
    request: &DeclareFieldRequest,
) {
    for input in &request.fields {
        let name = input.name.trim().to_string();
        let identity = compute_declared_field_identity(
            &record.declaration_id,
            &name,
            &input.field_type,
            input.version,
        );
        let field = DeclaredField {
            name: name.clone(),
            field_type: input.field_type.clone(),
            description: input.description.trim().to_string(),
            version: input.version,
            identity,
        };
        match record.fields.iter_mut().find(|f| f.name == name) {
            // Description edits are free; identity is unaffected by them.
            Some(existing) => *existing = field,
            None => record.fields.push(field),
        }
    }
    record.fields.sort_by(|a, b| a.name.cmp(&b.name));
}

/// Add the requested readers to the record.
pub(super) fn grant_readers(
    record: &mut DeclaredFieldRecord,
    request: &DeclareFieldRequest,
    owner: &str,
) {
    // Grants are additive. Removing one is a separate, deliberate act.
    for reader in &request.readers {
        let reader = reader.trim();
        if !reader.is_empty() && reader != owner && !record.readers.iter().any(|r| r == reader) {
            record.readers.push(reader.to_string());
        }
    }
    record.readers.sort();
}

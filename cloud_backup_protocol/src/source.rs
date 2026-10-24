//! Read only an existing authorized consumer copy. No registration or adoption.
use crate::{
    limits, CloudScope, DecimalU64, Digest, FileDependencyKind, ProtocolError, Text, Version,
};
use serde::{Deserialize, Serialize};
pub const SOURCE_READ_OBJECTS_MAX: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadSelector {
    pub kind: FileDependencyKind,
    pub owner_scope: Text<128>,
    pub app_id: Option<Text<128>>,
    /// Object-path digest: ciphertext SHA-256 for account-v2 files, plaintext
    /// SHA-256 for old suites. The wire name remains stable for old services.
    pub file_hash: Digest,
    pub expected_ciphertext_bytes: DecimalU64,
}
impl SourceReadSelector {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        component(self.owner_scope.as_str())?;
        if let Some(app_id) = &self.app_id {
            component(app_id.as_str())?;
        }
        if self.expected_ciphertext_bytes.get() == 0
            || self.expected_ciphertext_bytes.get() > limits::BACKUP_STORED_BYTES_MAX
        {
            return Err(ProtocolError::limit("source.ciphertext_bytes"));
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}
fn component(value: &str) -> Result<(), ProtocolError> {
    if value == "." || value.contains("..") || value.contains(['/', '\\']) || value.trim() != value
    {
        return Err(ProtocolError::invalid("source.component"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadRequest {
    pub version: Version,
    pub scope: CloudScope,
    pub objects: Vec<SourceReadSelector>,
}
impl SourceReadRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.objects.is_empty() || self.objects.len() > SOURCE_READ_OBJECTS_MAX {
            return Err(ProtocolError::limit("source.objects"));
        }
        for (index, selector) in self.objects.iter().enumerate() {
            selector.validate()?;
            if self.objects[..index].iter().any(|previous| {
                previous.owner_scope == selector.owner_scope
                    && previous.app_id == selector.app_id
                    && previous.file_hash == selector.file_hash
            }) {
                return Err(ProtocolError::invalid("source.duplicate"));
            }
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// An object-path digest alone is not proof of a file. Original encryption,
/// plaintext identity, exact length and EOF still require local verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadGrant {
    pub bytes: DecimalU64,
    pub expires_ms: DecimalU64,
    pub url: Text<8192>,
    pub if_match: Text<256>,
}
impl SourceReadGrant {
    pub fn validate_for(&self, selector: &SourceReadSelector) -> Result<(), ProtocolError> {
        selector.validate()?;
        validate_source_etag(self.if_match.as_str())?;
        if self.bytes != selector.expected_ciphertext_bytes || self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("source.grant"));
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

/// Require one quoted strong entity tag. Wildcards, weak tags and header lists
/// cannot select the exact saved consumer copy. Multipart tags remain valid.
pub fn validate_source_etag(value: &str) -> Result<(), ProtocolError> {
    let bytes = value.as_bytes();
    if bytes.len() < 3
        || bytes.len() > 256
        || bytes[0] != b'"'
        || bytes[bytes.len() - 1] != b'"'
        || bytes[1..bytes.len() - 1]
            .iter()
            .any(|byte| *byte == b'"' || *byte < 0x21 || *byte == 0x7f)
    {
        return Err(ProtocolError::invalid("source.if_match"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceReadResult {
    Grant {
        selector: SourceReadSelector,
        grant: SourceReadGrant,
    },
    Error {
        selector: SourceReadSelector,
        error: ProtocolError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadResponse {
    pub version: Version,
    pub scope: CloudScope,
    pub results: Vec<SourceReadResult>,
}
impl SourceReadResponse {
    pub fn validate_for(&self, request: &SourceReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        if self.scope != request.scope || self.results.len() != request.objects.len() {
            return Err(ProtocolError::invalid("source.response"));
        }
        for (expected, result) in request.objects.iter().zip(&self.results) {
            let selector = match result {
                SourceReadResult::Grant { selector, grant } => {
                    grant.validate_for(expected)?;
                    selector
                }
                SourceReadResult::Error { selector, .. } => selector,
            };
            if selector != expected {
                return Err(ProtocolError::invalid("source.selector"));
            }
        }
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES).map(|_| ())
    }
}

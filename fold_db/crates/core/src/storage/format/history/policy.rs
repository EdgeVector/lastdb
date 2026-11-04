//! The history policy and activation marker use the same format-owned writer.

use super::{policy_key, EncodedHistory};
use crate::records::field_history::FieldHistoryPolicy;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

pub(super) fn validate_policy(field: &str, policy: FieldHistoryPolicy) -> Result<(), SchemaError> {
    if let FieldHistoryPolicy::Ttl { seconds } = policy {
        if seconds == 0 || seconds.checked_mul(1_000_000_000).is_none() {
            return Err(SchemaError::InvalidData(
                "history TTL must be positive and fit nanoseconds".into(),
            ));
        }
    }
    if field.len() != 64 || !field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SchemaError::InvalidData(
            "history needs a canonical field identity".into(),
        ));
    }
    Ok(())
}

pub(crate) fn policy_batch(
    field: &str,
    policy: FieldHistoryPolicy,
) -> Result<EncodedHistory, SchemaError> {
    validate_policy(field, policy)?;
    let mut rows = vec![KvMutation::put(
        policy_key(field),
        serde_json::to_vec(&policy).map_err(|error| SchemaError::InvalidData(error.to_string()))?,
    )];
    if policy != FieldHistoryPolicy::Off {
        rows.push(KvMutation::put(
            crate::records::key_format::FIELD_HISTORY_KEY
                .as_bytes()
                .to_vec(),
            crate::records::key_format::FIELD_HISTORY_ON.to_vec(),
        ));
    }
    Ok(EncodedHistory { rows })
}

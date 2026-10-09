//! `X-API-Key` validation for the auth-gated `GET /v1/snapshot` route.
//!
//! The hosted deployment checks the key against a DynamoDB table that the
//! operator of the deployment owns. This module is the whole contract between
//! the Lambda and that table, so a different operator can provide a table with
//! the same shape:
//!
//! - hash key `api_key_hash` (string): lowercase hex SHA-256 of the key
//! - `is_active` (bool): the key is accepted only when this is `true`
//! - `user_hash` (string): the identity the key belongs to
//!
//! The DynamoDB client is injected, so this module needs no Secrets Manager,
//! Lambda runtime, or OpenTelemetry code.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use sha2::{Digest, Sha256};

/// Hash an API key with SHA-256 and return it as 64 lowercase hex characters.
pub(crate) fn hash_api_key(api_key: &str) -> String {
    format!("{:x}", Sha256::digest(api_key.as_bytes()))
}

/// Validate an API key by a direct DynamoDB `GetItem`.
///
/// Returns the `user_hash` when the key exists and is active.
pub(crate) async fn validate_api_key(
    dynamo: &DynamoClient,
    table_name: &str,
    api_key: &str,
) -> Result<String, String> {
    let result = dynamo
        .get_item()
        .table_name(table_name)
        .key("api_key_hash", AttributeValue::S(hash_api_key(api_key)))
        .send()
        .await
        .map_err(|e| format!("Failed to validate API key: {e}"))?;

    let item = result.item().ok_or_else(|| "Invalid API key".to_string())?;

    let is_active = item
        .get("is_active")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    if !is_active {
        return Err("API key is deactivated".to_string());
    }

    item.get("user_hash")
        .and_then(|v| v.as_s().ok())
        .map(String::to_string)
        .ok_or_else(|| "API key valid but no user_hash found".to_string())
}

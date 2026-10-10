//! Environment-driven client configuration for an S3-compatible schema store
//! (R2, MinIO, ...). Shared by the request-path Lambda and the worker binaries
//! so the two cannot drift.

pub const SCHEMA_STORE_R2_ENDPOINT_ENV: &str = "SCHEMA_STORE_R2_ENDPOINT";
const SCHEMA_STORE_ACCESS_KEY_ID_ENV: &str = "SCHEMA_STORE_ACCESS_KEY_ID";
const SCHEMA_STORE_SECRET_ACCESS_KEY_ENV: &str = "SCHEMA_STORE_SECRET_ACCESS_KEY";
const SCHEMA_STORE_REGION_ENV: &str = "SCHEMA_STORE_REGION";
const SCHEMA_STORE_DEFAULT_REGION: &str = "auto";

/// Static credentials for an S3-compatible store, read from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStoreS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    region: String,
}

/// `Ok(None)` when neither key is set; an error when only one of them is.
pub fn schema_store_s3_credentials_from_env() -> Result<Option<SchemaStoreS3Credentials>, String> {
    let access_key_id = env_var_trimmed(SCHEMA_STORE_ACCESS_KEY_ID_ENV);
    let secret_access_key = env_var_trimmed(SCHEMA_STORE_SECRET_ACCESS_KEY_ENV);
    match (access_key_id, secret_access_key) {
        (None, None) => Ok(None),
        (Some(access_key_id), Some(secret_access_key)) => Ok(Some(SchemaStoreS3Credentials {
            access_key_id,
            secret_access_key,
            region: env_var_trimmed(SCHEMA_STORE_REGION_ENV)
                .unwrap_or_else(|| SCHEMA_STORE_DEFAULT_REGION.to_string()),
        })),
        _ => Err(format!(
            "{SCHEMA_STORE_ACCESS_KEY_ID_ENV} and {SCHEMA_STORE_SECRET_ACCESS_KEY_ENV} must both be set for S3-compatible schema-store credentials"
        )),
    }
}

/// Point `builder` at `endpoint_url` (path-style) and apply `credentials`.
/// Without an endpoint the builder is returned unchanged.
pub fn schema_store_s3_config_builder(
    mut builder: aws_sdk_s3::config::Builder,
    endpoint_url: Option<&str>,
    credentials: Option<&SchemaStoreS3Credentials>,
) -> aws_sdk_s3::config::Builder {
    if let Some(endpoint_url) = endpoint_url {
        builder = builder.endpoint_url(endpoint_url).force_path_style(true);
        if let Some(credentials) = credentials {
            let s3_credentials = aws_sdk_s3::config::Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "schema-store-s3-compatible",
            );
            builder = builder
                .region(aws_sdk_s3::config::Region::new(credentials.region.clone()))
                .credentials_provider(s3_credentials);
        }
    }
    builder
}

/// Env var value with whitespace trimmed; `None` when unset or blank.
pub fn env_var_trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

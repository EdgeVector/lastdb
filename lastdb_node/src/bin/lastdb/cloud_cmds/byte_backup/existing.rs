//! Owner-only import of an existing API key. This command does not enroll an account.
use super::*;
use cloud_backup_protocol::AccountId;
use lastdb_node::byte_cloud_backup::SensitiveConnectionRequest;
use std::io::Read;
use zeroize::Zeroizing;

pub(super) fn run(
    home: &Path,
    environment: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    if !home.is_absolute() || !matches!(environment, "dev" | "prod") {
        return Err(
            "existing connection requires an absolute home and dev or prod environment".into(),
        );
    }
    let expected_user_hash = expected
        .map(|value| AccountId::try_from(value.to_owned()))
        .transpose()
        .map_err(|_| "invalid expected account identity")?;
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read existing account API key")?;
    if bytes.len() > 4096 {
        return Err("existing account API key exceeds its byte limit".into());
    }
    let api_key = std::str::from_utf8(&bytes)
        .map_err(|_| "API key must be UTF-8")?
        .trim();
    if api_key.is_empty() || api_key.chars().any(char::is_control) {
        return Err("existing account API key is empty or invalid".into());
    }
    let request = SensitiveConnectionRequest {
        environment: environment.to_owned(),
        expected_user_hash,
        api_key: api_key.to_owned(),
    };
    let body = Zeroizing::new(
        serde_json::to_vec(&request).map_err(|_| "cannot encode protected connection request")?,
    );
    let mut wire = Zeroizing::new(format!(
        "POST /api/sync/byte-backup-v2/connect HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        super::super::super::socket_client::client_headers(), body.len(),
    ).into_bytes());
    wire.extend_from_slice(&body);
    let response = super::super::super::socket_client::request_with_timeout(
        &home.join("data/folddb.sock"),
        &wire,
        admin_scan_client_timeout(),
    )?;
    print(
        &parse_json_response(&response, "V2 existing account connection")?,
        json,
    )
}

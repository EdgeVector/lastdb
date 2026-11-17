//! One Schema Service definition, by the signed portable id.
//!
//! `GET <schema service>/v1/schemas/<definition id>` takes no credentials.
//! The address is `endpoints::schema_service_url`: `FOLD_SCHEMA_SERVICE_URL`,
//! or the URL for the environment already pinned for this install
//! (`LASTDB_ENV` / `ENVIRONMENT` / `EXEMEM_ENV`, then the home cloud
//! environment, then the build default). The request waits at most five
//! seconds. A miss is not an install error.

use std::time::Duration;

use fold_db::schema::types::DeclarativeSchemaDefinition;
use serde::Deserialize;

use super::definition_id;

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BYTES: u64 = 2 * 1024 * 1024;

pub(super) fn fetch(signed_id: &str) -> Result<DeclarativeSchemaDefinition, String> {
    crate::block_on("schema service", fetch_one(signed_id))
}

async fn fetch_one(signed_id: &str) -> Result<DeclarativeSchemaDefinition, String> {
    let url = service_url(signed_id)?;
    // trace-egress: public — Schema Service definition fetch sends no credentials.
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TIMEOUT)
        .connect_timeout(TIMEOUT)
        .build()
        .map_err(|_| "client unavailable".to_owned())?;
    let response = client.get(url).send().await.map_err(miss_from_transport)?;
    let status = response.status();
    if status.as_u16() == 404 {
        return Err("returned 404".into());
    }
    if !status.is_success() {
        return Err(format!("returned {}", status.as_u16()));
    }
    let bytes = lastdb_node::cloud_backup_client::read_bounded_bytes(response, MAX_BYTES)
        .await
        .map_err(|_| "response was too large or unreadable".to_owned())?;
    let reply: Reply = serde_json::from_slice(&bytes)
        .map_err(|_| "response was not a schema definition".to_owned())?;
    let id = definition_id::portable_id(&reply.definition)
        .map_err(|error| format!("definition id could not be checked: {error}"))?;
    if id != signed_id {
        return Err(format!(
            "returned definition id {id}, which does not match the signed id"
        ));
    }
    Ok(reply.definition)
}

#[derive(Deserialize)]
struct Reply {
    definition: DeclarativeSchemaDefinition,
}

fn service_url(definition_id: &str) -> Result<reqwest::Url, String> {
    if !hex_id(definition_id) {
        return Err("definition id is malformed".into());
    }
    let base = lastdb_node::profile::endpoints::schema_service_url();
    let base = base.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(&format!("{base}/v1/schemas/{definition_id}"))
        .map_err(|_| "address is not a URL".to_owned())?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err("address is not HTTPS".into());
    }
    Ok(url)
}

fn hex_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn miss_from_transport(error: reqwest::Error) -> String {
    if error.is_timeout() {
        "timed out".to_owned()
    } else if error.is_connect() {
        "unreachable".to_owned()
    } else {
        "request failed".to_owned()
    }
}

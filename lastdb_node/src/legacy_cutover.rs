//! Publisher cutover for one existing database.
//!
//! Dev and prod use [`MUTATION_LOG_CUTOVER_PATH`]. The body is D's
//! [`CutoverReceipt`]. The node signs those exact bytes with
//! [`crate::node_read::authorization`]. The live TLS body is the only
//! authority. This module does not write a file, open the store, or generate
//! an identity.
//!
//! A fresh database does not use this module. The normal signed create
//! registers it.

use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::share_access::{
    CommittedMark, CutoverReceipt, ShareEnvironment, MUTATION_LOG_CUTOVER_PATH,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{canonical_bytes, AccountId, DecimalU64, Digest, Id, ProtocolError};
use fold_db::security::Ed25519KeyPair;
use futures::StreamExt;
use reqwest::Url;
use zeroize::Zeroizing;

use crate::profile::endpoints::{exemem_api_url_for, Environment};

const RESPONSE_CAP: usize = 16 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Values the evidence check already accepted for one existing database.
pub struct CutoverSubmit {
    /// Cloud database id. This is a UUID.
    pub database_id: String,
    /// Evidence digest from `database_cutover::verify`.
    pub evidence_sha256: String,
    /// Grant revision named by that evidence. Zero is rejected.
    pub grant_revision: u64,
    /// Publisher or personal owner node key, lowercase hex.
    pub authority_node_key: String,
}

struct SavedService {
    environment: ShareEnvironment,
    api_url: String,
}

/// Realm of the saved cloud connection. Dev and prod use the same check.
pub fn connection_realm(home: &Path) -> Result<ShareEnvironment, String> {
    Ok(saved(home)?.environment)
}

/// Send one idempotent cutover and return the live receipt.
///
/// A repeated call sends the same receipt bytes and a new nonce. The service
/// decides whether that receipt is already stored. Nothing is written locally.
pub async fn post(home: &Path, submit: &CutoverSubmit) -> Result<CutoverReceipt, String> {
    let saved = saved(home)?;
    let keypair = load_keypair(home)?;
    require_authority(&keypair, &submit.authority_node_key)?;
    let scope = account_scope(home, &keypair).await?;
    let receipt = build_receipt(submit, saved.environment)?;
    let body = canonical_bytes(&receipt).map_err(|_| "cutover receipt was refused".to_owned())?;
    let header = crate::node_read::authorization(
        &keypair,
        crate::node_read::SignedPostTarget {
            scope: &scope,
            database_id: &receipt.database_id,
            environment: saved.environment,
            route: crate::node_read::CanonicalPost::Cutover,
        },
        &body,
        submit.grant_revision,
        fold_db::clock::unix_millis(),
    )?;
    let url = cutover_url(&saved.api_url)?;
    let bytes = post_body(&url, &header, body).await?;
    accept(&receipt, &bytes)
}

fn saved(home: &Path) -> Result<SavedService, String> {
    refuse_primary(home)?;
    let loaded = crate::cloud::load_saved_credentials(home)?
        .ok_or("cutover needs saved cloud credentials")?;
    let connection = loaded
        .connection
        .as_ref()
        .ok_or("cutover needs the saved cloud connection")?;
    let environment = match connection.environment.as_str() {
        "dev" => ShareEnvironment::Dev,
        "prod" => ShareEnvironment::Prod,
        _ => return Err("cloud connection environment is not dev or prod".into()),
    };
    let registry = match environment {
        ShareEnvironment::Dev => Environment::Dev,
        ShareEnvironment::Prod => Environment::Prod,
    };
    if loaded.api_url != exemem_api_url_for(registry) {
        return Err("cloud endpoint does not match the saved connection".into());
    }
    origin_url(&loaded.api_url)?;
    Ok(SavedService {
        environment,
        api_url: loaded.api_url,
    })
}

fn refuse_primary(home: &Path) -> Result<(), String> {
    let located = home
        .canonicalize()
        .map_err(|_| "cannot read the cloud home".to_owned())?;
    let Some(owner) = std::env::var_os("HOME") else {
        return Err("cannot identify the primary LastDB home".into());
    };
    let primary = std::path::PathBuf::from(owner).join(".lastdb");
    let primary = match primary.canonicalize() {
        Ok(path) => path,
        Err(_) => primary,
    };
    if located.starts_with(&primary) {
        return Err("refusing the primary LastDB home".into());
    }
    Ok(())
}

fn load_keypair(home: &Path) -> Result<Ed25519KeyPair, String> {
    let path = home.join(crate::identity::IDENTITY_KEY_FILE);
    let metadata = std::fs::symlink_metadata(&path).map_err(|_| "cannot read the node identity")?;
    if !metadata.file_type().is_file() {
        return Err("node identity must be a regular file".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|_| "cannot read the node identity")?;
    let mut bytes = Vec::new();
    file.take(33)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read the node identity")?;
    if bytes.len() != 32 {
        return Err("node identity has the wrong length".into());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    let seed = Zeroizing::new(seed);
    Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "node identity is not an Ed25519 seed".into())
}

fn require_authority(keypair: &Ed25519KeyPair, authority: &str) -> Result<(), String> {
    if fold_db::hex::hex_lower(keypair.public_key_bytes()) == authority {
        Ok(())
    } else {
        Err("the node key is not the evidence authority".into())
    }
}

async fn account_scope(
    home: &Path,
    keypair: &Ed25519KeyPair,
) -> Result<cloud_backup_protocol::CloudScope, String> {
    let derived = crate::identity::user_hash_from_pubkey(&keypair.public_key_base64())?;
    let account = AccountId::try_from(derived)
        .map_err(|_| "node account id is not a cloud account id".to_owned())?;
    let backup = crate::byte_cloud_backup::client_from_home(home)?;
    let context = backup
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|_| "cloud account context failed")?;
    if context.scope.account_id != account {
        return Err("cloud account does not match this node key".into());
    }
    Ok(context.scope)
}

fn build_receipt(
    submit: &CutoverSubmit,
    environment: ShareEnvironment,
) -> Result<CutoverReceipt, String> {
    let receipt = CutoverReceipt {
        state: CommittedMark::Committed,
        version: Version::V2,
        environment,
        database_id: Id::try_from(String::from(submit.database_id.as_str()))
            .map_err(|_| "database id is not a cloud database id")?,
        evidence_sha256: Digest::try_from(String::from(submit.evidence_sha256.as_str()))
            .map_err(|_| "evidence digest was refused")?,
        grant_revision: DecimalU64::new(submit.grant_revision),
    };
    receipt
        .validate()
        .map_err(|_| "cutover receipt was refused")?;
    Ok(receipt)
}

fn origin_url(api_url: &str) -> Result<Url, String> {
    let api = Url::parse(api_url).map_err(|_| "cloud endpoint is not a URL")?;
    if api.scheme() != "https" || !api.username().is_empty() || api.password().is_some() {
        return Err("cutover requires an HTTPS cloud endpoint".into());
    }
    if !(api.path().is_empty() || api.path() == "/") {
        return Err("cutover requires an origin cloud endpoint".into());
    }
    if api.host_str().is_none() {
        return Err("cloud endpoint has no host".into());
    }
    Ok(api)
}

fn cutover_url(api_url: &str) -> Result<Url, String> {
    let api = origin_url(api_url)?;
    let host = api.host_str().ok_or("cloud endpoint has no host")?;
    let origin = match api.port() {
        Some(port) => format!("{}://{host}:{port}", api.scheme()),
        None => format!("{}://{host}", api.scheme()),
    };
    Url::parse(&format!("{origin}{MUTATION_LOG_CUTOVER_PATH}"))
        .map_err(|_| "cutover URL was refused".to_owned())
}

async fn post_body(url: &Url, header: &str, body: Vec<u8>) -> Result<Vec<u8>, String> {
    let client = crate::cloud_backup_client::build_shared_http_client()
        .map_err(|_| "cannot build the cutover client")?;
    let request = client
        .post(url.clone())
        .timeout(REQUEST_TIMEOUT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::AUTHORIZATION, header)
        .body(body);
    let response = observability::propagation::inject_w3c(request)
        .send()
        .await
        .map_err(|_| "cutover request failed")?;
    let status = response.status();
    let bytes = read_capped(response).await?;
    if status.is_success() {
        Ok(bytes)
    } else {
        Err(refusal(status.as_u16(), &bytes))
    }
}

async fn read_capped(response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut collected = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|_| "cutover response was interrupted".to_owned())?;
        if collected.len().saturating_add(chunk.len()) > RESPONSE_CAP {
            return Err("cutover response exceeds its size cap".into());
        }
        collected.extend_from_slice(&chunk);
    }
    Ok(collected)
}

fn refusal(status: u16, bytes: &[u8]) -> String {
    match serde_json::from_slice::<ProtocolError>(bytes) {
        Ok(error) => format!("cutover was refused: {error}"),
        Err(_) => format!("cutover was refused: HTTP {status}"),
    }
}

fn accept(sent: &CutoverReceipt, bytes: &[u8]) -> Result<CutoverReceipt, String> {
    let got = serde_json::from_slice::<CutoverReceipt>(bytes)
        .map_err(|_| "cutover response was not a committed receipt".to_owned())?;
    got.validate()
        .map_err(|_| "cutover response was not a committed receipt")?;
    if &got == sent {
        Ok(got)
    } else {
        Err("cutover response does not match this database".into())
    }
}

//! Saved publisher session. The API key is used for context, then dropped.

use crate::cloud::load_saved_credentials;
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use crate::cloud_backup_client::byte_backup::ShareEnvironment as GuestEnvironment;
use crate::cloud_backup_client::{build_shared_http_client, AuthClient};
use crate::identity::{user_hash_from_pubkey, IDENTITY_KEY_FILE};
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{AccountId, CloudScope, Digest};
use fold_db::security::Ed25519KeyPair;
use reqwest::Url;
use std::fs::OpenOptions;
use std::io::Read as _;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) struct PublisherSession {
    pub http: reqwest::Client,
    pub origin: Url,
    pub environment: ShareEnvironment,
    pub guest_environment: GuestEnvironment,
    pub signer: Ed25519KeyPair,
    pub publisher: Digest,
    pub scope: CloudScope,
    pub seed: Zeroizing<[u8; 32]>,
}

pub(super) async fn open(home: &Path) -> Result<PublisherSession, String> {
    let saved =
        load_saved_credentials(home)?.ok_or("share publisher has no saved Exemem credentials")?;
    let connection = saved
        .connection
        .as_ref()
        .ok_or("share publisher has no saved account connection")?;
    let (environment, guest_environment, profile) = realms(&connection.environment)?;
    let endpoint = exemem_api_url_for(profile);
    if saved.api_url != endpoint || connection.endpoint != endpoint || saved.api_key.is_empty() {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    let seed = read_seed(home)?;
    let signer = Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid share publisher key")?;
    let publisher = Digest::try_from(hex::encode(signer.public_key_bytes()))
        .map_err(|_| "invalid share publisher public key")?;
    let account = account_id(&signer)?;
    if connection.account_id != account {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    let scope = read_scope(&saved.api_url, saved.api_key.as_str(), &account).await?;
    if scope.account_id != account {
        return Err("share.grant.identity".into());
    }
    drop(saved);
    Ok(PublisherSession {
        http: build_shared_http_client().map_err(|_| "cannot construct member service client")?,
        origin: origin(endpoint)?,
        environment,
        guest_environment,
        signer,
        publisher,
        scope,
        seed,
    })
}

fn realms(value: &str) -> Result<(ShareEnvironment, GuestEnvironment, Environment), String> {
    match value {
        "dev" => Ok((
            ShareEnvironment::Dev,
            GuestEnvironment::Dev,
            Environment::Dev,
        )),
        "prod" => Ok((
            ShareEnvironment::Prod,
            GuestEnvironment::Prod,
            Environment::Prod,
        )),
        _ => Err("saved credentials do not match the share publisher and environment".into()),
    }
}

fn account_id(signer: &Ed25519KeyPair) -> Result<AccountId, String> {
    let hash = user_hash_from_pubkey(&signer.public_key_base64())
        .map_err(|_| "invalid share publisher account")?;
    AccountId::try_from(hash).map_err(|_| "invalid share publisher account".into())
}

async fn read_scope(url: &str, api_key: &str, account: &AccountId) -> Result<CloudScope, String> {
    let client = ByteBackupClient::from_auth(AuthClient::new(url.to_owned(), api_key.to_owned()))
        .map_err(|_| "share publisher credentials were refused")?;
    let response = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|_| "share publisher account context was refused")?;
    Ok(response.scope)
}

fn origin(endpoint: &str) -> Result<Url, String> {
    let url = Url::parse(endpoint)
        .map_err(|_| "saved credentials do not match the share publisher and environment")?;
    let empty_path = url.path().is_empty() || url.path() == "/";
    if url.scheme() != "https" || !empty_path || url.query().is_some() || !url.username().is_empty()
    {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    Ok(url)
}

fn read_seed(home: &Path) -> Result<Zeroizing<[u8; 32]>, String> {
    let path = home.join(IDENTITY_KEY_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|_| "share requires an initialized LastDB identity")?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect LastDB identity")?;
    if !metadata.is_file()
        || metadata.len() != 32
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err("share requires an initialized LastDB identity".into());
    }
    let mut bytes = [0u8; 32];
    file.read_exact(&mut bytes)
        .map_err(|_| "cannot read LastDB identity")?;
    Ok(Zeroizing::new(bytes))
}

impl std::fmt::Debug for PublisherSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublisherSession")
            .field("publisher", &self.publisher)
            .field("environment", &self.environment)
            .finish()
    }
}

//! Saved publisher session. The API key is used for context, then dropped.

use crate::cloud::{load_home_keypair, load_saved_credentials};
use crate::cloud_backup_client::{
    build_shared_http_client, AuthClient, BackupTransportError, SignedBlobClient,
};
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use crate::service_home::user_hash_from_pubkey;
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{AccountId, CloudScope, Digest};
use fold_db::security::Ed25519KeyPair;
use reqwest::Url;
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct PublisherSession {
    pub http: reqwest::Client,
    pub origin: Url,
    pub environment: ShareEnvironment,
    pub signer: Arc<Ed25519KeyPair>,
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
    let (environment, profile) = realms(&connection.environment)?;
    let endpoint = exemem_api_url_for(profile);
    if saved.api_url != endpoint {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    let signer =
        Arc::new(load_home_keypair(home)?.ok_or("share requires an initialized LastDB identity")?);
    let seed = Zeroizing::new(signer.secret_key_bytes());
    let backup = SignedBlobClient::from_auth_and_keypair(
        AuthClient::new(endpoint.to_owned(), saved.api_key.as_str().to_owned()),
        Arc::clone(&signer),
    )
    .map_err(|_| "share publisher credentials were refused")?;
    if connection.endpoint != backup.canonical_endpoint() {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    let publisher = Digest::try_from(hex::encode(signer.public_key_bytes()))
        .map_err(|_| "invalid share publisher public key")?;
    let account = account_id(&signer)?;
    if connection.account_id != account {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    let scope = read_scope(&backup, &account).await?;
    if scope.account_id != account {
        return Err("share.grant.identity".into());
    }
    drop((backup, saved));
    Ok(PublisherSession {
        http: build_shared_http_client().map_err(|_| "cannot construct member service client")?,
        origin: origin(endpoint)?,
        environment,
        signer,
        publisher,
        scope,
        seed,
    })
}

fn realms(value: &str) -> Result<(ShareEnvironment, Environment), String> {
    match value {
        "dev" => Ok((ShareEnvironment::Dev, Environment::Dev)),
        "prod" => Ok((ShareEnvironment::Prod, Environment::Prod)),
        _ => Err("saved credentials do not match the share publisher and environment".into()),
    }
}

fn account_id(signer: &Ed25519KeyPair) -> Result<AccountId, String> {
    let hash = user_hash_from_pubkey(&signer.public_key_base64())
        .map_err(|_| "invalid share publisher account")?;
    AccountId::try_from(hash).map_err(|_| "invalid share publisher account".into())
}

async fn read_scope(backup: &SignedBlobClient, account: &AccountId) -> Result<CloudScope, String> {
    let response = backup
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|error| {
            if matches!(&error, BackupTransportError::Protocol(p) if p.is_payment_subscription()) {
                crate::cloud::PAYMENT_REQUIRED_MESSAGE.to_owned()
            } else {
                "share publisher account context was refused".to_owned()
            }
        })?;
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

impl std::fmt::Debug for PublisherSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublisherSession")
            .field("publisher", &self.publisher)
            .field("environment", &self.environment)
            .finish()
    }
}

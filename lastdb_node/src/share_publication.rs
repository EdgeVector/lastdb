//! Local publication ownership. Cloud upload authority stays with the owner's home.
use crate::byte_cloud_backup::connection::files::Directory;
use crate::byte_cloud_backup::PublicationSource;
use crate::cloud_backup_client::byte_backup::{
    BackupTransportError, ByteBackupClient, ByteBackupReader, GuestReadPermission, ShareEnvironment,
};
use crate::cloud_backup_client::AuthClient;
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use cloud_backup_protocol::v2::{
    context::ContextReadRequest, publication::HeadReadResult, Stream, Version,
};
use cloud_backup_protocol::{AccountId, Digest, Id};
use fold_db::{crypto::E2eKeys, security::Ed25519KeyPair};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};
use tracing::Instrument;
use zeroize::Zeroizing;

pub use crate::byte_cloud_backup::PUBLICATION_MARKER as MARKER_FILE;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    PublisherOnly,
    PublisherValidated,
}

impl WriteMode {
    fn is_publisher_only(&self) -> bool {
        *self == Self::PublisherOnly
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationMarker {
    version: u8,
    pub id: Id,
    pub publisher: Digest,
    pub environment: ShareEnvironment,
    #[serde(default, skip_serializing_if = "WriteMode::is_publisher_only")]
    pub write_mode: WriteMode,
}

pub struct PreparedPublication {
    pub marker: PublicationMarker,
    pub seed: Zeroizing<[u8; 32]>,
}

pub(crate) struct PublicationIdentity {
    pub(crate) id: Id,
    publisher: Digest,
    pub(crate) account: AccountId,
    content_key: Zeroizing<[u8; 32]>,
    pub(crate) environment: ShareEnvironment,
    pub(crate) signer: Ed25519KeyPair,
    pub(crate) write_mode: WriteMode,
}

/// Validated configuration for one running publication home. The owner's
/// signing key is used to prepare this binding and is not kept in the daemon.
pub struct AutomaticPublication {
    home: PathBuf,
    owner_home: PathBuf,
    environment: ShareEnvironment,
    source: PublicationSource,
    inbox: Option<x25519_dalek::StaticSecret>,
}

pub fn prepare_automatic(home: &Path, owner_home: &Path) -> Result<AutomaticPublication, String> {
    let identity = load(home, owner_home)?;
    identity.client(owner_home)?;
    crate::byte_cloud_backup::automatic_publication::preflight(home).map_err(|e| e.to_string())?;
    let environment = identity.environment;
    let inbox = if identity.write_mode == WriteMode::PublisherValidated {
        let owner = owner_seed(owner_home)?;
        Some(crate::share_contribution::inbox_key(&owner, &identity.id)?)
    } else {
        None
    };
    Ok(AutomaticPublication {
        home: home.to_owned(),
        owner_home: owner_home.to_owned(),
        environment,
        source: identity.into_source(),
        inbox,
    })
}

impl AutomaticPublication {
    pub fn spawn(
        self,
        host: Arc<crate::host::Host>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| "invalid live publication key")?;
        if host.home != self.home || keys.encryption_key() != *self.source.content_key {
            return Err("running database differs from the prepared publication".into());
        }
        let Self {
            source,
            owner_home,
            environment,
            inbox,
            ..
        } = self;
        let account = source.account.clone();
        let publication = source.id.clone();
        let backup_home = owner_home.clone();
        let backup_account = account.clone();
        let backup = crate::byte_cloud_backup::automatic_publication::spawn(
            Arc::clone(&host),
            Arc::clone(&shutdown),
            source,
            move || publication_client(&backup_home, &backup_account, environment),
        )
        .map_err(|e| e.to_string())?;
        let Some(inbox) = inbox else {
            return Ok(backup);
        };
        Ok(tokio::spawn(
            async move {
                let follow = crate::share_contribution::follow(
                    host,
                    shutdown,
                    &owner_home,
                    &account,
                    &publication,
                    &inbox,
                    || publication_client(&owner_home, &account, environment),
                );
                let _ = tokio::join!(backup, follow);
            }
            .in_current_span(),
        ))
    }
}

/// Prepare a new ordinary database identity, separate from the owner's identity.
pub fn prepare(
    owner_home: &Path,
    environment: ShareEnvironment,
    write_mode: WriteMode,
) -> Result<PreparedPublication, String> {
    let owner = owner_seed(owner_home)?;
    let signer = signer(&owner)?;
    let id =
        Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(|_| "invalid publication ID")?;
    let seed = publication_seed(&owner, &id)?;
    Ok(PreparedPublication {
        marker: PublicationMarker {
            version: 1,
            id,
            publisher: publisher(&signer)?,
            environment,
            write_mode,
        },
        seed,
    })
}

pub(crate) fn load(home: &Path, owner_home: &Path) -> Result<PublicationIdentity, String> {
    crate::offline_home::refuse_primary(home)?;
    let directory = owned(home)?;
    let marker = read_marker(&directory)?.ok_or("share publication marker is missing")?;
    let owner = owner_seed(owner_home)?;
    let signer = signer(&owner)?;
    if publisher(&signer)? != marker.publisher {
        return Err("share publication belongs to a different publisher".into());
    }
    let seed = publication_seed(&owner, &marker.id)?;
    if *seed != *read_seed(&directory)? {
        return Err("share publication does not match its database identity".into());
    }
    let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| "invalid share database key")?;
    let account = crate::identity::user_hash_from_pubkey(&signer.public_key_base64())?;
    Ok(PublicationIdentity {
        id: marker.id,
        publisher: marker.publisher,
        account: AccountId::try_from(account).map_err(|_| "invalid share publisher account")?,
        environment: marker.environment,
        write_mode: marker.write_mode,
        signer,
        content_key: Zeroizing::new(keys.encryption_key()),
    })
}

pub(crate) fn read_marker(directory: &Directory) -> Result<Option<PublicationMarker>, String> {
    let Some(bytes) = directory.read(MARKER_FILE).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let marker: PublicationMarker =
        serde_json::from_slice(&bytes).map_err(|_| "invalid share publication marker")?;
    if marker.version != 1 {
        return Err("unsupported share publication marker version".into());
    }
    Ok(Some(marker))
}

impl PublicationIdentity {
    /// Read the existing owner's credentials in place and pin their issuer.
    pub(crate) fn client(&self, owner_home: &Path) -> Result<ByteBackupClient, String> {
        publication_client(owner_home, &self.account, self.environment)
    }

    fn into_source(self) -> PublicationSource {
        PublicationSource {
            id: self.id,
            publisher: self.publisher,
            account: self.account,
            content_key: self.content_key,
        }
    }
}

fn publication_client(
    owner_home: &Path,
    account: &AccountId,
    environment: ShareEnvironment,
) -> Result<ByteBackupClient, String> {
    let bytes = owned(owner_home)?
        .read(crate::cloud::CLOUD_CREDENTIALS_FILE)
        .map_err(|e| e.to_string())?
        .ok_or("share publisher has no saved Exemem credentials")?;
    let credentials = crate::cloud::SavedCredentials::parse(&bytes)
        .map_err(|_| "invalid publisher cloud credentials")?;
    let environment = match environment {
        ShareEnvironment::Dev => Environment::Dev,
        ShareEnvironment::Prod => Environment::Prod,
    };
    let endpoint = exemem_api_url_for(environment);
    if credentials.api_url != endpoint
        || credentials.api_key.is_empty()
        || credentials
            .connection
            .as_ref()
            .is_some_and(|c| &c.account_id != account)
    {
        return Err("saved credentials do not match the share publisher and environment".into());
    }
    ByteBackupClient::from_auth(AuthClient::new(
        endpoint.to_owned(),
        credentials.api_key.as_str().to_owned(),
    ))
    .map_err(|e| e.to_string())
}

/// Mint a short-lived invitation only after Backup V2 has accepted this publication.
pub async fn invite(
    home: &Path,
    owner_home: &Path,
    contact: &crate::share_invitation::ShareContact,
    lifetime_hours: u32,
) -> Result<Vec<u8>, String> {
    if !(1..=24 * 7).contains(&lifetime_hours) {
        return Err("share invitation lifetime must be between 1 and 168 hours".into());
    }
    let publication = load(home, owner_home)?;
    let client = publication.client(owner_home)?;
    let context = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: publication.account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|e| e.to_string())?;
    let stream = Stream {
        scope: context.scope,
        stream_id: publication.id,
    };
    let expires = chrono::Utc::now() + chrono::Duration::hours(i64::from(lifetime_hours));
    let permission = GuestReadPermission::sign(
        stream,
        publication.environment,
        &expires.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        publication.signer.public_key_bytes(),
        |bytes| publication.signer.sign(bytes).to_bytes(),
    )
    .map_err(|e| e.to_string())?;
    require_guest_head(&permission).await?;
    crate::share_invitation::seal_invitation(
        contact,
        &permission,
        &publication.content_key,
        &publication.signer,
    )
    .map_err(|e| e.to_string())
}

async fn require_guest_head(permission: &GuestReadPermission) -> Result<(), String> {
    // Exercise the recipient route before writing a file, including the service's
    // independent-head check and the guest enablement of the selected issuer.
    let read_permission =
        GuestReadPermission::parse(permission.authorization()).map_err(|e| e.to_string())?;
    let reader = ByteBackupReader::guest_recovery(read_permission).map_err(|e| e.to_string())?;
    match reader.guest_head().await {
        Ok(HeadReadResult::Ok { response }) if response.accepted.is_some() => Ok(()),
        Ok(_) => Err("publish an independent backup before issuing a share invitation".into()),
        Err(BackupTransportError::Protocol(e)) if e.field == "guest.independent_snapshot" => {
            Err("the current share backup has dependencies; publish a full snapshot".into())
        }
        Err(BackupTransportError::Protocol(e)) if e.field == "guest.disabled" => {
            Err("guest reads are disabled in this Exemem environment".into())
        }
        Err(e) => Err(e.to_string()),
    }
}

pub fn recipient(home: &Path) -> Result<crate::share_invitation::ShareRecipient, String> {
    let seed = read_seed(&owned(home)?)?;
    crate::share_invitation::ShareRecipient::from_identity_seed(&seed).map_err(|e| e.to_string())
}

fn owned(home: &Path) -> Result<Directory, String> {
    Directory::open(home).map_err(|e| e.to_string())
}

fn read_seed(directory: &Directory) -> Result<Zeroizing<[u8; 32]>, String> {
    let bytes = directory
        .read(crate::identity::IDENTITY_KEY_FILE)
        .map_err(|e| e.to_string())?
        .ok_or("share requires an initialized LastDB identity")?;
    let seed = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "invalid LastDB identity size")?;
    Ok(Zeroizing::new(seed))
}

pub(crate) fn owner_seed(home: &Path) -> Result<Zeroizing<[u8; 32]>, String> {
    let directory = owned(home)?;
    if directory
        .read(MARKER_FILE)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("a publication database cannot sponsor another share".into());
    }
    read_seed(&directory)
}

pub(crate) fn signer(seed: &[u8; 32]) -> Result<Ed25519KeyPair, String> {
    Ed25519KeyPair::from_secret_key(seed).map_err(|_| "invalid share publisher key".into())
}

fn publisher(signer: &Ed25519KeyPair) -> Result<Digest, String> {
    Digest::try_from(fold_db::hex::hex_lower(signer.public_key_bytes()))
        .map_err(|_| "invalid share publisher public key".into())
}

fn publication_seed(owner: &[u8; 32], id: &Id) -> Result<Zeroizing<[u8; 32]>, String> {
    let mut seed = Zeroizing::new([0; 32]);
    Hkdf::<Sha256>::new(Some(b"lastdb:share:publication-home:v1"), owner)
        .expand(id.as_str().as_bytes(), seed.as_mut())
        .map_err(|_| "cannot derive share database identity")?;
    Ok(seed)
}

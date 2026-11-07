//! Exact object upload and conditional publication for a registered shared device.

use crate::backup_transport::{BackupTransport, StoredObject, TemporaryGrant};
use crate::shared_authority::device_headers;
use crate::AuthClient;
use cloud_backup_protocol::shared::objects::{
    ObjectAuthorityRequest, SharedObjectPrepare, SharedObjectRequest, SharedObjectRole,
    SharedObjectStatus, SharedUploadGrant, SnapshotHeadRequest, SnapshotInventoryPage,
    SnapshotLogEndsReply, SnapshotLogEndsRequest, INVENTORY_PAGE_ENTRIES,
};
use cloud_backup_protocol::shared::{SealedObject, SnapshotManifest, SnapshotReceipt, StreamCover};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{limits, AccountId, DecimalU64, ErrorCode, Id, ProtocolError};
use serde::de::DeserializeOwned;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::backup_transport::BackupTransportError;

mod qualification;

type Result<T> = std::result::Result<T, BackupTransportError>;
const RETRY_DELAYS_MS: [u64; 6] = [25, 50, 100, 200, 400, 800];

/// Result of submitting one immutable snapshot manifest.
pub enum SnapshotPublication {
    Accepted(SnapshotReceipt),
    /// The authoritative predecessor or cover check refused this input.
    Refused,
}

/// Exact shared control transport scoped to one registered account device.
pub struct SharedObjectClient {
    transport: BackupTransport,
    account: AccountId,
    device: Id,
}

impl SharedObjectClient {
    pub fn from_auth(auth: AuthClient, account: AccountId, device: Id) -> Result<Self> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
            account,
            device,
        })
    }

    /// Reuse prior verified bytes or send a bounded, create-once PUT and
    /// reconcile it through the service's exact stored-byte verification.
    pub async fn upload_exact(
        &self,
        request: &SharedObjectRequest,
        source: Box<dyn Read + Send>,
        deadline_ms: u64,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<SealedObject> {
        let prepare = self
            .post_transfer(ObjectAuthorityRequest::Prepare(request), deadline_ms, sign)
            .await?;
        let grant = match prepare {
            SharedObjectPrepare::Verified { object } => return exact(request, object),
            SharedObjectPrepare::Upload { grant } => grant,
            SharedObjectPrepare::Complete { object } => {
                exact(request, object)?;
                if destination(request) != Some(Destination::B2) {
                    return Err(BackupTransportError::InvalidResponse(
                        "shared upload settlement",
                    ));
                }
                return self.complete(request, deadline_ms, sign).await;
            }
        };
        match self.put_grant(request, source, deadline_ms, grant).await {
            Ok(()) => {}
            Err(BackupTransportError::Http(404))
                if destination(request) == Some(Destination::B2) => {}
            Err(error) => return Err(error),
        }
        self.complete(request, deadline_ms, sign).await
    }

    async fn put_grant(
        &self,
        request: &SharedObjectRequest,
        source: Box<dyn Read + Send>,
        deadline_ms: u64,
        grant: SharedUploadGrant,
    ) -> Result<()> {
        grant.validate_for(request, DecimalU64::new(now_ms()?))?;
        let object = StoredObject {
            sha256: &request.object.sha256,
            bytes: request.object.bytes.get(),
        };
        let grant = TemporaryGrant {
            url: grant.url.as_str(),
            expires_ms: grant.expires_at_ms.get().min(deadline_ms),
        };
        match destination(request) {
            Some(Destination::B2) => {
                require_provider(grant.url, Destination::B2)?;
                self.transport.upload_part(object, grant, source).await?;
            }
            destination => {
                if let Some(destination) = destination {
                    require_provider(grant.url, destination)?;
                }
                self.transport.upload(object, grant, source).await?;
            }
        }
        Ok(())
    }

    async fn complete(
        &self,
        request: &SharedObjectRequest,
        deadline_ms: u64,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<SealedObject> {
        let mut previous = DecimalU64::new(0);
        let mut delays_ms = RETRY_DELAYS_MS.into_iter();
        loop {
            let status = self
                .post_transfer(ObjectAuthorityRequest::Complete(request), deadline_ms, sign)
                .await?;
            match status {
                SharedObjectStatus::Verified { object } => return exact(request, object),
                SharedObjectStatus::Pending { verified_through } => {
                    if verified_through >= request.object.bytes || verified_through < previous {
                        return Err(BackupTransportError::Integrity(
                            "shared object verification",
                        ));
                    }
                    if verified_through == previous {
                        let delay_ms = delays_ms.next().ok_or_else(|| ProtocolError {
                            code: ErrorCode::ProviderPending,
                            field: "shared.object.provider".into(),
                        })?;
                        if now_ms()?.saturating_add(delay_ms) >= deadline_ms {
                            return Err(BackupTransportError::Timeout);
                        }
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    } else {
                        previous = verified_through;
                        delays_ms = RETRY_DELAYS_MS.into_iter();
                    }
                }
            }
        }
    }

    pub async fn snapshot_head(
        &self,
        request: &SnapshotHeadRequest,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<Option<SnapshotReceipt>> {
        let head: Option<SnapshotReceipt> = self
            .post(ObjectAuthorityRequest::Head(request), None, sign)
            .await?;
        if head
            .as_ref()
            .is_some_and(|head| head.database_id != request.database_id)
        {
            return Err(BackupTransportError::Integrity("shared snapshot head"));
        }
        Ok(head)
    }

    /// Obtain all exact commitments for a local checkpoint, including before
    /// the first snapshot exists. Missing accepted ends fail the whole lookup.
    pub async fn snapshot_log_ends(
        &self,
        request: &SnapshotLogEndsRequest,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<Vec<StreamCover>> {
        let reply: SnapshotLogEndsReply = self
            .post(ObjectAuthorityRequest::LogEnds(request), None, sign)
            .await?;
        reply
            .validate_for(request)
            .map_err(|_| BackupTransportError::Integrity("shared snapshot log ends"))?;
        Ok(reply.covers)
    }

    /// Register the immutable, exact inventory before submitting publication.
    pub async fn stage_snapshot_inventory(
        &self,
        manifest: &SnapshotManifest,
        inventory: &[SealedObject],
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<()> {
        manifest.validate()?;
        manifest.validate_inventory(inventory)?;
        for (index, objects) in inventory.chunks(INVENTORY_PAGE_ENTRIES).enumerate() {
            let page = SnapshotInventoryPage {
                database_id: manifest.database_id.clone(),
                snapshot_id: manifest.snapshot_id.clone(),
                page_index: u16::try_from(index)
                    .map_err(|_| BackupTransportError::Configuration("shared inventory page"))?,
                objects: objects.to_vec(),
            };
            self.post::<()>(ObjectAuthorityRequest::InventoryPage(&page), None, sign)
                .await?;
        }
        Ok(())
    }

    /// Submit one exact manifest. An uncertain result retries this operation
    /// without re-registering inventory already held by the service.
    pub async fn publish_snapshot(
        &self,
        manifest: &SnapshotManifest,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<SnapshotPublication> {
        let response: Result<SnapshotReceipt> = self
            .post(ObjectAuthorityRequest::Publish(manifest), None, sign)
            .await;
        match response {
            Ok(receipt) if receipt == manifest.receipt()? => {
                Ok(SnapshotPublication::Accepted(receipt))
            }
            Ok(_) => Err(BackupTransportError::Integrity("shared snapshot receipt")),
            Err(BackupTransportError::Protocol(error))
                if error.code == ErrorCode::PredecessorConflict =>
            {
                Ok(SnapshotPublication::Refused)
            }
            Err(error) => Err(error),
        }
    }

    /// Prepare and complete reconcile the same exact object on retry. The
    /// provider upload consumes its source once and stays outside this loop.
    async fn post_transfer<R: DeserializeOwned>(
        &self,
        request: ObjectAuthorityRequest<'_>,
        deadline_ms: u64,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<R> {
        let mut delays_ms = RETRY_DELAYS_MS.into_iter();
        loop {
            match self.post(request, Some(deadline_ms), sign).await {
                Err(BackupTransportError::Protocol(error))
                    if matches!(
                        error.code,
                        ErrorCode::RetryLater | ErrorCode::OutcomeUnknown
                    ) =>
                {
                    let Some(delay_ms) = delays_ms.next() else {
                        return Err(error.into());
                    };
                    if now_ms()?.saturating_add(delay_ms) >= deadline_ms {
                        return Err(BackupTransportError::Timeout);
                    }
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                result => return result,
            }
        }
    }

    async fn post<R: DeserializeOwned>(
        &self,
        request: ObjectAuthorityRequest<'_>,
        deadline_ms: Option<u64>,
        sign: &(impl Fn(&[u8]) -> Result<[u8; 64]> + Sync),
    ) -> Result<R> {
        let now = now_ms()?;
        let expires = DecimalU64::new(now.saturating_add(30_000));
        let digest = request.authority_digest(&self.account, &self.device, expires)?;
        let proof = sign(digest.as_str().as_bytes())?;
        let headers = device_headers(&self.device, expires, proof)?;
        let action = self.transport.post_bounded_with_headers(
            self.transport
                .api_endpoint(request.path().trim_start_matches('/')),
            &request,
            limits::REQUEST_JSON_MAX_BYTES,
            headers,
        );
        if let Some(deadline) = deadline_ms {
            let remaining = deadline
                .checked_sub(now)
                .filter(|millis| *millis > 0)
                .ok_or(BackupTransportError::Timeout)?;
            tokio::time::timeout(Duration::from_millis(remaining), action)
                .await
                .map_err(|_| BackupTransportError::Timeout)?
        } else {
            action.await
        }
    }
}

fn destination(request: &SharedObjectRequest) -> Option<Destination> {
    match &request.role {
        SharedObjectRole::NativeSnapshot { destination, .. } => Some(*destination),
        _ => None,
    }
}

fn require_provider(value: &str, destination: Destination) -> Result<()> {
    let invalid = BackupTransportError::InvalidResponse("shared upload provider");
    let url = reqwest::Url::parse(value).map_err(|_| invalid)?;
    let host = url.host_str().unwrap_or("");
    let valid = match destination {
        Destination::R2 => host
            .strip_suffix(".r2.cloudflarestorage.com")
            .is_some_and(|account| {
                account.len() == 32
                    && account
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            }),
        Destination::B2 => host
            .strip_prefix("s3.")
            .and_then(|host| host.strip_suffix(".backblazeb2.com"))
            .is_some_and(|region| {
                (1..=32).contains(&region.len())
                    && region.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            }),
    };
    if !valid || url.scheme() != "https" || url.port_or_known_default() != Some(443) {
        return Err(BackupTransportError::InvalidResponse(
            "shared upload provider",
        ));
    }
    Ok(())
}

fn exact(request: &SharedObjectRequest, object: SealedObject) -> Result<SealedObject> {
    if request.object != object {
        return Err(BackupTransportError::Integrity("shared object receipt"));
    }
    Ok(object)
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BackupTransportError::Configuration("clock"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| BackupTransportError::Configuration("clock"))
}

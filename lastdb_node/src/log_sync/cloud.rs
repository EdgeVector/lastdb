use std::sync::Arc;

use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use mutation_log::{ListPage, LogError, PrincipalId, ScopedHead, Writer};
use serde::{Deserialize, Serialize};

use crate::host::Host;

use super::http::{self, Endpoint, Proof};
use super::{account, paths, wire, SyncOutcome, SyncState};

#[derive(Serialize)]
struct WriterTipRequest<'a> {
    database_id: &'a str,
    device_id: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriterTipResponse {
    last_writer_seq: DecimalU64,
}

#[derive(Clone)]
pub(crate) struct CloudLog {
    host: Option<Arc<Host>>,
    endpoint: Arc<Endpoint>,
    keypair: Arc<Ed25519KeyPair>,
    pub(super) scope: CloudScope,
    database_id: Id,
    principal: PrincipalId,
    device: mutation_log::DeviceId,
    pub(super) member_revision: u64,
    environment_label: &'static str,
}

/// Inputs of [`CloudLog::open`]. Publishers and receivers use this one constructor.
pub(super) struct LogOpen<'a> {
    pub host: &'a Arc<Host>,
    pub endpoint: Endpoint,
    pub scope: CloudScope,
    pub database_id: Id,
    pub device: &'a str,
    pub member_revision: u64,
    pub environment_label: &'static str,
}

impl CloudLog {
    pub(crate) async fn connect(
        host: &Arc<Host>,
        database_id: &str,
        device: &str,
        member_revision: u64,
    ) -> Result<Self, String> {
        let database_id =
            Id::try_from(database_id.to_owned()).map_err(|_| "database id is not a UUID")?;
        let (scope, endpoint, environment_label) = account::publisher_scope(host).await?;
        Self::open(LogOpen {
            host,
            endpoint,
            scope,
            database_id,
            device,
            member_revision,
            environment_label,
        })
    }

    pub(super) fn open(parts: LogOpen<'_>) -> Result<Self, String> {
        let keypair = Arc::clone(&parts.host.keypair);
        let mut log = Self::open_offline(
            parts.endpoint,
            parts.scope,
            parts.database_id,
            parts.device,
            keypair,
            parts.environment_label,
        )?;
        log.member_revision = parts.member_revision;
        log.host = Some(Arc::clone(parts.host));
        Ok(log)
    }

    pub(super) fn open_offline(
        endpoint: Endpoint,
        scope: CloudScope,
        database_id: Id,
        device: &str,
        keypair: Arc<Ed25519KeyPair>,
        environment_label: &'static str,
    ) -> Result<Self, String> {
        let principal = PrincipalId::new(&hex::encode(keypair.public_key_bytes()))
            .map_err(|_| "node key is not a log label")?;
        Ok(Self {
            host: None,
            endpoint: Arc::new(endpoint),
            keypair,
            scope,
            database_id,
            principal,
            device: mutation_log::DeviceId::new(device)
                .map_err(|_| "device id is not a log label")?,
            member_revision: 0,
            environment_label,
        })
    }

    pub(super) fn for_staging(&self, host: &Arc<Host>) -> Self {
        let mut staged = self.clone();
        staged.host = Some(Arc::clone(host));
        staged
    }

    pub(super) fn host(&self) -> Result<&Arc<Host>, LogError> {
        self.host.as_ref().ok_or(LogError::UnexpectedOutcome)
    }

    pub(super) fn database_id(&self) -> &str {
        self.database_id.as_str()
    }

    pub(crate) fn environment(&self) -> &'static str {
        self.environment_label
    }

    pub(crate) fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub(crate) fn device(&self) -> &mutation_log::DeviceId {
        &self.device
    }

    pub(crate) async fn post_status(
        &self,
        path: &str,
        body: Vec<u8>,
        proof: Proof,
    ) -> Result<(u16, Vec<u8>), ()> {
        self.post_status_limit(path, body, proof, 65_536).await
    }

    pub(super) async fn post_status_limit(
        &self,
        path: &str,
        body: Vec<u8>,
        proof: Proof,
        limit: u64,
    ) -> Result<(u16, Vec<u8>), ()> {
        self.endpoint
            .post_exchange(self.signed(path, &body, proof), limit)
            .await
            .map_err(|_| ())
    }

    pub(crate) fn reader(&self) -> Writer {
        Writer {
            key: self.principal.clone(),
            device: self.device.clone(),
        }
    }

    pub(crate) async fn drive_once(&self, state: SyncState) -> Result<SyncOutcome, LogError> {
        let page = self.list_async(state.cursor).await?;
        Ok(SyncOutcome {
            entries: page.entries,
            cursor: page.next,
        })
    }

    pub(super) async fn head_async(&self) -> Result<Option<ScopedHead>, LogError> {
        let body = wire::database_body(self.database_id.as_str())?;
        wire::scoped_head(&self.post(paths::HEAD, body).await?)
    }

    pub(super) async fn list_async(&self, after: u64) -> Result<ListPage, LogError> {
        let database = self.database_id();
        let body = wire::list_body(database, after, mutation_log::LIST_PAGE_LIMIT, false)?;
        let reply = self.post(paths::LIST, body).await?;
        wire::list_page(database, &reply)
    }

    pub(super) async fn writer_tip_async(&self) -> Result<u64, LogError> {
        let body = canonical_bytes(&WriterTipRequest {
            database_id: self.database_id.as_str(),
            device_id: self.device.as_str(),
        })
        .map_err(|_| LogError::InvalidEntry)?;
        let response: WriterTipResponse =
            serde_json::from_slice(&self.post(paths::HEAD, body).await?)
                .map_err(|_| LogError::UnexpectedOutcome)?;
        Ok(response.last_writer_seq.get())
    }

    /// Ask for an exact snapshot only when this caller can install one.
    pub(super) async fn list_with_native_reset(
        &self,
        after: u64,
    ) -> Result<Result<ListPage, wire::NativeCut>, LogError> {
        let limit = mutation_log::LIST_PAGE_LIMIT;
        let database = self.database_id();
        let body = wire::list_body(database, after, limit, true)?;
        let (status, payload) = self
            .post_status_limit(paths::LIST, body, Proof::Data, http::JSON_LIMIT)
            .await
            .map_err(|_| LogError::UnexpectedOutcome)?;
        if status == 400 && wire::unsupported_reset(&payload) {
            return self.list_async(after).await.map(Ok);
        }
        if status == 409 {
            if let Some(cut) = wire::native_cut(&payload, after) {
                return Ok(Err(cut));
            }
        }
        if status == 200 {
            return wire::list_page(database, &payload).map(Ok);
        }
        Err(wire::service_error(&payload).unwrap_or(LogError::UnexpectedOutcome))
    }

    pub(super) async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        self.endpoint
            .post_json(self.signed(path, &body, Proof::Data))
            .await
    }

    fn signed<'a>(&'a self, path: &'a str, body: &'a [u8], proof: Proof) -> http::LogRequest<'a> {
        http::LogRequest {
            keypair: &self.keypair,
            scope: &self.scope,
            database_id: &self.database_id,
            path,
            body,
            member_revision: self.member_revision,
            proof,
        }
    }
}

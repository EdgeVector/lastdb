use std::sync::{mpsc, Arc};

use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use mutation_log::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, MutationLog, PrincipalId, Recovery,
    ScopedHead, Snapshot, SnapshotRef, Writer,
};
use mutation_log_sync::{Outcome, SyncEvent, SyncState};
use serde::{Deserialize, Serialize};

use crate::host::Host;

use super::account;
use super::http::{self, Endpoint, Proof};
use super::paths;
use super::wire;

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
    handle: tokio::runtime::Handle,
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
            handle: tokio::runtime::Handle::current(),
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

    pub(crate) async fn drive_once(
        &self,
        state: SyncState,
        event: SyncEvent,
    ) -> Result<(SyncState, Outcome), mutation_log_sync::SyncError> {
        let mut log = self.clone();
        match tokio::task::spawn_blocking(move || {
            mutation_log_sync::drive(&state, &mut log, event, &mutation_log::classify_shareable)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(LogError::UnexpectedOutcome.into()),
        }
    }

    pub(super) async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        self.endpoint
            .post_json(self.signed(path, &body, Proof::Data))
            .await
    }

    pub(super) fn wait<T>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<T, LogError>
    where
        T: Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(1);
        self.handle.spawn(async move {
            let _ = tx.send(future.await);
        });
        rx.recv().map_err(|_| LogError::UnexpectedOutcome)
    }

    fn call(&self, path: &'static str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        let endpoint = Arc::clone(&self.endpoint);
        let keypair = Arc::clone(&self.keypair);
        let scope = self.scope.clone();
        let database_id = self.database_id.clone();
        let revision = self.member_revision;
        self.wait(async move {
            endpoint
                .post_json(http::LogRequest {
                    keypair: &keypair,
                    scope: &scope,
                    database_id: &database_id,
                    path,
                    body: &body,
                    member_revision: revision,
                    proof: Proof::Data,
                })
                .await
        })?
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

impl MutationLog for CloudLog {
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError> {
        self.same_reader(&batch.writer.key)?;
        self.same_device(&batch.writer.device)?;
        let body = wire::append_request(&batch)?;
        wire::append_outcome(&self.call(paths::APPEND, body)?, &batch)
    }

    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError> {
        self.same_reader(reader)?;
        let body = wire::database_body(self.database_id.as_str())?;
        wire::scoped_head(&self.call(paths::HEAD, body)?)
    }

    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        _now_ms: u64,
    ) -> Result<ListPage, LogError> {
        self.same_reader(reader)?;
        let body = wire::list_body(self.database_id.as_str(), after, limit)?;
        wire::list_page(self.database_id.as_str(), &self.call(paths::LIST, body)?)
    }

    fn ack(&mut self, reader: &Writer, through: u64) -> Result<u64, LogError> {
        self.same_reader(&reader.key)?;
        self.same_device(&reader.device)?;
        // The signed cloud log has no reader progress to update.
        Ok(through)
    }

    fn register_reader(&mut self, _reader: &Writer) -> Result<(), LogError> {
        Ok(())
    }

    fn recover(
        &self,
        reader: &Writer,
        _local_cursor: u64,
        _now_ms: u64,
    ) -> Result<Recovery, LogError> {
        self.same_reader(&reader.key)?;
        self.same_device(&reader.device)?;
        Err(LogError::SnapshotRequired)
    }

    fn grant(
        &mut self,
        _caller: &PrincipalId,
        _principal: PrincipalId,
        _capability: Capability,
    ) -> Result<(), LogError> {
        Err(LogError::UnexpectedOutcome)
    }

    fn revoke(
        &mut self,
        _caller: &PrincipalId,
        _principal: &PrincipalId,
        _capability: &Capability,
    ) -> Result<(), LogError> {
        Err(LogError::UnexpectedOutcome)
    }

    fn snapshot(
        &mut self,
        caller: &PrincipalId,
        _at_ms: u64,
        _snapshot_ref: SnapshotRef,
    ) -> Result<Snapshot, LogError> {
        self.same_reader(caller)?;
        Err(LogError::UnexpectedOutcome)
    }

    fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError> {
        if reader != &writer.key {
            return Err(LogError::Unauthorized);
        }
        self.same_device(&writer.device)?;
        let body = canonical_bytes(&WriterTipRequest {
            database_id: self.database_id.as_str(),
            device_id: writer.device.as_str(),
        })
        .map_err(|_| LogError::InvalidEntry)?;
        let response: WriterTipResponse = serde_json::from_slice(&self.call(paths::HEAD, body)?)
            .map_err(|_| LogError::UnexpectedOutcome)?;
        Ok(response.last_writer_seq.get())
    }
}

impl CloudLog {
    fn same_reader(&self, reader: &PrincipalId) -> Result<(), LogError> {
        if reader == &self.principal {
            Ok(())
        } else {
            Err(LogError::Unauthorized)
        }
    }

    fn same_device(&self, device: &mutation_log::DeviceId) -> Result<(), LogError> {
        if device == &self.device {
            Ok(())
        } else {
            Err(LogError::Unauthorized)
        }
    }
}

use std::sync::{mpsc, Arc};

use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{canonical_bytes, CloudScope, Id};
use fold_db::security::Ed25519KeyPair;
use mutation_log::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, MutationLog, PrincipalId, Recovery,
    ScopedHead, Snapshot, SnapshotRef, Writer,
};
use mutation_log_sync::{Outcome, SyncEvent, SyncState};

use crate::host::Host;
use crate::node_read::CanonicalPost;

use super::account::{self, account_of, node_key};
use super::cutover;
use super::http::{self, Endpoint, Proof};
use super::paths;
use super::registry::Database;
use super::status;
use super::wire;

#[derive(Clone)]
pub(crate) struct CloudLog {
    handle: tokio::runtime::Handle,
    host: Arc<Host>,
    endpoint: Arc<Endpoint>,
    keypair: Arc<Ed25519KeyPair>,
    pub(super) scope: CloudScope,
    database_id: Id,
    principal: PrincipalId,
    device: mutation_log::DeviceId,
    member_revision: u64,
    named: bool,
    environment_label: &'static str,
}

impl CloudLog {
    pub(crate) async fn connect(
        host: &Arc<Host>,
        database_id: &str,
        device: &str,
        member_revision: u64,
        named: bool,
    ) -> Result<Self, String> {
        let selected = http::selected_service(&host.home)?;
        let wire_id =
            Id::try_from(database_id.to_owned()).map_err(|_| "database id is not a UUID")?;
        let account = account_of(host)?;
        let endpoint =
            Endpoint::open(&selected.api_url, selected.label)?.load_owner_key(&host.home);
        let backup = crate::byte_cloud_backup::client_from_home(&host.home)?;
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
        Ok(Self {
            handle: tokio::runtime::Handle::current(),
            host: Arc::clone(host),
            endpoint: Arc::new(endpoint),
            keypair: Arc::clone(&host.keypair),
            scope: context.scope,
            database_id: wire_id,
            principal: PrincipalId::new(&node_key(host))
                .map_err(|_| "node key is not a log label")?,
            device: mutation_log::DeviceId::new(device)
                .map_err(|_| "device id is not a log label")?,
            member_revision,
            named,
            environment_label: selected.label,
        })
    }

    pub(super) fn named(&self) -> bool {
        self.named
    }

    pub(super) fn host(&self) -> &Host {
        &self.host
    }

    pub(super) fn database_id(&self) -> &str {
        self.database_id.as_str()
    }

    pub(crate) fn member_revision(&self) -> u64 {
        self.member_revision
    }

    pub(crate) fn environment(&self) -> &'static str {
        self.environment_label
    }

    pub(crate) async fn service_view(&self, database: &Database) -> cutover::ServiceView {
        let Ok(body) = wire::database_body(self.database_id.as_str()) else {
            return cutover::ServiceView::Unavailable;
        };
        match self
            .endpoint
            .post_exchange(
                self.signed(
                    paths::ACCESS,
                    &body,
                    Proof::Canonical(CanonicalPost::Access),
                ),
                65_536,
            )
            .await
        {
            Ok((status, bytes)) => {
                status::classify(status, &bytes, database, self.environment_label)
            }
            Err(_) => cutover::ServiceView::Unavailable,
        }
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
        self.endpoint
            .post_exchange(self.signed(path, &body, proof), 65_536)
            .await
            .map_err(|_| ())
    }

    pub(super) async fn post_named(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        self.post(path, body).await
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

    pub(crate) async fn resumed(
        &self,
        state: SyncState,
    ) -> Result<SyncState, mutation_log_sync::SyncError> {
        let log = self.clone();
        match tokio::task::spawn_blocking(move || SyncState::resume(&state, &log)).await {
            Ok(result) => result,
            Err(_) => Err(LogError::UnexpectedOutcome.into()),
        }
    }

    pub(crate) async fn ack_through(&self, reader: Writer, through: u64) -> Result<u64, LogError> {
        let mut log = self.clone();
        match tokio::task::spawn_blocking(move || log.ack(&reader, through)).await {
            Ok(result) => result,
            Err(_) => Err(LogError::UnexpectedOutcome),
        }
    }

    pub(crate) async fn snapshot_bytes(&self, snapshot_ref: &str) -> Result<Vec<u8>, LogError> {
        let body = canonical_bytes(&wire::SnapshotBytesRequest {
            database_id: self.database_id.as_str(),
            snapshot_ref,
        })
        .map_err(|_| LogError::InvalidEntry)?;
        self.endpoint
            .post_bytes(self.signed(paths::SNAPSHOT_BYTES, &body, self.data_proof()))
            .await
    }

    async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        self.endpoint
            .post_json(self.signed(path, &body, self.data_proof()))
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
        let proof = self.data_proof();
        self.wait(async move {
            endpoint
                .post_json(http::LogRequest {
                    keypair: &keypair,
                    scope: &scope,
                    database_id: &database_id,
                    path,
                    body: &body,
                    member_revision: revision,
                    proof,
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

    fn data_proof(&self) -> Proof {
        if self.named {
            Proof::Named
        } else {
            Proof::Legacy
        }
    }
}

impl MutationLog for CloudLog {
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError> {
        self.same_reader(&batch.writer.key)?;
        self.same_device(&batch.writer.device)?;
        let body = wire::append_request(&batch)?;
        wire::append_outcome(&self.call(paths::APPEND, body)?)
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
        let body = wire::list_body(
            self.database_id.as_str(),
            self.device.as_str(),
            after,
            limit,
        )?;
        wire::list_page(self.database_id.as_str(), &self.call(paths::LIST, body)?)
    }

    fn ack(&mut self, reader: &Writer, through: u64) -> Result<u64, LogError> {
        self.same_reader(&reader.key)?;
        self.same_device(&reader.device)?;
        let body = wire::ack_body(self.database_id.as_str(), reader.device.as_str(), through)?;
        wire::acknowledged(&self.call(paths::ACK, body)?)
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
        super::restore::recover(self, reader)
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
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<(), LogError> {
        if self.named {
            return Err(LogError::UnexpectedOutcome);
        }
        self.same_reader(caller)?;
        let body = account::json_body(&wire::RevokeRequest {
            database_id: self.database_id.as_str(),
            principal: principal.as_str(),
            capability: wire::capability_name(capability),
        })?;
        self.call(paths::REVOKE, body).map(|_| ())
    }

    fn snapshot(
        &mut self,
        caller: &PrincipalId,
        _at_ms: u64,
        _snapshot_ref: SnapshotRef,
    ) -> Result<Snapshot, LogError> {
        self.same_reader(caller)?;
        let body = wire::database_body(self.database_id.as_str())?;
        wire::snapshot(
            self.database_id.as_str(),
            &self.call(paths::SNAPSHOT, body)?,
        )
    }

    fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError> {
        if reader != &writer.key {
            return Err(LogError::Unauthorized);
        }
        self.same_device(&writer.device)?;
        match self.head(reader)? {
            None => Ok(0),
            Some(head) => match head.stamp {
                Some(stamp)
                    if stamp.writer.key == writer.key && stamp.writer.device == writer.device =>
                {
                    Ok(stamp.sequence)
                }
                _ => Err(LogError::UnexpectedOutcome),
            },
        }
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

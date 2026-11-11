//! Cloud [`MutationLog`]. HTTP runs on the runtime; `drive` runs on a blocking thread.

use std::sync::{mpsc, Arc};

use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{canonical_bytes, AccountId, CloudScope, DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use mutation_log::{
    AppendBatch, AppendOutcome, Capability, ListPage, LogError, MutationLog, PrincipalId, Recovery,
    ScopedHead, Snapshot, SnapshotRef, Writer,
};
use mutation_log_sync::{Outcome, SyncEvent, SyncState};
use serde::Serialize;

use crate::host::Host;
use crate::profile::endpoints::{exemem_api_url_for, Environment};

use super::http::Endpoint;
use super::paths;
use super::wire;

#[derive(Clone)]
pub(crate) struct CloudLog {
    handle: tokio::runtime::Handle,
    endpoint: Arc<Endpoint>,
    keypair: Arc<Ed25519KeyPair>,
    scope: CloudScope,
    database_id: Id,
    principal: PrincipalId,
    device: mutation_log::DeviceId,
    member_revision: u64,
}

impl CloudLog {
    pub(crate) async fn connect(
        host: &Host,
        database_id: &str,
        device: &str,
        member_revision: u64,
    ) -> Result<Self, String> {
        assert_dev(&host.home)?;
        let wire_id =
            Id::try_from(database_id.to_owned()).map_err(|_| "database id is not a UUID")?;
        let account = account_of(host)?;
        let endpoint = Endpoint::open(&api_url(&host.home)?)?;
        let backup = crate::byte_cloud_backup::credentials::client_from_home(&host.home)?;
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
            endpoint: Arc::new(endpoint),
            keypair: Arc::clone(&host.keypair),
            scope: context.scope,
            database_id: wire_id,
            principal: PrincipalId::new(&node_key(host))
                .map_err(|_| "node key is not a log label")?,
            device: mutation_log::DeviceId::new(device)
                .map_err(|_| "device id is not a log label")?,
            member_revision,
        })
    }

    pub(crate) fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub(crate) fn device(&self) -> &mutation_log::DeviceId {
        &self.device
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

    pub(crate) async fn grant_owner(&self) -> Result<(), LogError> {
        let mut log = self.clone();
        let caller = self.principal.clone();
        match tokio::task::spawn_blocking(move || {
            log.grant(&caller, caller.clone(), Capability::Owner)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(LogError::UnexpectedOutcome),
        }
    }

    pub(crate) async fn reject_submission(
        &self,
        writer: &Writer,
        writer_seq: u64,
    ) -> Result<(), LogError> {
        let body = canonical_bytes(&wire::RejectRequest {
            database_id: self.database_id.as_str(),
            caller: self.principal.as_str(),
            node_public_key: writer.key.as_str(),
            device_id: writer.device.as_str(),
            writer_seq: DecimalU64::new(writer_seq),
        })
        .map_err(|_| LogError::InvalidEntry)?;
        self.post(paths::REJECT, body).await.map(|_| ())
    }

    pub(crate) async fn snapshot_bytes(&self, snapshot_ref: &str) -> Result<Vec<u8>, LogError> {
        let body = canonical_bytes(&wire::SnapshotBytesRequest {
            database_id: self.database_id.as_str(),
            snapshot_ref,
        })
        .map_err(|_| LogError::InvalidEntry)?;
        self.endpoint
            .post_bytes(
                &self.keypair,
                &self.scope,
                &self.database_id,
                paths::SNAPSHOT_BYTES,
                &body,
                self.member_revision,
            )
            .await
    }

    async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, LogError> {
        self.endpoint
            .post_json(
                &self.keypair,
                &self.scope,
                &self.database_id,
                path,
                &body,
                self.member_revision,
            )
            .await
    }

    fn wait<T>(
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
                .post_json(&keypair, &scope, &database_id, path, &body, revision)
                .await
        })?
    }
}

impl MutationLog for CloudLog {
    fn append(&mut self, batch: AppendBatch) -> Result<AppendOutcome, LogError> {
        let body = wire::append_request(&batch)?;
        wire::append_outcome(&self.call(paths::APPEND, body)?)
    }

    fn head(&self, reader: &PrincipalId) -> Result<Option<ScopedHead>, LogError> {
        let body = json_body(&wire::ListRequest {
            database_id: self.database_id.as_str(),
            reader: reader.as_str(),
            after: DecimalU64::new(0),
            limit: DecimalU64::new(0),
            now_ms: DecimalU64::new(0),
        })?;
        let bytes = self.call(paths::HEAD, body)?;
        let page = wire::list_page(&bytes).ok();
        Ok(page.map(|page| ScopedHead {
            cursor: page.next,
            stamp: page.entries.last().map(|entry| entry.entry.stamp()),
        }))
    }

    fn list(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ListPage, LogError> {
        let body = json_body(&wire::ListRequest {
            database_id: self.database_id.as_str(),
            reader: reader.as_str(),
            after: DecimalU64::new(after),
            limit: DecimalU64::new(limit as u64),
            now_ms: DecimalU64::new(now_ms),
        })?;
        wire::list_page(&self.call(paths::LIST, body)?)
    }

    fn ack(&mut self, reader: &Writer, through: u64) -> Result<u64, LogError> {
        let body = json_body(&wire::AckRequest {
            database_id: self.database_id.as_str(),
            node_public_key: reader.key.as_str(),
            device_id: reader.device.as_str(),
            through: DecimalU64::new(through),
        })?;
        let bytes = self.call(paths::ACK, body)?;
        let response: wire::TipResponse =
            serde_json::from_slice(&bytes).map_err(|_| LogError::UnexpectedOutcome)?;
        Ok(response.tip.get())
    }

    fn register_reader(&mut self, reader: &Writer) -> Result<(), LogError> {
        let body = cursor_body(self, reader, 0)?;
        self.call(paths::REGISTER_READER, body).map(|_| ())
    }

    fn recover(
        &self,
        reader: &Writer,
        local_cursor: u64,
        now_ms: u64,
    ) -> Result<Recovery, LogError> {
        let body = json_body(&wire::ListRequest {
            database_id: self.database_id.as_str(),
            reader: reader.key.as_str(),
            after: DecimalU64::new(local_cursor),
            limit: DecimalU64::new(mutation_log::LIST_PAGE_LIMIT as u64),
            now_ms: DecimalU64::new(now_ms),
        })?;
        wire::recovery(&self.call(paths::RECOVER, body)?)
    }

    fn grant(
        &mut self,
        caller: &PrincipalId,
        principal: PrincipalId,
        capability: Capability,
    ) -> Result<(), LogError> {
        let body = json_body(&wire::GrantRequest {
            database_id: self.database_id.as_str(),
            caller: caller.as_str(),
            principal: principal.as_str(),
            capability: wire::capability_name(&capability),
        })?;
        self.call(paths::GRANT, body).map(|_| ())
    }

    fn revoke(
        &mut self,
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<(), LogError> {
        let body = json_body(&wire::GrantRequest {
            database_id: self.database_id.as_str(),
            caller: caller.as_str(),
            principal: principal.as_str(),
            capability: wire::capability_name(capability),
        })?;
        self.call(paths::REVOKE, body).map(|_| ())
    }

    fn snapshot(
        &mut self,
        caller: &PrincipalId,
        at_ms: u64,
        snapshot_ref: SnapshotRef,
    ) -> Result<Snapshot, LogError> {
        let body = json_body(&wire::SnapshotRequest {
            database_id: self.database_id.as_str(),
            caller: caller.as_str(),
            at_ms: DecimalU64::new(at_ms),
            snapshot_ref: snapshot_ref.as_str(),
        })?;
        wire::snapshot(&self.call(paths::SNAPSHOT, body)?)
    }

    fn writer_tip(&self, reader: &PrincipalId, writer: &Writer) -> Result<u64, LogError> {
        let body = json_body(&wire::TipRequest {
            database_id: self.database_id.as_str(),
            reader: reader.as_str(),
            node_public_key: writer.key.as_str(),
            device_id: writer.device.as_str(),
        })?;
        let bytes = self.call(paths::WRITER_TIP, body)?;
        let response: wire::TipResponse =
            serde_json::from_slice(&bytes).map_err(|_| LogError::UnexpectedOutcome)?;
        Ok(response.tip.get())
    }
}

fn cursor_body(log: &CloudLog, reader: &Writer, through: u64) -> Result<Vec<u8>, LogError> {
    json_body(&wire::CursorRequest {
        database_id: log.database_id.as_str(),
        node_public_key: reader.key.as_str(),
        device_id: reader.device.as_str(),
        delivery_seq: DecimalU64::new(through),
    })
}

fn json_body<T: Serialize>(value: &T) -> Result<Vec<u8>, LogError> {
    canonical_bytes(value).map_err(|_| LogError::InvalidEntry)
}

fn node_key(host: &Host) -> String {
    hex::encode(host.keypair.public_key_bytes())
}

fn account_of(host: &Host) -> Result<AccountId, String> {
    let derived = crate::identity::user_hash_from_pubkey(&host.public_key())?;
    if derived != host.user_hash {
        return Err("node account id does not match the node key".into());
    }
    AccountId::try_from(derived).map_err(|_| "node account id is not a cloud account id".into())
}

fn api_url(home: &std::path::Path) -> Result<String, String> {
    let saved = crate::cloud::load_saved_credentials(home)?
        .ok_or("log sync needs saved cloud credentials")?;
    Ok(saved.api_url)
}

fn assert_dev(home: &std::path::Path) -> Result<(), String> {
    let saved = crate::cloud::load_saved_credentials(home)?
        .ok_or("log sync needs saved cloud credentials")?;
    if saved
        .connection
        .as_ref()
        .is_some_and(|connection| connection.environment != "dev")
    {
        return Err("log sync refuses a non-dev cloud connection".into());
    }
    let api = reqwest::Url::parse(&saved.api_url).map_err(|_| "cloud endpoint is not a URL")?;
    if api.path() != "/" && !api.path().is_empty() {
        return Err("log sync requires an origin cloud endpoint".into());
    }
    let prod = exemem_api_url_for(Environment::Prod);
    let prod = reqwest::Url::parse(prod).map_err(|_| "production endpoint is not configured")?;
    if api.scheme() == prod.scheme() && api.host() == prod.host() && api.port() == prod.port() {
        return Err("log sync refuses the production API".into());
    }
    Ok(())
}

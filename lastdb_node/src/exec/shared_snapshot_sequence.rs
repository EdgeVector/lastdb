//! Owner-triggered shared publication and bounded writer catch-up.

mod workspace;

use crate::byte_cloud_restore::{local, RecoveryKeys, RestoreError, Result};
use crate::cloud::{SavedCredentials, CLOUD_CREDENTIALS_FILE};
use crate::host::Host;
use crate::shared_snapshot::native::{
    expire, release, saved_cut, snapshot_uuid, NativeCheckpoint, PublishedFiles,
};
use crate::shared_snapshot::SignedSnapshotTransport;
use crate::shared_upload::{upload_next, ServiceObjects, UploadIdentity, UploadOutcome};
use cloud_backup_client::{
    shared_log::SharedLogClient, shared_objects::SharedObjectClient, AuthClient,
};
use cloud_backup_protocol::Id;
use fold_db::fold_db_core::MutationManager;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_pack::PackRuntime;
use folddb_profile::endpoints::{exemem_api_url_for, schema_service_url_for, Environment};
use futures::{StreamExt, TryStreamExt};
use schema_service_client::SchemaServiceClient;
use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use workspace::Workspace;
use zeroize::Zeroizing;

const DEV: Environment = Environment::Dev;
static ACTIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedSnapshotRequest {
    pub device_id: Id,
    pub device_key_file: PathBuf,
    pub workspace: PathBuf,
}

#[derive(Serialize)]
pub(crate) struct SharedSnapshotResult {
    snapshot_id: Id,
    writer_uploaded_through: u64,
    r2_files: usize,
    b2_files: usize,
}

pub(crate) async fn run_shared_snapshot(
    host: &Host,
    request: &SharedSnapshotRequest,
    budget: Duration,
) -> Result<SharedSnapshotResult> {
    crate::offline_home::refuse_primary(&host.home)
        .map_err(|_| RestoreError::Invalid("shared snapshot requires a dedicated DEV home"))?;
    let _active = ACTIVE
        .try_lock()
        .map_err(|_| RestoreError::Invalid("shared snapshot already running"))?;
    let runtime = host.db.db_ops().native_runtime();
    runtime
        .require_direct_shared_home()
        .map_err(|_| RestoreError::Invalid("snapshot requires a dedicated shared disk home"))?;
    let native = host.db.mutation_manager();
    let (stream, _) = native
        .portable_writer_progress()
        .await
        .map_err(native_error)?;
    let home = runtime.native_home_id();
    let keys = RecoveryKeys::from_seed(Zeroizing::new(host.keypair.secret_key_bytes()))?;
    let auth = dev_auth(&host.home)?;
    let device_signer = Arc::new(device_signer(&request.device_key_file)?);
    let transport = SignedSnapshotTransport::from_auth(
        auth.clone(),
        keys.account_id().clone(),
        request.device_id.clone(),
        Arc::clone(&device_signer),
    )?;
    let mut state = Workspace::open(&request.workspace, home, &request.device_id, &stream)?;
    let logs = SharedLogClient::from_auth(auth.clone()).map_err(|_| transport_error())?;
    let objects =
        SharedObjectClient::from_auth(auth, keys.account_id().clone(), request.device_id.clone())
            .map_err(|_| transport_error())?;
    let millis = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
    let deadline = fold_db::clock::unix_millis().saturating_add(millis);
    let upload = Upload {
        native,
        data: &host.data_dir,
        identity: UploadIdentity {
            account: keys.account_id(),
            device: &request.device_id,
            device_signer: &device_signer,
            content_key: keys.content_key(),
        },
        logs,
        objects: ServiceObjects {
            client: &objects,
            device_signer: &device_signer,
            deadline_ms: deadline,
        },
    };

    let operation = async {
        let files = publish_cut(&runtime, &state, &upload, &transport, &keys, deadline).await?;
        let (_, progress) = native
            .portable_writer_progress()
            .await
            .map_err(native_error)?;
        let tail = state.tail_through(progress.applied_through)?;
        let mut uploaded = progress.uploaded_through.unwrap_or_default();
        while uploaded < tail {
            uploaded = upload
                .next()
                .await?
                .ok_or(RestoreError::Invalid("shared writer tail is missing"))?;
        }
        Ok(SharedSnapshotResult {
            snapshot_id: state.snapshot_id().clone(),
            writer_uploaded_through: tail,
            r2_files: files.r2,
            b2_files: files.b2,
        })
    };
    match tokio::time::timeout(budget, operation).await {
        Ok(result) => result,
        Err(_) => {
            expire(&runtime, &state.checkpoint()?, state.snapshot_id()).await?;
            Err(RestoreError::Limit(
                "snapshot deadline reached. Retry with the same workspace",
            ))
        }
    }
}

async fn publish_cut(
    runtime: &Arc<PackRuntime>,
    state: &Workspace,
    upload: &Upload<'_>,
    transport: &SignedSnapshotTransport,
    keys: &RecoveryKeys,
    deadline: u64,
) -> Result<PublishedFiles> {
    let root = state.checkpoint()?;
    let id = state.snapshot_id();
    expire(runtime, &root, id).await?;
    let mut checkpoint = if let Some(cut) = saved_cut(&root)? {
        if &cut.snapshot_id != id || &cut.database_id != state.database_id() {
            return Err(RestoreError::Invalid(
                "saved snapshot differs from its workspace",
            ));
        }
        NativeCheckpoint::reopen(runtime, &root, cut, keys).await?
    } else {
        transport.current_snapshot(state.database_id()).await?;
        let definitions = definitions(upload.native).await?;
        let uuid = snapshot_uuid(id)?;
        let (source, capture) = loop {
            if let Some(frozen) = upload
                .native
                .try_freeze_portable_checkpoint(&definitions, uuid, keys.content_key())
                .await
                .map_err(native_error)?
            {
                break frozen;
            }
            if upload.next().await?.is_none() {
                return Err(RestoreError::Invalid(
                    "shared writer pending prefix is missing",
                ));
            }
        };
        match NativeCheckpoint::prepare(
            runtime,
            source,
            capture,
            &root,
            id.clone(),
            deadline,
            transport,
        )
        .await
        {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                // An atomic rename followed by a directory sync error can
                // leave a visible cut. Keep the corresponding hold for retry.
                if saved_cut(&root)?.is_none() {
                    release(runtime, id).await?;
                }
                return Err(error);
            }
        }
    };
    checkpoint.upload(transport, keys).await
}

struct Upload<'a> {
    native: &'a MutationManager,
    data: &'a Path,
    identity: UploadIdentity<'a>,
    logs: SharedLogClient,
    objects: ServiceObjects<'a>,
}

impl Upload<'_> {
    async fn next(&self) -> Result<Option<u64>> {
        match upload_next(
            self.data,
            self.native,
            &self.identity,
            &self.logs,
            &self.objects,
        )
        .await
        .map_err(|_| RestoreError::Source("shared writer upload; retry with the same workspace"))?
        {
            UploadOutcome::Idle => Ok(None),
            UploadOutcome::Accepted(head) => Ok(Some(head.sequence.get())),
            UploadOutcome::Reconcile(_) => Err(RestoreError::Source(
                "shared writer requires reconciliation",
            )),
        }
    }
}

async fn definitions(
    native: &MutationManager,
) -> Result<BTreeMap<String, PortableSchemaDefinition>> {
    let bindings = native
        .portable_writer_schemas()
        .await
        .map_err(native_error)?;
    let service = SchemaServiceClient::new(schema_service_url_for(DEV));
    futures::stream::iter(bindings)
        .map(|(id, alias)| {
            let service = &service;
            async move {
                let id = PortableSchemaId::try_from(id)
                    .map_err(|_| RestoreError::Invalid("shared schema identity"))?;
                let definition = service
                    .get_portable_schema(&id)
                    .await
                    .map_err(|_| RestoreError::Source("bound shared schema definition"))?;
                Ok((alias, definition))
            }
        })
        .buffered(4)
        .try_collect()
        .await
}

fn dev_auth(home: &Path) -> Result<AuthClient> {
    local::directory(home)?;
    let file = local::owner_file(&home.join(CLOUD_CREDENTIALS_FILE), false)?;
    let mut body = Zeroizing::new(Vec::new());
    file.take(65_537).read_to_end(&mut body)?;
    if body.len() > 65_536 {
        return Err(RestoreError::Limit("saved DEV credentials"));
    }
    let saved = SavedCredentials::parse(&body)
        .map_err(|_| RestoreError::Invalid("saved DEV credentials"))?;
    let endpoint = exemem_api_url_for(DEV);
    if saved.api_url != endpoint || saved.api_key.trim().is_empty() {
        return Err(RestoreError::Invalid(
            "shared snapshot requires saved DEV credentials",
        ));
    }
    Ok(AuthClient::new(
        endpoint.to_owned(),
        saved.api_key.as_str().to_owned(),
    ))
}

fn device_signer(path: &Path) -> Result<Ed25519KeyPair> {
    if !path.is_absolute() {
        return Err(RestoreError::Invalid("absolute shared device key path"));
    }
    local::directory(
        path.parent()
            .ok_or(RestoreError::Invalid("shared device key parent"))?,
    )?;
    let file = local::owner_file(path, false)?;
    let mut seed = Zeroizing::new(Vec::new());
    file.take(33).read_to_end(&mut seed)?;
    Ed25519KeyPair::from_secret_key(&seed).map_err(|_| RestoreError::Invalid("shared device key"))
}

fn native_error(_: fold_db::schema::SchemaError) -> RestoreError {
    RestoreError::Invalid("shared writer state")
}

fn transport_error() -> RestoreError {
    RestoreError::Source("shared DEV transport")
}

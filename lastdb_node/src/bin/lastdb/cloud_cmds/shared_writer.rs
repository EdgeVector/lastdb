//! Explicit owner operations for one stopped, isolated DEV writer home.

use super::super::{SharedWriterAction, SharedWriterCommand};
use super::shared_restore;
use cloud_backup_client::shared_peer::SharedPeerClient;
use cloud_backup_client::{
    shared_log::SharedLogClient, shared_objects::SharedObjectClient, AuthClient,
};
use cloud_backup_protocol::shared::{peer::PeerHeadsRequest, WriterStream};
use cloud_backup_protocol::{AccountId, DecimalU64, Id};
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::factory::{create_fold_db_with_options, FoldDbOptions};
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::FoldDB;
use folddb_profile::endpoints::Environment;
use lastdb_node::shared_peer::{PeerIdentity, PeerOutcome, PeerWorker};
use lastdb_node::shared_snapshot::SignedSnapshotTransport;
use lastdb_node::shared_upload::{upload_next, ServiceObjects, UploadIdentity, UploadOutcome};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::Zeroizing;

#[path = "shared_writer/qualification.rs"]
mod qualification;
#[path = "shared_writer/rejoin.rs"]
mod rejoin;
#[path = "shared_writer/schema.rs"]
mod schema;

const DEV: Environment = Environment::Dev;
const UPLOAD_DEADLINE_SECONDS: u32 = 300;

pub(super) fn run(home: Option<&Path>, selection: SharedWriterCommand) -> Result<(), String> {
    if let SharedWriterAction::DefineSchema {
        namespace,
        declaration,
    } = &selection.action
    {
        return schema::define(namespace, declaration);
    }
    let home = home
        .filter(|path| path.is_absolute())
        .ok_or("shared writer requires an explicit absolute --data-dir")?;
    lastdb_node::offline_home::refuse_primary(home)?;
    if !selection.recovery_phrase_stdin {
        return Err("shared writer requires --recovery-phrase-stdin".into());
    }
    let auth = shared_restore::protected_auth(home, DEV)?;
    let device = id(selection.device_id)?;
    let phrase = super::read_recovery_phrase()?;
    let account = lastdb_node::backup_access::account_from_phrase(&phrase)?;
    let seed = Zeroizing::new(lastdb_node::cloud::seed_from_phrase(&phrase)?);
    let e2e = E2eKeys::from_ed25519_seed(&seed).map_err(|_| "shared writer account key")?;
    let account_signer = Arc::new(
        Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| "shared writer account signer")?,
    );
    let device_signer = Arc::new(shared_restore::device_signer(&selection.device_key_file)?);
    let context = Owner {
        auth,
        account,
        device,
        account_signer,
        device_signer,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct shared writer runtime")?;
    runtime.block_on(context.execute(home, selection.action, &e2e))
}

struct Owner {
    auth: AuthClient,
    account: AccountId,
    device: Id,
    account_signer: Arc<Ed25519KeyPair>,
    device_signer: Arc<Ed25519KeyPair>,
}

impl Owner {
    async fn execute(
        &self,
        home: &Path,
        action: SharedWriterAction,
        e2e: &E2eKeys,
    ) -> Result<(), String> {
        if let SharedWriterAction::QualifyStaging { plan } = &action {
            return qualification::execute(self, plan).await;
        }
        let store = home.join("data");
        if matches!(action, SharedWriterAction::Rejoin { .. }) {
            lastdb_node::shared_sync::require_rejoin_ready(home)
                .map_err(|_| "finish the pending shared snapshot before rejoining")?;
            rejoin::require_source_data(&store)?;
        }
        lastdb_node::offline_home::preflight_stopped_home(home, &store)?;
        let db = create_fold_db_with_options(
            &DatabaseConfig::local(store.clone()),
            e2e,
            Arc::clone(&self.account_signer),
            FoldDbOptions {
                offline_maintenance: true,
                search_outbox_inbox: None,
            },
        )
        .await
        .map_err(|_| "cannot open stopped shared writer home")?;
        if let SharedWriterAction::Rejoin { batches } = action {
            return self.rejoin(db, &store, e2e, batches).await;
        }
        let result = self.run_on_writer(&db, &store, action, e2e).await;
        let shutdown = db
            .shutdown()
            .await
            .map_err(|_| "cannot close shared writer home".to_owned());
        result.and(shutdown)
    }

    async fn run_on_writer(
        &self,
        db: &FoldDB,
        store: &Path,
        action: SharedWriterAction,
        e2e: &E2eKeys,
    ) -> Result<(), String> {
        match action {
            SharedWriterAction::Install { schema_bindings } => {
                let definitions = schema::definitions(&schema_bindings)?;
                schema::install(db, &definitions).await?;
                println!("Shared schemas installed");
            }
            SharedWriterAction::Bind {
                initialize_publisher,
                database_id,
                source_stream,
                schema_bindings,
            } => {
                let stream = WriterStream {
                    database_id: id(database_id)?,
                    source_stream: id(source_stream)?,
                };
                let publisher = self
                    .check_writer_enrollment(&stream, initialize_publisher)
                    .await?;
                let definitions = schema::definitions(&schema_bindings)?;
                db.mutation_manager()
                    .bind_portable_journal(
                        stream.clone(),
                        &definitions,
                        &self.device,
                        initialize_publisher,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                lastdb_node::shared_sync::save_bound(
                    store.parent().ok_or("shared writer home is missing")?,
                    &self.account,
                    stream,
                    &self.device,
                    &self.device_signer,
                    publisher,
                )
                .map_err(|_| "cannot save the shared automatic connection")?;
                println!("Shared writer bound");
            }
            SharedWriterAction::Admit { schema_bindings } => {
                let definitions = schema::admitted(&schema_bindings)?;
                db.mutation_manager()
                    .admit_portable_schemas(&definitions)
                    .await
                    .map_err(|error| error.to_string())?;
                println!("Shared schemas admitted");
            }
            SharedWriterAction::Push { batches } => {
                let content_key = Zeroizing::new(e2e.encryption_key());
                self.push(db, store, &content_key, batches).await?;
            }
            SharedWriterAction::Pull { ranges } => {
                let content_key = Zeroizing::new(e2e.encryption_key());
                self.pull(db, &content_key, ranges).await?;
            }
            SharedWriterAction::DefineSchema { .. } => {
                unreachable!("this action has no source writer")
            }
            SharedWriterAction::Rejoin { .. } | SharedWriterAction::QualifyStaging { .. } => {
                unreachable!("handled before writer operations")
            }
        }
        Ok(())
    }

    async fn check_writer_enrollment(
        &self,
        stream: &WriterStream,
        initialize_publisher: bool,
    ) -> Result<bool, String> {
        let snapshots = SignedSnapshotTransport::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
            self.device_signer.clone(),
        )
        .map_err(|error| error.to_string())?;
        let publisher = match snapshots.current_snapshot(&stream.database_id).await {
            Ok(head) => {
                if initialize_publisher && head.is_some() {
                    return Err(
                        "the database already has a snapshot; restore before binding".into(),
                    );
                }
                true
            }
            Err(lastdb_node::byte_cloud_restore::RestoreError::Protocol(error))
                if !initialize_publisher
                    && error.code == cloud_backup_protocol::ErrorCode::Unauthorized
                    && error.field == "shared.snapshot.publisher" =>
            {
                false
            }
            Err(error) => return Err(error.to_string()),
        };
        let peers = SharedPeerClient::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
        )
        .map_err(|error| error.to_string())?;
        peers
            .heads(
                &PeerHeadsRequest {
                    reader: stream.clone(),
                },
                DecimalU64::new(deadline(30)?),
                |message| Ok(self.device_signer.sign(message).to_bytes()),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(publisher)
    }

    async fn push(
        &self,
        db: &FoldDB,
        store: &Path,
        content_key: &[u8; 32],
        batches: u16,
    ) -> Result<(), String> {
        let logs =
            SharedLogClient::from_auth(self.auth.clone()).map_err(|error| error.to_string())?;
        let objects = SharedObjectClient::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
        )
        .map_err(|error| error.to_string())?;
        let identity = UploadIdentity {
            account: &self.account,
            device: &self.device,
            device_signer: &self.device_signer,
            content_key,
        };
        let uploader = ServiceObjects {
            client: &objects,
            device_signer: &self.device_signer,
            deadline_ms: deadline(UPLOAD_DEADLINE_SECONDS)?,
        };
        for _ in 0..batches {
            match upload_next(store, db.mutation_manager(), &identity, &logs, &uploader)
                .await
                .map_err(|error| error.to_string())?
            {
                UploadOutcome::Idle => {
                    println!("Shared writer caught up");
                    break;
                }
                UploadOutcome::Accepted(head) => {
                    println!("Accepted shared writer position {}", head.sequence.get());
                }
                UploadOutcome::Reconcile(_) => {
                    return Err("shared writer acceptance needs reconciliation".into())
                }
            }
        }
        Ok(())
    }

    async fn pull(&self, db: &FoldDB, content_key: &[u8; 32], ranges: u16) -> Result<(), String> {
        let client = SharedPeerClient::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
        )
        .map_err(|error| error.to_string())?;
        let worker = PeerWorker::new(
            db,
            &client,
            PeerIdentity {
                device_signer: &self.device_signer,
                content_key,
            },
        );
        let mut applied = 0;
        for _ in 0..ranges {
            let (outcome, reclaimed) = worker.step().await.map_err(|error| error.to_string())?;
            match outcome {
                PeerOutcome::Idle => {}
                PeerOutcome::Applied { stream, through } => {
                    applied += 1;
                    println!(
                        "Applied shared peer {} through {}",
                        stream.as_str(),
                        through
                    );
                }
                PeerOutcome::NeedsCheckpoint { stream } => {
                    return Err(format!(
                        "shared peer {} requires a new checkpoint",
                        stream.as_str()
                    ));
                }
            }
            if reclaimed != 0 {
                println!("Reclaimed {reclaimed} shared delete notes");
            }
        }
        println!("Applied {applied} shared peer ranges");
        Ok(())
    }
}

fn deadline(seconds: u32) -> Result<u64, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "shared writer clock")?;
    let ms = now.as_millis().saturating_add(u128::from(seconds) * 1000);
    u64::try_from(ms).map_err(|_| "shared writer deadline".into())
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "shared writer identifiers require canonical UUIDs".into())
}

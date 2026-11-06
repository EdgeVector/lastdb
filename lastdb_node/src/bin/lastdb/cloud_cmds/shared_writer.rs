//! Explicit owner operations for one stopped, isolated DEV writer home.

use super::super::{SharedWriterAction, SharedWriterCommand};
use super::shared_restore;
use cloud_backup_client::shared_peer::SharedPeerClient;
use cloud_backup_client::{
    shared_log::SharedLogClient, shared_objects::SharedObjectClient, AuthClient,
};
use cloud_backup_protocol::shared::WriterStream;
use cloud_backup_protocol::{AccountId, Id};
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::factory::{create_fold_db_with_options, FoldDbOptions};
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::FoldDB;
use folddb_profile::endpoints::{schema_service_url_for, Environment};
use lastdb_node::byte_cloud_restore::RecoveryKeys;
use lastdb_node::shared_peer::{PeerIdentity, PeerOutcome, PeerWorker};
use lastdb_node::shared_restore::recovery::SnapshotSchema;
use lastdb_node::shared_snapshot::{
    filtered::{prepare_native, publish_native, resume_native, NativePublishRequest},
    SignedSnapshotTransport, SnapshotOutcome,
};
use lastdb_node::shared_upload::{upload_next, ServiceObjects, UploadIdentity, UploadOutcome};
use schema_service_client::SchemaServiceClient;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::Zeroizing;

#[path = "shared_writer/schema.rs"]
mod schema;

const DEV: Environment = Environment::Dev;
const UPLOAD_DEADLINE_SECONDS: u32 = 300;

pub(super) fn run(home: Option<&Path>, selection: SharedWriterCommand) -> Result<(), String> {
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
    let keys =
        RecoveryKeys::from_scope_phrase(&phrase, &account).map_err(|error| error.to_string())?;
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
        keys,
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
    keys: RecoveryKeys,
}

impl Owner {
    async fn execute(
        &self,
        home: &Path,
        action: SharedWriterAction,
        e2e: &E2eKeys,
    ) -> Result<(), String> {
        let action = match action {
            SharedWriterAction::PublishSchema {
                namespace,
                declaration,
            } => {
                return schema::publish(&namespace, &declaration, &self.account_signer).await;
            }
            SharedWriterAction::Resume { workspace } => {
                let transport = self.publisher()?;
                let result = resume_native(&workspace, &transport, &self.keys)
                    .await
                    .map_err(|error| error.to_string())?;
                return print_snapshot(result);
            }
            action => action,
        };
        let store = home.join("data");
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
                let definitions = schema::definitions(&schema_bindings).await?;
                schema::install(db, &definitions).await?;
                println!("Shared schemas installed");
            }
            SharedWriterAction::Bind {
                database_id,
                source_stream,
                schema_bindings,
            } => {
                let stream = WriterStream {
                    database_id: id(database_id)?,
                    source_stream: id(source_stream)?,
                };
                let definitions = schema::definitions(&schema_bindings).await?;
                db.mutation_manager()
                    .bind_portable_journal(stream, &definitions)
                    .await
                    .map_err(|error| error.to_string())?;
                println!("Shared writer bound");
            }
            SharedWriterAction::Push { batches } => {
                let content_key = Zeroizing::new(e2e.encryption_key());
                self.push(db, store, &content_key, batches).await?;
            }
            SharedWriterAction::Pull {
                schema_bindings,
                ranges,
            } => {
                let content_key = Zeroizing::new(e2e.encryption_key());
                self.pull(db, &schema_bindings, &content_key, ranges)
                    .await?;
            }
            SharedWriterAction::Publish {
                workspace,
                snapshot_id,
                schema_bindings,
                deadline_seconds,
                prepare_only,
            } => {
                let transport = self.publisher()?;
                let definitions = schema::definitions(&schema_bindings).await?;
                let request = NativePublishRequest {
                    workspace: &workspace,
                    snapshot_id: id(snapshot_id)?,
                    expires_at_ms: deadline(deadline_seconds)?,
                    definitions: &definitions,
                };
                if prepare_only {
                    prepare_native(db.mutation_manager(), request, &transport, &self.keys)
                        .await
                        .map_err(|error| error.to_string())?;
                    println!("Prepared shared snapshot cut");
                } else {
                    let result =
                        publish_native(db.mutation_manager(), request, &transport, &self.keys)
                            .await
                            .map_err(|error| error.to_string())?;
                    print_snapshot(result)?;
                }
            }
            SharedWriterAction::Resume { .. } | SharedWriterAction::PublishSchema { .. } => {
                unreachable!("this action has no source writer")
            }
        }
        Ok(())
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

    async fn pull(
        &self,
        db: &FoldDB,
        bindings: &Path,
        content_key: &[u8; 32],
        ranges: u16,
    ) -> Result<(), String> {
        let client = SharedPeerClient::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
        )
        .map_err(|error| error.to_string())?;
        let service = SchemaServiceClient::new(schema_service_url_for(DEV));
        let definitions = schema::definitions(bindings).await?;
        let schemas: BTreeMap<_, _> = definitions
            .into_iter()
            .map(|(local_schema, definition)| {
                (
                    definition.id().clone(),
                    SnapshotSchema {
                        local_schema,
                        definition,
                    },
                )
            })
            .collect();
        let mut worker = PeerWorker::new(
            db,
            &client,
            &service,
            PeerIdentity {
                device_signer: &self.device_signer,
                content_key,
            },
            schemas,
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

    fn publisher(&self) -> Result<SignedSnapshotTransport, String> {
        SignedSnapshotTransport::from_auth(
            self.auth.clone(),
            self.account.clone(),
            self.device.clone(),
            Arc::clone(&self.device_signer),
        )
        .map_err(|error| error.to_string())
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

fn print_snapshot(result: SnapshotOutcome) -> Result<(), String> {
    match result {
        SnapshotOutcome::Published(receipt) => {
            println!("Published shared snapshot {}", receipt.snapshot_id.as_str());
            Ok(())
        }
        SnapshotOutcome::Abandoned => Err("shared snapshot publication was abandoned".into()),
    }
}

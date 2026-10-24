use super::super::super::fetch::FetchedEntry;
use super::super::{DownloadProgress, SyncEngine};
use super::{homes, require, Result};
use crate::crypto::{CryptoProvider, LocalCryptoProvider};
use crate::db_operations::atom_store::counter::restore_validation;
use crate::schema::types::KeyValue;
use crate::storage::disk_pack::restore_safety;
use crate::sync::auth::{AuthClient, SyncAuth};
use crate::sync::log::{LogEntry, LogOp, LogicalChange, MutationEnvelope};
use crate::sync::org_sync::SyncTarget;
use crate::sync::s3::S3Client;
use crate::sync::SyncConfig;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

pub(super) async fn run(root: &Path) -> Result<Value> {
    let home = homes::Home::open(&root.join("replay"))?;
    home.initialize().await?;
    homes::snapshot(root)?
        .restore(home.logical.as_ref())
        .await?;
    let db = home.db().await?;
    let (engine, target) = make_engine(&home, &db).await?;
    engine.save_download_cursor("", 41).await;
    engine
        .download_cursors
        .lock()
        .await
        .insert(String::new(), 41);
    let mut progress = DownloadProgress {
        max_contiguous_seq: 41,
        ..Default::default()
    };
    counted_intent(&engine, &target, &home, &mut progress).await?;
    let refused = refusals(&engine, &target, &home, root, &mut progress).await?;
    db.shutdown_local_only().await?;
    Ok(
        json!({"case":"raw-replay", "passed":true, "complete_mutation_counted":true,
        "refused_operations":refused, "memory_cursor":42, "disk_cursor":42, "partial_effects":0}),
    )
}

async fn make_engine(
    home: &homes::Home,
    db: &Arc<crate::fold_db_core::FoldDB>,
) -> Result<(SyncEngine, SyncTarget)> {
    let http = Arc::new(reqwest::Client::builder().build()?);
    let crypto: Arc<dyn CryptoProvider> = Arc::new(LocalCryptoProvider::from_key(
        homes::keys().encryption_key(),
    ));
    let auth = AuthClient::new(
        Arc::clone(&http),
        "http://127.0.0.1:9".into(),
        SyncAuth::BearerToken(String::new()),
    );
    let mut engine = SyncEngine::new(
        "generated-receiver".into(),
        Arc::clone(&crypto),
        S3Client::new(http),
        auth,
        Arc::clone(&home.logical),
        SyncConfig::default(),
        homes::signer()?,
    );
    engine.set_at_rest_key(homes::keys().encryption_key());
    engine.set_cursor_store(Arc::clone(&home.raw));
    engine.set_atom_store(db.db_ops().atoms().clone()).await;
    let materialize_db = Arc::clone(&db);
    let materializer: crate::sync::engine::types::MutationIntentMaterializer =
        Arc::new(move |envelopes| {
            let db = Arc::clone(&materialize_db);
            Box::pin(async move {
                crate::sync::mutation_intent::materialize_field_values(
                    envelopes,
                    db.db_ops().atoms(),
                )
                .await
            })
        });
    engine
        .set_mutation_intent_applier(db.pr354_replay_applier(materializer))
        .await;
    let target = SyncTarget {
        label: "generated-personal".into(),
        prefix: String::new(),
        crypto,
        read_binding: None,
    };
    Ok((engine, target))
}

async fn counted_intent(
    engine: &SyncEngine,
    target: &SyncTarget,
    home: &homes::Home,
    progress: &mut DownloadProgress,
) -> Result<()> {
    let intent: MutationEnvelope = serde_json::from_value(json!({
        "schema_name":homes::SCHEMA, "mutation_type":"update",
        "key_value":KeyValue::new(Some("item-000".into()), None),
        "fields_and_values":{"id":"item-000", "payload":"peer-value"}, "pub_key":"generated-drill",
        "written_at":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos() as u64,
        "writer_id":"generated-peer", "mutation_uuid":uuid::Uuid::new_v4().to_string()
    }))?;
    engine
        .apply_fetched_entry(
            &target,
            fetched(
                42,
                LogOp::MutationIntent {
                    mutations: vec![intent],
                },
            ),
            progress,
        )
        .await?;
    require(
        progress.max_contiguous_seq == 42 && progress.total_replayed == 1,
        "complete mutation did not advance the real replay cursor",
    )?;
    restore_safety::exclusive(home.runtime(), async {
        restore_validation::validate(home.logical.as_ref()).await?;
        restore_validation::finish(home.logical.as_ref(), &home.runtime()).await
    })
    .await?;
    Ok(())
}

async fn refusals(
    engine: &SyncEngine,
    target: &SyncTarget,
    home: &homes::Home,
    root: &Path,
    progress: &mut DownloadProgress,
) -> Result<usize> {
    let snapshot = homes::snapshot(root)?;
    let tip = snapshot
        .namespaces
        .iter()
        .find(|namespace| namespace.name == "main")
        .ok_or("missing main")?
        .entries
        .iter()
        .find(|entry| {
            BASE64
                .decode(&entry.key)
                .is_ok_and(|key| key.starts_with(b"mk:"))
        })
        .ok_or("missing tip")?;
    let cases = raw_cases(&tip.key, &tip.value);
    let count = cases.len();
    require(count >= 21, "membership refusal cases are missing")?;
    let main = home.logical.open_namespace("main").await?;
    let opaque_before = b"original generated metadata".to_vec();
    main.put(b"drill:must-not-write", opaque_before.clone())
        .await?;
    let cursors = home.raw.open_namespace("sync_cursors").await?;
    let before = cursors
        .get(b"cursor:")
        .await?
        .ok_or("cursor was not durable")?;
    let protected_key = BASE64.decode(&tip.key)?;
    let protected_before = main.get(&protected_key).await?;
    for op in cases {
        let error = engine
            .apply_fetched_entry(&target, fetched(43, op), progress)
            .await
            .expect_err("raw lifecycle replay succeeded");
        require(
            error.to_string().contains("counter"),
            "replay failed for another reason",
        )?;
        require(
            progress.max_contiguous_seq == 42 && progress.total_replayed == 1,
            "refused entry advanced cycle progress",
        )?;
        require(
            engine.download_cursors.lock().await.get("") == Some(&42),
            "refused entry advanced memory cursor",
        )?;
        require(
            cursors.get(b"cursor:").await?.as_ref() == Some(&before),
            "refused entry advanced disk cursor",
        )?;
        require(
            main.get(b"drill:must-not-write").await? == Some(opaque_before.clone()),
            "preflight allowed a partial batch effect",
        )?;
        require(
            main.get(&protected_key).await? == protected_before,
            "refused entry changed its lifecycle record",
        )?;
    }
    Ok(count)
}

fn fetched(seq: u64, op: LogOp) -> FetchedEntry {
    FetchedEntry {
        seq,
        entry: Some(LogEntry {
            seq,
            timestamp_ms: 1,
            device_id: "generated-peer".into(),
            op,
        }),
        mutation_log_records: None,
        ciphertext_bytes: 1,
        skipped_oversize: false,
    }
}

fn raw_cases(key: &str, value: &str) -> Vec<LogOp> {
    let opaque = LogOp::encode_bytes(b"drill:must-not-write");
    let bytes = LogOp::encode_bytes(b"generated opaque metadata");
    let mut cases = vec![
        LogOp::Put {
            namespace: "main".into(),
            key: key.into(),
            value: value.into(),
        },
        LogOp::Delete {
            namespace: "main".into(),
            key: key.into(),
        },
        LogOp::BatchPut {
            namespace: "main".into(),
            items: vec![(opaque.clone(), bytes.clone()), (key.into(), value.into())],
        },
        LogOp::BatchDelete {
            namespace: "main".into(),
            keys: vec![opaque.clone(), key.into()],
        },
        LogOp::LogicalCommit {
            changes: vec![
                LogicalChange {
                    namespace: "main".into(),
                    key: opaque,
                    value: Some(bytes),
                },
                LogicalChange {
                    namespace: "main".into(),
                    key: key.into(),
                    value: Some(value.into()),
                },
            ],
        },
        LogOp::Put {
            namespace: "atoms".into(),
            key: LogOp::encode_bytes(b"physical-alias"),
            value: value.into(),
        },
        LogOp::Put {
            namespace: "molecule_ref_edges".into(),
            key: LogOp::encode_bytes(b"physical-alias"),
            value: value.into(),
        },
    ];
    for (namespace, key) in [
        ("main", "protein:generated"),
        ("main", "molprot:generated"),
        ("proteins", "physical-alias"),
        ("schemas", "generated-authority"),
        ("schema_states", "generated-authority"),
        ("schema_superseded_by", "generated-authority"),
        ("db_catalog", "generated-authority"),
    ] {
        let key = LogOp::encode_bytes(key.as_bytes());
        cases.push(LogOp::Delete {
            namespace: namespace.into(),
            key: key.clone(),
        });
        cases.push(LogOp::LogicalCommit {
            changes: vec![
                LogicalChange {
                    namespace: "main".into(),
                    key: LogOp::encode_bytes(b"drill:must-not-write"),
                    value: Some(LogOp::encode_bytes(b"must not publish")),
                },
                LogicalChange {
                    namespace: namespace.into(),
                    key,
                    value: Some(LogOp::encode_bytes(b"generated authority")),
                },
            ],
        });
    }
    cases
}

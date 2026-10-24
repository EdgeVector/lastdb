use super::{canonical, require, Result};
use crate::benchmark_support::BenchmarkSchemaBuilder;
use crate::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use crate::fold_db_core::factory::{create_fold_db, create_fold_db_with_options, FoldDbOptions};
use crate::fold_db_core::FoldDB;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation};
use crate::security::Ed25519KeyPair;
use crate::storage::config::DatabaseConfig;
use crate::storage::disk_pack::{PackNamespacedStore, PackReadScope, PackRuntime};
use crate::storage::{EncryptingNamespacedStore, NamespacedStore};
use crate::sync::snapshot::Snapshot;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) const SCHEMA: &str = "CounterRecoveryDrill";

// A public, synthetic key protects generated data only. No account credential is read.
pub(super) fn keys() -> E2eKeys {
    E2eKeys::from_secret(&Sha256::digest(b"PR354 public generated-data drill").into())
}

pub(super) fn signer() -> Result<Arc<Ed25519KeyPair>> {
    Ok(Arc::new(
        Ed25519KeyPair::generate().map_err(std::io::Error::other)?,
    ))
}

pub(super) struct Home {
    pub path: PathBuf,
    pub raw: Arc<dyn NamespacedStore>,
    pub logical: Arc<dyn NamespacedStore>,
}

impl Home {
    pub fn open(path: &Path) -> Result<Self> {
        let raw: Arc<dyn NamespacedStore> = Arc::new(PackNamespacedStore::open(path)?);
        let crypto: Arc<dyn CryptoProvider> =
            Arc::new(LocalCryptoProvider::from_key(keys().encryption_key()));
        let logical = Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
            Arc::clone(&raw),
            crypto,
            crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .copied()
                .map(str::to_string)
                .collect(),
        ));
        Ok(Self {
            path: path.to_owned(),
            raw,
            logical,
        })
    }

    pub fn runtime(&self) -> Arc<PackRuntime> {
        self.raw.disk_pack_runtime().expect("native drill runtime")
    }

    pub async fn db(&self) -> Result<Arc<FoldDB>> {
        Ok(Arc::new(
            FoldDB::for_restore(
                Arc::clone(&self.raw),
                self.path.to_str().ok_or("non-UTF-8 drill path")?,
                signer()?,
                &keys(),
            )
            .await?,
        ))
    }

    pub async fn initialize(&self) -> Result<()> {
        let db = self.db().await?;
        db.shutdown_local_only().await?;
        Ok(())
    }
}

pub(super) fn mutation(key: &str, payload: &str, kind: MutationType) -> Mutation {
    Mutation::new(
        SCHEMA.to_string(),
        HashMap::from([
            ("id".into(), json!(key)),
            ("payload".into(), json!(payload)),
        ]),
        KeyValue::new(Some(key.into()), None),
        "generated-drill".into(),
        kind,
    )
}

pub(super) async fn seed(root: &Path) -> Result<Value> {
    std::fs::create_dir_all(root)?;
    let db = create_fold_db_with_options(
        &DatabaseConfig::local(root.join("source")),
        &keys(),
        signer()?,
        FoldDbOptions {
            offline_maintenance: true,
            ..Default::default()
        },
    )
    .await?;
    db.load_schema_from_json(
        &BenchmarkSchemaBuilder::new(SCHEMA)
            .fields(&["id", "payload"])
            .hash_key("id")
            .build_json(),
    )
    .await?;
    let mutations = (0..96)
        .map(|i| {
            mutation(
                &format!("item-{i:03}"),
                &format!("value-{i:03}"),
                MutationType::Create,
            )
        })
        .collect();
    db.mutation_manager()
        .write_mutations_batch_async(mutations, None)
        .await?;
    db.flush().await?;
    let snapshot = Snapshot::create(
        db.db_ops().namespaced_store().as_ref(),
        "generated-source",
        41,
    )
    .await?;
    let main = snapshot
        .namespaces
        .iter()
        .find(|namespace| namespace.name == "main")
        .ok_or("missing main")?;
    require(
        main.entries.len() > 128,
        "source is too small for an interrupted multi-batch restore",
    )?;
    std::fs::write(root.join("snapshot.json"), serde_json::to_vec(&snapshot)?)?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"seed", "passed":true, "records":96, "main_entries":main.entries.len()}))
}

pub(super) fn snapshot(root: &Path) -> Result<Snapshot> {
    Ok(serde_json::from_slice(&std::fs::read(
        root.join("snapshot.json"),
    )?)?)
}

pub(super) async fn compare(expected: &Snapshot, store: &dyn NamespacedStore) -> Result<()> {
    let actual = Snapshot::create(store, "restored", expected.last_seq).await?;
    require(
        canonical(expected) == canonical(&actual),
        "restored logical records differ from their source",
    )
}

pub(super) async fn reopen(root: &Path, name: &str) -> Result<Value> {
    let db = create_fold_db(&DatabaseConfig::local(root.join(name)), &keys(), signer()?).await?;
    let store = db.db_ops().namespaced_store();
    let scope = PackReadScope::for_store(&store)
        .await?
        .ok_or("missing native read scope")?;
    scope
        .run_result(compare_known(&snapshot(root)?, store.as_ref()))
        .await?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":format!("reopen-{name}"), "passed":true, "normal_serving_open":true}))
}

async fn compare_known(expected: &Snapshot, store: &dyn NamespacedStore) -> Result<()> {
    let main = expected
        .namespaces
        .iter()
        .find(|namespace| namespace.name == "main")
        .ok_or("missing main")?;
    let kv = store.open_namespace("main").await?;
    let mut entries = Vec::new();
    for entry in &main.entries {
        let key = BASE64.decode(&entry.key)?;
        if crate::storage::current_keys::counter_owned("main", &key)? {
            entries.push((key, BASE64.decode(&entry.value)?));
        }
    }
    require(
        entries.len() > 128,
        "reopen did not check the restored lifecycle records",
    )?;
    for chunk in entries.chunks(128) {
        let found = kv
            .get_many(chunk.iter().map(|(key, _)| key.clone()).collect())
            .await?;
        require(found.len() == chunk.len(), "incomplete reopen batch")?;
        require(
            found
                .iter()
                .zip(chunk)
                .all(|(actual, (_, expected))| actual.as_ref() == Some(expected)),
            "a restored lifecycle record changed before its normal serving read",
        )?;
    }
    Ok(())
}

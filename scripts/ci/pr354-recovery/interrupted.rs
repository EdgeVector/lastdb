use super::{homes, require, Result};
use crate::fold_db_core::factory::create_fold_db;
use crate::storage::config::DatabaseConfig;
use crate::storage::disk_pack::{restore_safety, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::traits::{ExecutionModel, FlushBehavior, PhysicalScanCursor, PhysicalScanPage};
use crate::storage::{KvMutation, KvStore, NamespacedStore};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

struct CutStore {
    inner: Arc<dyn NamespacedStore>,
    batches: Arc<AtomicUsize>,
    reached: Arc<Notify>,
}

struct CutKv {
    inner: Arc<dyn KvStore>,
    batches: Arc<AtomicUsize>,
    reached: Arc<Notify>,
}

#[async_trait]
impl NamespacedStore for CutStore {
    fn disk_pack_runtime(&self) -> Option<Arc<PackRuntime>> {
        self.inner.disk_pack_runtime()
    }
    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let inner = self.inner.open_namespace(name).await?;
        if name != "main" {
            return Ok(inner);
        }
        Ok(Arc::new(CutKv {
            inner,
            batches: Arc::clone(&self.batches),
            reached: Arc::clone(&self.reached),
        }))
    }
    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }
    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }
    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        self.inner.restore_durability_barrier().await
    }
}

#[async_trait]
impl KvStore for CutKv {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn get_many(&self, keys: Vec<Vec<u8>>) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.inner.get_many(keys).await
    }
    async fn put(&self, key: &[u8], value: Vec<u8>) -> StorageResult<()> {
        self.inner.put(key, value).await
    }
    async fn delete(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.delete(key).await
    }
    async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.exists(key).await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_range_physical_paged(
        &self,
        start: &[u8],
        end: &[u8],
        cursor: Option<&PhysicalScanCursor>,
        limit: usize,
        max_handles: usize,
    ) -> StorageResult<PhysicalScanPage> {
        self.inner
            .scan_range_physical_paged(start, end, cursor, limit, max_handles)
            .await
    }
    async fn batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        self.inner.batch_put(items).await
    }
    async fn batch_mutate(&self, mutations: Vec<KvMutation>) -> StorageResult<()> {
        self.inner.batch_mutate(mutations).await
    }
    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        self.inner.batch_delete(keys).await
    }
    async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
    fn execution_model(&self) -> ExecutionModel {
        self.inner.execution_model()
    }
    fn flush_behavior(&self) -> FlushBehavior {
        self.inner.flush_behavior()
    }
    async fn restore_batch_put(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<()> {
        if self.batches.load(Ordering::SeqCst) != 0 {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.restore_batch_put(items).await?;
        // Retain one real durable restore batch before cancellation.
        self.inner.flush().await?;
        self.batches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) async fn interrupt(root: &Path) -> Result<Value> {
    let home = homes::Home::open(&root.join("partial"))?;
    home.initialize().await?;
    let batches = Arc::new(AtomicUsize::new(0));
    let reached = Arc::new(Notify::new());
    let store = CutStore {
        inner: Arc::clone(&home.logical),
        batches: Arc::clone(&batches),
        reached: Arc::clone(&reached),
    };
    let snapshot = homes::snapshot(root)?;
    let task = tokio::spawn(async move { snapshot.restore(&store).await });
    tokio::time::timeout(std::time::Duration::from_secs(30), reached.notified()).await?;
    task.abort();
    require(
        task.await.is_err_and(|error| error.is_cancelled()),
        "restore did not cancel",
    )?;
    require(
        batches.load(Ordering::SeqCst) == 1,
        "cancellation did not retain exactly one main batch",
    )?;
    let runtime = home.runtime();
    require(
        runtime.restore_validation_pending(),
        "cancellation cleared pending state",
    )?;
    let owner = Arc::clone(&runtime);
    let pending = restore_safety::exclusive(runtime, async {
        restore_safety::raw_main(owner)
            .await?
            .get(restore_safety::PENDING_KEY.as_bytes())
            .await
    })
    .await?;
    require(pending.is_some(), "durable pending marker is absent")?;
    require(
        home.logical.open_namespace("main").await.is_err(),
        "ordinary operation bypassed pending state",
    )?;
    Ok(
        json!({"case":"interrupt", "passed":true, "durable_main_batches":1,
        "pending_marker":true, "ordinary_operations_refused":true}),
    )
}

pub(super) async fn refuse(root: &Path) -> Result<Value> {
    let result = create_fold_db(
        &DatabaseConfig::local(root.join("partial")),
        &homes::keys(),
        homes::signer()?,
    )
    .await;
    match result {
        Ok(db) => {
            db.shutdown_local_only().await?;
            Err(std::io::Error::other("normal startup accepted incomplete restore").into())
        }
        Err(error) => {
            require(
                error.to_string().contains("snapshot restore is incomplete"),
                "startup failed for another reason",
            )?;
            Ok(json!({"case":"refuse-partial", "passed":true, "normal_startup_refused":true}))
        }
    }
}

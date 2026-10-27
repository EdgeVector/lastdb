//! CI-only real node-store cleanup, old reads, later writes and durable resume.

use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus, PackReadScope};

const COUNT: usize = 512;
const MAIN: &str = "cleanup-proof";
const CAS: &str = "cas_blobs";

pub(super) async fn run() -> Result<()> {
    let folder = tempfile::TempDir::new()?;
    let mut rng = Rng::new(31);
    let node_key: [u8; 32] = rng.bytes(32).try_into().expect("32 bytes");
    held_read(&folder.path().join("held"), &node_key).await?;
    resume(&folder.path().join("resume"), &node_key).await
}

async fn seed(node: &Path, node_key: &[u8; 32]) -> Result<PackNamespacedStore> {
    let store = PackNamespacedStore::create_run(node, node_key)?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    for name in [MAIN, CAS] {
        let kv = store.open_namespace(name).await?;
        kv.batch_put(
            (0..COUNT)
                .map(|n| (key(n), vec![(n % 251) as u8; 256]))
                .collect(),
        )
        .await?;
    }
    Ok(store)
}

async fn delete_half(store: &PackNamespacedStore) -> Result<()> {
    for name in [MAIN, CAS] {
        store
            .open_namespace(name)
            .await?
            .batch_delete((0..COUNT / 2).map(key).collect())
            .await?;
    }
    Ok(())
}

async fn held_read(node: &Path, node_key: &[u8; 32]) -> Result<()> {
    let store = seed(node, node_key).await?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    let held = PackReadScope::for_store(&shared)
        .await?
        .ok_or("no held native read")?;
    delete_half(&store).await?;
    let runtime = store.disk_pack_runtime().ok_or("no cleanup runtime")?;
    let request = runtime.request_native_cleanup(None).await?;
    if request.phase != NativeCleanupPhase::Pending || request.complete {
        return Err("offline cleanup request did not remain durably pending".into());
    }
    runtime.resume_native_cleanup();
    wait(&runtime, NativeCleanupPhase::WaitingForReads).await?;
    for name in [MAIN, CAS] {
        store
            .open_namespace(name)
            .await?
            .put(b"later", vec![99; 256])
            .await?;
    }
    held.run(async {
        for name in [MAIN, CAS] {
            let values = store
                .open_namespace(name)
                .await?
                .get_many((0..COUNT).map(key).collect())
                .await?;
            for (n, value) in values.into_iter().enumerate() {
                if value != Some(vec![(n % 251) as u8; 256]) {
                    return Err("cleanup changed an old held read".into());
                }
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?;
    if runtime.status_cleanup().complete {
        return Err("cleanup completed before read release".into());
    }
    drop(held);
    let completed = wait(&runtime, NativeCleanupPhase::Complete).await?;
    check_values(&store, true).await?;
    let target = request.target.ok_or("no target")?;
    let finish = completed
        .completed_through
        .ok_or("no completion boundary")?;
    if finish.main_frontier <= target.main_frontier || finish.cas_frontier <= target.cas_frontier {
        return Err("cleanup completion lost later writes".into());
    }
    println!("node cleanup held read: both homes, 512 seeded each, 256 deleted each, old reads equal, incomplete until release, later writes retained, complete after release");
    Ok(())
}

async fn resume(node: &Path, node_key: &[u8; 32]) -> Result<()> {
    let store = seed(node, node_key).await?;
    delete_half(&store).await?;
    let runtime = store.disk_pack_runtime().ok_or("no cleanup runtime")?;
    let pending = runtime.request_native_cleanup(None).await?;
    drop((runtime, store));
    let store = super::reopen(node, node_key).await?;
    let runtime = store.disk_pack_runtime().ok_or("no reopened runtime")?;
    let restored = runtime.status_cleanup();
    if restored.request_id != pending.request_id
        || restored.target != pending.target
        || restored.complete
    {
        return Err("reopen lost the pending cleanup request".into());
    }
    runtime.resume_native_cleanup();
    let completed = wait(&runtime, NativeCleanupPhase::Complete).await?;
    check_values(&store, false).await?;
    let id = completed.request_id.clone().ok_or("no completed id")?;
    let boundary = completed.completed_through;
    drop((runtime, store));
    let store = super::reopen(node, node_key).await?;
    let runtime = store.disk_pack_runtime().ok_or("no completed runtime")?;
    let repeat = runtime
        .request_native_cleanup(Some(uuid::Uuid::parse_str(&id)?))
        .await?;
    if !repeat.complete
        || repeat.completed_through != boundary
        || repeat.request_id.as_deref() != Some(id.as_str())
    {
        return Err("repeat/reopen lost the completed cleanup receipt".into());
    }
    println!("node cleanup durable request: both homes, pending request reopened and resumed, current reads equal, completed receipt reopened, same-id repeat unchanged");
    Ok(())
}

async fn check_values(store: &PackNamespacedStore, later: bool) -> Result<()> {
    for name in [MAIN, CAS] {
        let kv = store.open_namespace(name).await?;
        let values = kv.get_many((0..COUNT).map(key).collect()).await?;
        for (n, value) in values.into_iter().enumerate() {
            let expected = (n >= COUNT / 2).then(|| vec![(n % 251) as u8; 256]);
            if value != expected {
                return Err("cleanup changed current values".into());
            }
        }
        if later && kv.get(b"later").await? != Some(vec![99; 256]) {
            return Err("cleanup lost a later write".into());
        }
    }
    Ok(())
}

async fn wait(
    runtime: &Arc<fold_db::storage::disk_pack::PackRuntime>,
    phase: NativeCleanupPhase,
) -> Result<NativeCleanupStatus> {
    let until = Instant::now() + Duration::from_secs(180);
    loop {
        let status = runtime.status_cleanup();
        if status.phase == phase {
            return Ok(status);
        }
        if matches!(
            status.phase,
            NativeCleanupPhase::Retrying | NativeCleanupPhase::RecoveryRequired
        ) || Instant::now() >= until
        {
            return Err(format!("cleanup did not reach {phase:?}: {status:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

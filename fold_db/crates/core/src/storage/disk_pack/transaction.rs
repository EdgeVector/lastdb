use super::*;
use std::collections::BTreeMap;

pub(crate) mod publication;

pub(super) struct PendingRecord {
    pub(super) collection: String,
    pub(super) key: String,
    pub(super) body: Option<Vec<u8>>,
}

#[derive(Default)]
struct PendingData {
    operations: Vec<Arc<PendingRecord>>,
    latest: BTreeMap<(String, String), usize>,
    bytes: u64,
}

pub(super) struct PendingBatch {
    identity: uuid::Uuid,
    data: Mutex<PendingData>,
    publications: publication::PendingPublications,
}

tokio::task_local! {
    static WRITE: Arc<PendingBatch>;
}

impl PendingBatch {
    pub(super) fn current(runtime: &PackRuntime) -> Option<Arc<Self>> {
        WRITE
            .try_with(Arc::clone)
            .ok()
            .filter(|batch| batch.identity == runtime.home.descriptor().identity)
    }

    pub(super) fn lookup(&self, collection: &str, key: &str) -> Option<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .get(&(collection.to_string(), key.to_string()))
            .map(|&at| Arc::clone(&data.operations[at]))
    }

    pub(super) fn matching(
        &self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let data = self.data.lock().expect("native pending batch");
        data.latest
            .iter()
            .filter(|((name, key), _)| {
                name == collection && key.as_bytes() >= start && key.as_bytes() < end
            })
            .map(|(_, at)| Arc::clone(&data.operations[*at]))
            .collect()
    }

    pub(super) fn stage(&self, operations: Vec<HomeMutation>) -> StorageResult<()> {
        validate(&operations)?;
        let mut data = self.data.lock().expect("native pending batch");
        let count = data
            .operations
            .len()
            .checked_add(operations.len())
            .filter(|count| *count <= 4096)
            .ok_or_else(|| {
                StorageError::InvalidOperation(
                    "complete native mutation exceeds 4096 storage operations".into(),
                )
            })?;
        let mut bytes = data.bytes;
        for operation in &operations {
            bytes = bytes
                .checked_add(
                    (160 + operation.collection.len()
                        + 2 * operation.key.len()
                        + operation.body.as_ref().map_or(0, Vec::len)) as u64,
                )
                .filter(|bytes| *bytes <= 128 * 1024 * 1024 - 256)
                .ok_or_else(|| {
                    StorageError::InvalidOperation(
                        "complete native mutation exceeds 128 MiB".into(),
                    )
                })?;
        }
        let additional = count - data.operations.len();
        data.operations
            .try_reserve(additional)
            .map_err(|_| StorageError::BackendError("native mutation allocation refused".into()))?;
        for operation in operations {
            let at = data.operations.len();
            data.latest
                .insert((operation.collection.clone(), operation.key.clone()), at);
            data.operations.push(Arc::new(PendingRecord {
                collection: operation.collection,
                key: operation.key,
                body: operation.body,
            }));
        }
        data.bytes = bytes;
        Ok(())
    }

    fn take_operations(&self) -> Vec<HomeMutation> {
        let operations = {
            let mut data = self.data.lock().expect("native pending batch");
            data.latest.clear();
            data.bytes = 0;
            std::mem::take(&mut data.operations)
        };
        operations
            .into_iter()
            .map(|record| {
                let record = Arc::try_unwrap(record).unwrap_or_else(|shared| PendingRecord {
                    collection: shared.collection.clone(),
                    key: shared.key.clone(),
                    body: shared.body.clone(),
                });
                HomeMutation {
                    collection: record.collection,
                    key: record.key,
                    body: record.body,
                }
            })
            .collect()
    }
}

pub(super) fn validate(operations: &[HomeMutation]) -> StorageResult<()> {
    if operations.len() > 4096 {
        return Err(StorageError::InvalidOperation(
            "complete native batch exceeds 4096 operations".into(),
        ));
    }
    let mut bytes = 256u64;
    for operation in operations {
        if operation.collection.len() > 65531 || operation.key.len() > 65535 {
            return Err(StorageError::InvalidOperation(
                "native collection or key exceeds format bounds".into(),
            ));
        }
        bytes = bytes
            .checked_add(
                (160 + operation.collection.len()
                    + 2 * operation.key.len()
                    + operation.body.as_ref().map_or(0, Vec::len)) as u64,
            )
            .filter(|bytes| *bytes <= 128 * 1024 * 1024)
            .ok_or_else(|| {
                StorageError::InvalidOperation("complete native batch exceeds 128 MiB".into())
            })?;
    }
    Ok(())
}

/// Keep one complete logical job in one storage batch. Commit precedes dirty clear.
pub(crate) async fn scope<T, E>(
    runtime: Option<Arc<PackRuntime>>,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<StorageError>,
{
    let Some(runtime) = runtime else {
        return future.await;
    };
    if PendingBatch::current(&runtime).is_some() {
        return future.await;
    }
    let gate = Arc::clone(&runtime.write_gate).lock_owned().await;
    let pending = Arc::new(PendingBatch {
        identity: runtime.home.descriptor().identity,
        data: Mutex::new(PendingData::default()),
        publications: publication::PendingPublications::default(),
    });
    let result = read::write_scope(
        Arc::clone(&runtime),
        WRITE.scope(Arc::clone(&pending), future),
    )
    .await??;
    let operations = pending.take_operations();
    // Once submitted, cancellation must not release the exclusive gate or drop
    // cache publication while the blocking disk commit continues independently.
    super::super::laststore::LastStoreKvStore::run_blocking(move || {
        let _gate = gate;
        if !operations.is_empty() {
            runtime.commit(operations)?;
        }
        if let Err(error) = pending.publications.finish() {
            runtime.failed.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    })
    .await?;
    Ok(result)
}

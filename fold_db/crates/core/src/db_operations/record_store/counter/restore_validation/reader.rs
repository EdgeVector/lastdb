//! Retain the logical encryption seam without borrowing a snapshot caller.

use crate::storage::disk_pack::{PackReadScope, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::{KvStore, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;

struct Reader {
    runtime: Arc<PackRuntime>,
    main: Arc<dyn KvStore>,
    cas: Arc<dyn KvStore>,
}

pub(super) async fn open(
    store: &dyn NamespacedStore,
    runtime: Arc<PackRuntime>,
    main: Arc<dyn KvStore>,
) -> StorageResult<Arc<dyn NamespacedStore>> {
    // A read scope opens an absent CAS namespace without registering it.
    // Validation must never repair a missing namespace as a side effect.
    let scope = PackReadScope::for_runtime(&runtime).await?;
    let cas = scope.run_result(store.open_namespace("cas_blobs")).await?;
    Ok(Arc::new(Reader { runtime, main, cas }))
}

#[async_trait]
impl NamespacedStore for Reader {
    fn disk_pack_runtime(&self) -> Option<Arc<PackRuntime>> {
        Some(Arc::clone(&self.runtime))
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        match name {
            "main" => Ok(Arc::clone(&self.main)),
            "cas_blobs" => Ok(Arc::clone(&self.cas)),
            _ => Err(StorageError::InvalidOperation(
                "restore reader namespace is unsupported".into(),
            )),
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        Ok(vec!["main".into(), "cas_blobs".into()])
    }

    async fn delete_namespace(&self, _name: &str) -> StorageResult<bool> {
        Err(StorageError::InvalidOperation(
            "restore reader cannot delete a namespace".into(),
        ))
    }
}

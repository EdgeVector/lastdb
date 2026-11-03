//! Retain the logical encryption seam without borrowing a snapshot caller.

use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::{KvStore, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;

struct Reader {
    runtime: Arc<PackRuntime>,
    main: Arc<dyn KvStore>,
}

/// Validation reads `main` only. It opens no other
/// namespace, so it never repairs a missing one as a side effect.
pub(super) fn open(runtime: Arc<PackRuntime>, main: Arc<dyn KvStore>) -> Arc<dyn NamespacedStore> {
    Arc::new(Reader { runtime, main })
}

#[async_trait]
impl NamespacedStore for Reader {
    fn disk_pack_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        match name {
            "main" => Ok(Arc::clone(&self.main)),
            _ => Err(StorageError::InvalidOperation(
                "restore reader namespace is unsupported".into(),
            )),
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        Ok(vec!["main".into()])
    }

    async fn delete_namespace(&self, _name: &str) -> StorageResult<bool> {
        Err(StorageError::InvalidOperation(
            "restore reader cannot delete a namespace".into(),
        ))
    }
}

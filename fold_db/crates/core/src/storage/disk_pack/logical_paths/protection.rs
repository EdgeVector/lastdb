//! Reserve the prepared core memory definitions before the complete
//! publication boundary.

use super::*;
use crate::resident::{CoreModelUpdate, PreparedCoreMemory};

pub(crate) struct PathProtection {
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    pending: Mutex<Option<CoreModelUpdate>>,
}

pub(crate) struct PathPublication {
    pub definitions: Vec<PreparedCoreMemory>,
    pub owner: Option<Arc<crate::memory_budget::DeferReservation>>,
}

impl PathProtection {
    pub(crate) fn reserve(
        runtime: &PackRuntime,
        publication: PathPublication,
    ) -> StorageResult<Arc<Self>> {
        let update = runtime
            .core
            .lock()
            .expect("core write reservation")
            .prepare_clean(publication.definitions, publication.owner.as_ref())
            .map_err(|_| capacity())?;
        Ok(Arc::new(Self {
            core: Arc::clone(&runtime.core),
            pending: Mutex::new(Some(update)),
        }))
    }

    /// The caller retains the view write lock through the complete typed change.
    pub(crate) fn publish_clean(&self) -> StorageResult<()> {
        let Some(update) = self.pending.lock().expect("core prepared paths").take() else {
            return Ok(());
        };
        self.core
            .lock()
            .expect("core durable publication")
            .install_clean(&update);
        Ok(())
    }
}

fn capacity() -> StorageError {
    StorageError::WriteResourceLimit {
        detail: "native mutation cannot reserve its complete logical core paths".into(),
    }
}

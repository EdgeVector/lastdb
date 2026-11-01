//! Forward and reverse stores for derived-field-key lineage.
//!
//! Open creates both namespaces. Shutdown flushes them. No caller writes
//! or reads the index.

use crate::schema::SchemaError;
use crate::storage::traits::KvStore;
use std::sync::Arc;

/// Two-way index from derived field keys to their source field keys, and back.
#[derive(Clone)]
pub struct LineageIndex {
    forward: Arc<dyn KvStore>,
    reverse: Arc<dyn KvStore>,
}

impl LineageIndex {
    pub(crate) fn new(forward: Arc<dyn KvStore>, reverse: Arc<dyn KvStore>) -> Self {
        Self { forward, reverse }
    }

    /// Flush both lineage namespaces to disk.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.forward.flush().await?;
        self.reverse.flush().await?;
        Ok(())
    }
}

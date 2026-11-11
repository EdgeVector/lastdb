use crate::storage::StorageError;

use super::FoldDB;

impl FoldDB {
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        tracing::info!(
            target: "fold_node::database",
            "Shutting down FoldDB: flushing storage"
        );
        self.flush().await
    }

    /// Flushes local storage to ensure all data is persisted
    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db_ops
            .flush()
            .await
            .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
    }
}

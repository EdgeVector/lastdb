//! Consolidated benchmarks utilities for database setup and common test patterns
//!
//! This module eliminates duplicate database setup code found across 11+ files

use crate::db_operations::DbOperations;
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::NamespacedStore;
use std::sync::Arc;

/// Consolidated temporary database creation - eliminates 11+ duplicates
pub struct BenchmarkDatabaseFactory;

impl BenchmarkDatabaseFactory {
    /// Create temporary native DbOperations on a leaked temp dir.
    pub async fn create_temp_db_ops() -> Result<DbOperations, Box<dyn std::error::Error>> {
        let dir = tempfile::TempDir::new()?.keep();
        let store = Arc::new(PackNamespacedStore::open(&dir)?) as Arc<dyn NamespacedStore>;
        Ok(DbOperations::from_namespaced_store(store).await?)
    }
}

//! Consolidated benchmarks utilities for database setup and common test patterns
//!
//! This module eliminates duplicate database setup code found across 11+ files

use crate::db_operations::DbOperations;
use crate::storage::{LastStoreNamespacedStore, NamespacedStore};
use std::sync::Arc;

/// Consolidated temporary database creation - eliminates 11+ duplicates
pub struct BenchmarkDatabaseFactory;

impl BenchmarkDatabaseFactory {
    /// Create temporary DbOperations backed by Last Store on a leaked temp dir.
    pub async fn create_temp_db_ops() -> Result<DbOperations, Box<dyn std::error::Error>> {
        let dir = tempfile::TempDir::new()?.keep();
        let store = Arc::new(LastStoreNamespacedStore::open(&dir)?) as Arc<dyn NamespacedStore>;
        Ok(DbOperations::from_namespaced_store(store).await?)
    }
}

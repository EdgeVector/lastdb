//! Consolidated testing utilities for database setup and common test patterns
//!
//! This module eliminates duplicate database setup code found across 11+ files

use crate::db_operations::DbOperations;
use crate::storage::{InMemoryNamespacedStore, LastStoreNamespacedStore, NamespacedStore};
use std::sync::Arc;

/// Consolidated temporary database creation - eliminates 11+ duplicates
pub struct TestDatabaseFactory;

impl TestDatabaseFactory {
    /// Create temporary DbOperations backed by Last Store on a leaked temp dir.
    pub async fn create_temp_db_ops() -> Result<DbOperations, Box<dyn std::error::Error>> {
        let dir = tempfile::TempDir::new()?.keep();
        let store = Arc::new(LastStoreNamespacedStore::open(&dir)?) as Arc<dyn NamespacedStore>;
        Ok(DbOperations::from_namespaced_store(store).await?)
    }

    /// In-memory DbOperations for pure unit tests (no disk).
    pub async fn create_inmemory_db_ops() -> Result<DbOperations, Box<dyn std::error::Error>> {
        let store = Arc::new(InMemoryNamespacedStore::new()) as Arc<dyn NamespacedStore>;
        Ok(DbOperations::from_namespaced_store(store).await?)
    }

    /// Create shared DbOperations for tests that need an `Arc`
    pub async fn create_test_environment() -> Result<Arc<DbOperations>, Box<dyn std::error::Error>>
    {
        Ok(Arc::new(Self::create_temp_db_ops().await?))
    }
}

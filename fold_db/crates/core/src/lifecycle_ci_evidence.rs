//! One-time generated-data CI support. This module must never merge.

use crate::db_operations::DbOperations;
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::schema::types::KeyValue;
use crate::security::Ed25519KeyPair;
use crate::storage::{traits::NamespacedStore, StorageError};
use crate::FoldDB;
use std::sync::Arc;

/// Open the exact supplied physical home with a stable generated signer.
pub async fn open(
    store: Arc<dyn NamespacedStore>,
    signer: Arc<Ed25519KeyPair>,
) -> Result<FoldDB, StorageError> {
    FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(DbOperations::from_namespaced_store(store).await?),
        signer,
        search_outbox_inbox: None,
        mutation_log_capture: None,
        offline_maintenance: true,
    })
    .await
}

/// Read actual current tip clocks, then deliberately replace the age rows.
/// This changes generated retention metadata only, never the product tips.
pub async fn inject_wrong_ages(
    db: &FoldDB,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, crate::schema::SchemaError> {
    db.db_ops()
        .atoms()
        .in_background_scope(async {
            let clocks = crate::fold_db_core::purge::retention_tip_clocks(
                db.db_ops(),
                &db.schema_manager(),
                name,
                keys,
            )
            .await?;
            let wrong: Vec<_> = clocks
                .iter()
                .map(|(key, age)| (key.clone(), age + 100_000))
                .collect();
            db.db_ops()
                .schemas()
                .record_schema_retention_write_ages(name, &wrong)
                .await?;
            Ok(clocks)
        })
        .await
}

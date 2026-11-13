//! FoldDB core database engine, typed record stores, schemas, and storage.

pub mod crypto;
pub mod db_operations;
pub mod error;
pub mod fold_db_core;
pub mod resident;
pub mod schema;
pub mod storage;

pub use crate::crypto::{clock, env_flag, security};
pub use crate::db_operations::alternate_keys;
pub use crate::error::{access, content_limit, FoldDbError, FoldDbResult};
pub use crate::fold_db_core::FoldDB;
pub use crate::schema::types::operations::MutationType;
pub use crate::schema::SchemaState;
pub use lastdb_model::{
    canonical, hex, portable_mutation, record_replacement, record_value, storage_policy,
};
pub use storage::{cloud_backup_plan, DatabaseConfig};

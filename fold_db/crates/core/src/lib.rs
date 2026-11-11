//! FoldDB core database engine, typed record stores, schemas, and storage.

pub mod access;
pub mod alternate_keys;
pub mod clock;
pub mod content_limit;
pub mod crypto;
pub mod db_operations;
pub mod env_flag;
pub mod error;
pub mod fold_db_core;
pub mod resident;
pub mod schema;
pub mod security;
pub mod storage;

pub use error::{FoldDbError, FoldDbResult};
pub use fold_db_core::FoldDB;
pub use lastdb_model::{
    canonical, hex, portable_mutation, portable_replay, record_replacement, record_value,
    storage_policy,
};
pub use schema::types::operations::MutationType;
pub use schema::SchemaState;
pub use storage::{cloud_backup_plan, DatabaseConfig};

//! FoldDB core database engine, typed record stores, schemas, and storage.

pub mod access;
pub mod crypto;
pub mod db_operations;
pub mod fold_db_core;
pub mod limits;
pub mod platform;
pub mod resident;
pub mod schema;
pub mod storage;

pub use crate::access::error;
pub use crate::crypto::security;
pub use crate::db_operations::alternate_keys;
pub use crate::limits::content_limit;
pub use crate::platform::{clock, env_flag};
pub use lastdb_model::{canonical, hex, record_replacement, record_value, storage_policy};

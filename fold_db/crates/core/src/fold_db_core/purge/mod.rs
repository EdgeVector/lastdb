//! Erase sizing, key rendering, and retention ages.
//!
//! A user Delete and a physical Purge merge into their records in the native
//! request (`mutation_manager::write::records`). What stays here serves the
//! TTL sweep and the retention backfill: the size bound of one erase request
//! and the write time of each record.

use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{SchemaCore, SchemaError};

pub mod budget;
mod helpers;
mod retention_backfill;

pub(crate) use retention_backfill::retention_tip_clocks;

pub(super) use helpers::describe_key;

//! Owner-facing DB inventory, storage reports and logical repair.

use crate::db_operations::{
    RecordDeleteLedgerEntry, SchemaRecordKey, SchemaRecordKeysReport, SchemaStorageReport,
};
use crate::error::FoldDbError;

use std::collections::HashMap;

use super::FoldDB;

mod repair;
mod retention_backfill;
mod schema_keys;
mod storage_reports;

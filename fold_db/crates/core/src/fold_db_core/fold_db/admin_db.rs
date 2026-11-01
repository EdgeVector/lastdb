//! Owner-facing DB inventory, storage reports and logical repair.

use crate::db_operations::{
    AtomDeleteLedgerEntry, DbInventory, SchemaRecordKey, SchemaRecordKeysReport,
    SchemaStorageReport, LIVE_ATTRIBUTION_EPOCH_ID,
};
use crate::error::FoldDbError;

use std::collections::HashMap;

use crate::access::AccessContext;

use super::FoldDB;

mod counters;
mod error;
mod repair;
mod retention_backfill;
mod schema_keys;
mod storage_reports;

use error::admin_op_error;

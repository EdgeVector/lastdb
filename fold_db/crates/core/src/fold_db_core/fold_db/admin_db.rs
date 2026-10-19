//! Owner-facing DB admin: inventory + history clear + atom GC + photo blob migrate.

use crate::db_operations::{
    AtomDeleteLedgerEntry, AtomGcReport, DbInventory, DroppedSchemaReapCursor, HistoryClearReport,
    LegacyKeyForkAudit, LegacyRefBlobPurgeReport, OrderLogAudit, ProteinGcReport,
    SchemaCurrentStorageReport, SchemaLogicalStorageReport, SchemaLogicalStorageRow,
    SchemaRecordKey, SchemaRecordKeysReport, SchemaStorageReport,
    SupersededVersionRetentionCheckpoint, SupersededVersionRetentionOptions,
    SupersededVersionRetentionReport, ThinTipMigrateReport, TipHistoryDrainCheckpoint,
    TipHistoryDrainOptions, TipHistoryDrainReport, TombstoneFlagBackfillReport,
    LIVE_ATTRIBUTION_EPOCH_ID,
};
use crate::error::FoldDbError;
use crate::hex::hex_lower;
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::access::AccessContext;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::operations::{Mutation, MutationType, Query};
use base64::Engine;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::FoldDB;

mod atom_gc;
mod error;
mod maintenance;
mod order_log;
mod photo_blobs;
mod repair;
mod retention;
mod storage_reports;
mod tombstone_admin;

use error::admin_op_error;

/// Report from migrating Photo `file_bytes` into the local CAS blob tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhotoBlobMigrateReport {
    pub dry_run: bool,
    pub photos_seen: u64,
    pub photos_migrated: u64,
    pub photos_skipped: u64,
    pub photos_failed: u64,
    pub bytes_to_cas: u64,
    pub errors: Vec<String>,
}

fn field_molecule_uuids(schema: &Schema) -> Vec<String> {
    let mut mols = Vec::new();
    for field in schema.runtime_fields.values() {
        if let Some(u) = field.common().molecule_uuid() {
            mols.push(u.clone());
        }
    }
    mols.sort();
    mols.dedup();
    mols
}

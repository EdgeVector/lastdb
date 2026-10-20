//! Owner-facing DB inventory, history, GC and logical repair.

use crate::db_operations::{
    AtomDeleteLedgerEntry, AtomGcReport, DanglingTipRepairOptions, DanglingTipRepairReport,
    DbInventory, DroppedSchemaReapCursor, HistoryClearReport, ProteinGcReport,
    SchemaCurrentStorageReport, SchemaLogicalStorageReport, SchemaLogicalStorageRow,
    SchemaRecordKey, SchemaRecordKeysReport, SchemaStorageReport, LIVE_ATTRIBUTION_EPOCH_ID,
};
use crate::error::FoldDbError;

use crate::schema::types::Schema;

use std::collections::HashMap;

use crate::access::AccessContext;

use super::FoldDB;

mod atom_gc;
mod error;
mod maintenance;
mod repair;
mod retention;
mod schema_keys;
mod storage_reports;

use error::admin_op_error;

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

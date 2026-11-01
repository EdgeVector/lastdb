//! Native logical paths retain the actual shared core molecule and atom objects.

use super::*;
use crate::atom::{AtomEntry, KeyMetadata};
use crate::resident::{CorePath, CoreWarmSlot};
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};

pub(crate) mod absence;
pub(super) mod body;
pub(super) mod controls;
pub(crate) mod core_filter;
pub(crate) mod core_read;
mod dependencies;
pub(crate) mod field;
mod protection;
mod read;
pub(crate) mod source;
mod tips;

pub(crate) use protection::{PathProtection, PathPublication};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LogicalPath {
    pub scope: Option<String>,
    pub schema: String,
    pub field: String,
    pub molecule: String,
    pub hash: String,
    pub range: String,
    pub atom: String,
}

impl LogicalPath {
    pub(super) fn core(&self) -> CorePath {
        CorePath {
            scope: self.scope.clone(),
            schema: self.schema.clone(),
            field: self.field.clone(),
            molecule: self.molecule.clone(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    pub(crate) fn id(&self) -> StorageResult<String> {
        if self.schema.is_empty() || self.field.is_empty() || self.molecule.is_empty() {
            return Err(StorageError::InvalidOperation(
                "logical path identity is incomplete".into(),
            ));
        }
        serde_json::to_string(&(
            &self.scope,
            &self.schema,
            &self.field,
            &self.molecule,
            &self.hash,
            &self.range,
        ))
        .map_err(|error| serialization(&error))
    }
}

pub(crate) fn schema_identity(schema: &Schema) -> StorageResult<String> {
    serde_json::to_string(&(&schema.name, &schema.identity_hash))
        .map_err(|error| serialization(&error))
}

/// The published view, the call receipt, and the core for one matching revision.
/// A newer view returns no guard and does not take the receipt or the core.
/// The core drops before the receipt, and the receipt drops before the view.
pub(super) fn lock_current_receipt<'a>(
    runtime: &'a PackRuntime,
    context: &'a super::read::ReadContext,
    view_label: &str,
    core_label: &str,
) -> Option<CurrentReceipt<'a>> {
    let view = runtime.view.read().expect(view_label);
    if view.revision != context.revision {
        return None;
    }
    let call = crate::read_receipts::lock(&context.call);
    let core = runtime.core.lock().expect(core_label);
    Some(CurrentReceipt {
        core,
        call,
        _view: view,
    })
}

pub(super) struct CurrentReceipt<'a> {
    pub(super) core: std::sync::MutexGuard<'a, crate::resident::CoreWarmSet>,
    pub(super) call: std::sync::MutexGuard<'a, super::physical::PhysicalRead>,
    _view: std::sync::RwLockReadGuard<'a, super::view::NativeReadView>,
}

/// The published view and the core for one matching revision.
/// A newer view returns no guard and does not take the core lock.
/// The core drops before the view. This path does not lock a call receipt.
pub(super) fn lock_current_model<'a>(
    runtime: &'a PackRuntime,
    context: &'a super::read::ReadContext,
    view_label: &str,
    core_label: &str,
) -> Option<CurrentModel<'a>> {
    let view = runtime.view.read().expect(view_label);
    if view.revision != context.revision {
        return None;
    }
    let core = runtime.core.lock().expect(core_label);
    Some(CurrentModel { core, _view: view })
}

pub(super) struct CurrentModel<'a> {
    pub(super) core: std::sync::MutexGuard<'a, crate::resident::CoreWarmSet>,
    _view: std::sync::RwLockReadGuard<'a, super::view::NativeReadView>,
}

fn serialization(error: &serde_json::Error) -> StorageError {
    StorageError::BackendError(format!("logical core path encoding: {error}"))
}

//! Live DB admin: the inventory (byte breakdown) and the storage reports.
//!
//! These run against the **running** store (no exclusive offline open). They are
//! the product surface for "what's inside my DB?".

use super::storage_breakdown::{SchemaStorage, StorageBreakdown};
use super::RecordStore;

use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

mod report_inventory;
pub use report_inventory::*;
mod report_field_keys;
pub use report_field_keys::*;
mod inventory;
mod inventory_key_classes;

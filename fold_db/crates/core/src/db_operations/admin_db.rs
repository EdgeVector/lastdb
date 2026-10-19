//! Live DB admin: inventory (byte breakdown) + mutation-history clear.
//!
//! These run against the **running** store (no exclusive offline open). They are
//! the product surface for "what's inside my DB?" and "trim long history".

use super::storage_breakdown::{SchemaStorage, StorageBreakdown};
use super::AtomStore;
use crate::atom::{molecule_key_codec, MutationEvent};
use crate::kind_partition;
use crate::protein::{Protein, MEMBER_BACKREF_PREFIX, PROTEIN_RECORD_PREFIX};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Colon form, NUL-plane start, exclusive end covering `moc\0` writes and leftover `moc:`.
fn moc_plane(storage_prefix: Option<&str>) -> (String, String, String) {
    let colon = build_storage_key(storage_prefix, molecule_key_codec::MOC_PREFIX);
    let (start, end) = kind_partition::colon_plane_bounds(&colon);
    (colon, start, end)
}

fn moc_molecule_from_key(key: &str) -> Option<String> {
    kind_partition::rest_of(key, "moc")
        .filter(|rest| !rest.is_empty())
        .map(str::to_owned)
}

fn cursor_is_moc_plane(key: &str) -> bool {
    kind_partition::rest_of(key, "moc").is_some()
}

mod report_inventory;
pub use report_inventory::*;
mod report_molecule_keys;
pub use report_molecule_keys::*;
mod report_order_log;
pub use report_order_log::*;
mod history_prune;
mod inventory;
mod inventory_key_classes;
mod inventory_molecule_keys;
mod inventory_tip_format;
mod order_log;
mod orphan_proteins;
mod tombstones;

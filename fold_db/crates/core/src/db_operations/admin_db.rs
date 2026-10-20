//! Live DB admin: inventory (byte breakdown) + mutation-history clear.
//!
//! These run against the **running** store (no exclusive offline open). They are
//! the product surface for "what's inside my DB?" and "trim long history".

use super::storage_breakdown::{SchemaStorage, StorageBreakdown};
use super::AtomStore;
use crate::atom::{molecule_key_codec, MutationEvent};

use crate::protein::{Protein, MEMBER_BACKREF_PREFIX, PROTEIN_RECORD_PREFIX};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

mod report_inventory;
pub use report_inventory::*;
mod report_molecule_keys;
pub use report_molecule_keys::*;
mod history_prune;
mod inventory;
mod inventory_key_classes;
mod inventory_molecule_keys;
mod inventory_molecules;
pub use inventory_molecules::MoleculeCounterTotals;
mod orphan_proteins;

//! Barrierless immutable molecule generations.

use crate::atom::{incoming_wins_lww, molecule_key_codec};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;
#[cfg(feature = "sharing")]
use serde_json::Value;
#[cfg(feature = "sharing")]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};
#[cfg(feature = "sharing")]
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::{
    AtomStore, MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot,
    PerKeyRecord,
};

mod activate;
mod deletes;
mod merge;
mod paging;
mod slots;

/// A read-only cut captured before a complete generation snapshot is built.
/// Ordinary writes continue after this value is created.
#[cfg(feature = "sharing")]
pub(crate) struct PreparedMoleculeGeneration {
    generation: String,
    observed: HashMap<(String, String), PerKeyRecord>,
    previous_generation: Option<String>,
    reclaim_generation: Option<String>,
}

/// One complete immutable body and the rows that select it.
#[cfg(feature = "sharing")]
pub(crate) struct PreparedMoleculeGenerationActivation {
    pub(crate) molecule_uuid: String,
    pub(crate) prepared: PreparedMoleculeGeneration,
    pub(crate) base_records: Vec<(String, PerKeyRecord)>,
    pub(crate) activation_items: Vec<(String, Value)>,
}

#[cfg(feature = "sharing")]
struct ReadyMoleculeGenerationActivation {
    molecule_uuid: String,
    prepared: PreparedMoleculeGeneration,
    generation: String,
    stale_direct: Vec<((String, String), PerKeyRecord)>,
    activation_items: Vec<(String, Value)>,
}

fn incoming_record_wins(incoming: &PerKeyRecord, current: &PerKeyRecord) -> bool {
    incoming_wins_lww(incoming.entry.lww_key(), current.entry.lww_key())
}

pub(crate) const MATERIALIZE_PAGE_ROWS: usize = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaterializeReadCost {
    pub(crate) page_count: u64,
    pub(crate) row_count: u64,
    pub(crate) value_bytes: u64,
    pub(crate) max_page_rows: u64,
}

impl MaterializeReadCost {
    fn record<T>(&mut self, page: &[(Vec<u8>, T)], value_bytes: u64) {
        self.page_count = self.page_count.saturating_add(1);
        self.row_count = self.row_count.saturating_add(page.len() as u64);
        self.value_bytes = self.value_bytes.saturating_add(value_bytes);
        self.max_page_rows = self.max_page_rows.max(page.len() as u64);
    }

    pub(super) fn merge(&mut self, other: Self) {
        self.page_count = self.page_count.saturating_add(other.page_count);
        self.row_count = self.row_count.saturating_add(other.row_count);
        self.value_bytes = self.value_bytes.saturating_add(other.value_bytes);
        self.max_page_rows = self.max_page_rows.max(other.max_page_rows);
    }
}

pub(super) struct PagedRows<T> {
    pub(super) rows: Vec<(String, T)>,
    pub(super) cost: MaterializeReadCost,
}

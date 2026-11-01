//! Barrierless immutable molecule generations.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, HashMap};

use super::super::{
    AtomStore, MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot,
    PerKeyRecord,
};

mod deletes;
mod merge;
mod paging;
mod slots;

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

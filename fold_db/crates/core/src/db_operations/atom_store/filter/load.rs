//! Filtered molecule / 1D / hash-range loads.

use crate::atom::{molecule_key_codec, AtomEntry, KeyMetadata, MoleculeHashRange};
use crate::schema::SchemaError;

use super::super::types::{
    FilterLayout, MoleculeData, MoleculeHeader, MoleculeRangeForm, OneDSlot, PerKeyRecord,
    EMPTY_KEY_COMPONENT,
};
use super::super::AtomStore;
use super::PageFill;

mod exact;
mod hash_range;
mod hash_range_pages;
mod hash_range_scan;
mod one_d;

impl AtomStore {
    /// [`Self::load_filtered_molecule`] for a query read, which knows only
    /// whether it will show tombstones.
    ///
    /// This is the boundary that keeps [`PageFill`] inside the atom store: a
    /// read that hides tombstones needs its page window measured in live rows,
    /// and nothing above this layer should have to know that.
    pub(crate) async fn load_filtered_molecule_for_read(
        &self,
        molecule_uuid: &str,
        layout: FilterLayout,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        include_tombstones: bool,
    ) -> Result<Option<(MoleculeData, MoleculeRangeForm)>, SchemaError> {
        let fill = if include_tombstones {
            PageFill::StoredRows
        } else {
            PageFill::LiveRows
        };
        let data = self
            .load_filtered_molecule_filled(molecule_uuid, layout, storage_prefix, filter, fill)
            .await?;
        // These composite producers echo exact API pairs. Every other producer
        // leaves ranges in storage form. Never infer the form from their bytes.
        let ranges = if layout == FilterLayout::Composite
            && matches!(
                filter,
                crate::schema::types::field::HashRangeFilter::HashRangeKey { .. }
                    | crate::schema::types::field::HashRangeFilter::HashRangeKeys(_)
            ) {
            MoleculeRangeForm::Api
        } else {
            MoleculeRangeForm::Storage
        };
        Ok(data.map(|data| (data, ranges)))
    }

    /// [`Self::load_filtered_molecule`], with the caller stating whether a page
    /// window counts stored rows or live ones.
    ///
    /// A read that hides tombstones must pass [`PageFill::LiveRows`]: bounding
    /// the fetch on stored rows and filtering afterwards is what made a page of
    /// 100 arrive as 23 with no error.
    pub(crate) async fn load_filtered_molecule_filled(
        &self,
        molecule_uuid: &str,
        layout: FilterLayout,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        // Exact queries still check their physical candidates without a header;
        // only that complete winner search can admit a missing logical key.
        let Some((header, storage_prefix)) = self
            .read_header_exact_prefix(molecule_uuid, storage_prefix)
            .await?
        else {
            return self
                .load_exact_query_without_header(
                    molecule_uuid,
                    layout,
                    storage_prefix,
                    filter,
                    fill,
                )
                .await;
        };
        match layout {
            FilterLayout::OneD(slot) => {
                self.load_filtered_1d(molecule_uuid, &header, storage_prefix, filter, slot, fill)
                    .await
            }
            FilterLayout::Composite => {
                self.load_filtered_hash_range(molecule_uuid, &header, storage_prefix, filter, fill)
                    .await
            }
        }
    }
}

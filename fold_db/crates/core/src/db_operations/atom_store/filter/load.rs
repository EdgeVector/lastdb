//! Filtered molecule / 1D / hash-range loads.

use crate::atom::{molecule_key_codec, AtomEntry, KeyMetadata, MoleculeHashRange};
use crate::schema::SchemaError;

use super::super::types::{
    FilterLayout, MoleculeData, MoleculeHeader, OneDSlot, PerKeyRecord, EMPTY_KEY_COMPONENT,
};
use super::super::AtomStore;
use super::PageFill;

mod hash_range;
mod legacy_fork;
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
    ) -> Result<Option<MoleculeData>, SchemaError> {
        let fill = if include_tombstones {
            PageFill::StoredRows
        } else {
            PageFill::LiveRows
        };
        self.load_filtered_molecule_filled(molecule_uuid, layout, storage_prefix, filter, fill)
            .await
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
        // The header is the per-key-layout marker. No header → absent molecule.
        let Some((header, storage_prefix)) = self
            .read_header_exact_prefix(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(None);
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

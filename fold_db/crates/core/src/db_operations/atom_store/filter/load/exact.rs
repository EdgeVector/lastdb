//! A missing definition does not replace an exact search of current keys.

use super::*;

impl AtomStore {
    pub(super) async fn load_exact_query_without_header(
        &self,
        molecule: &str,
        layout: FilterLayout,
        scope: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

        let exact = match layout {
            FilterLayout::OneD(OneDSlot::Hash) => matches!(
                filter,
                F::HashKey(_) | F::HashRangeKey { .. } | F::HashRangeKeys(_)
            ),
            FilterLayout::Composite => {
                matches!(filter, F::HashRangeKey { .. } | F::HashRangeKeys(_))
            }
            _ => false,
        };
        if !exact {
            return Ok(None);
        }
        // This transient definition only wraps the exact resolver's result.
        // It is never written or used as evidence that any key is absent.
        let header = MoleculeHeader::new(0, chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
        match layout {
            FilterLayout::OneD(slot) => {
                self.load_filtered_1d(molecule, &header, scope, filter, slot, fill)
                    .await
            }
            FilterLayout::Composite => {
                self.load_filtered_hash_range(molecule, &header, scope, filter, fill)
                    .await
            }
        }
    }
}

//! Read locations already selected by an ordered cursor without another lookup.

use super::{Location, ReadCall};
use crate::Result;

impl ReadCall {
    pub(super) fn read_selected_records(
        &mut self,
        collection: &str,
        selected: &[(String, Location)],
    ) -> Result<Vec<Vec<u8>>> {
        let bytes = selected
            .len()
            .checked_mul(
                std::mem::size_of::<Result<Option<Location>>>() + std::mem::size_of::<Vec<u8>>(),
            )
            .ok_or_else(super::resource)?;
        self.charge(bytes as u64)?;
        let mut locations = Vec::new();
        let mut bodies = Vec::new();
        locations
            .try_reserve_exact(selected.len())
            .map_err(|_| super::resource())?;
        bodies
            .try_reserve_exact(selected.len())
            .map_err(|_| super::resource())?;
        locations.extend(
            selected
                .iter()
                .map(|(_, location)| Ok(Some(location.clone()))),
        );
        let (plans, mut failures) = self.plan_batch_blocks(&mut locations)?;
        self.read_planned_blocks(plans, &mut failures);
        for ((key, _), location) in selected.iter().zip(locations) {
            let location = location?.ok_or_else(|| super::corrupt("selected location absent"))?;
            bodies.push(self.finish_batch_record(collection, key, &location, &failures)?);
        }
        Ok(bodies)
    }
}

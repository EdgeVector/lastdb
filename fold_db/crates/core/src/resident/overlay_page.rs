//! Bounded resident overlay pages, including pending hard deletes.
use super::*;

pub(crate) struct ResidentOverlayEntry {
    pub range: String,
    pub tip: Option<ResidentTip>,
    pub deleted: bool,
}

impl ResidentGraph {
    /// A missing clean tip advances the cursor without hiding a durable row.
    pub(crate) fn partition_overlay_page(
        &self,
        molecule: &str,
        hash: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<ResidentOverlayEntry> {
        let start = ResidentMoleculeKey::new(hash, after.unwrap_or(""));
        let end = ResidentMoleculeKey::new(format!("{hash}\0"), "");
        let lower = if after.is_some() {
            Bound::Excluded(&start)
        } else {
            Bound::Included(&start)
        };
        let mut selected = BTreeMap::new();
        if let Some(keys) = self.key_index.read().expect("key index lock").get(molecule) {
            selected.extend(
                keys.range((lower, Bound::Excluded(&end)))
                    .take(limit)
                    .map(|key| (key.clone(), false)),
            );
        }
        if let Some(keys) = self
            .key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(molecule)
        {
            selected.extend(
                keys.range((lower, Bound::Excluded(&end)))
                    .take(limit)
                    .map(|(key, _)| (key.clone(), true)),
            );
        }
        selected
            .into_iter()
            .take(limit)
            .map(|(key, deleted)| ResidentOverlayEntry {
                tip: if deleted {
                    None
                } else {
                    self.resolve_tip(molecule, hash, &key.range)
                        .map(|tip| tip.value)
                },
                range: key.range,
                deleted,
            })
            .collect()
    }
}

//! Dependent selectors reuse decoded blocks and skip bodies of found counters.

use super::*;

pub(in crate::disk_run) enum Selected {
    Record(Found),
    Presence(bool),
}

impl RunReader {
    /// A presence selector can depend on a primary selector in this same batch.
    /// Counter-first order avoids a body read when its counter is in this run.
    pub(in crate::disk_run) fn select_many(
        &self,
        keys: &[&[u8]],
        presence: &[bool],
        parents: &[Option<usize>],
        scratch: &mut DecodeScratch,
    ) -> Result<Vec<Option<Selected>>> {
        let mut order: Vec<_> = (0..keys.len()).collect();
        order.sort_by(|&a, &b| {
            keys[parents[a].unwrap_or(a)]
                .cmp(keys[parents[b].unwrap_or(b)])
                .then_with(|| presence[a].cmp(&presence[b]))
        });
        let mut out: Vec<_> = keys.iter().map(|_| None).collect();
        let mut index: Option<(usize, IndexBlock)> = None;
        let mut data: Option<(u64, BlockRecords)> = None;
        for slot in order {
            if parents[slot].is_some_and(|parent| {
                matches!(&out[parent], Some(Selected::Record(Found::Value(_))))
            }) {
                continue;
            }
            let key = keys[slot];
            let Some(number) = self.index_for(key) else {
                continue;
            };
            if index.as_ref().map(|(n, _)| *n) != Some(number) {
                index = Some((number, self.read_index_with(number, Some(&mut *scratch))?));
            }
            let (_, block_index) = index.as_ref().expect("index loaded");
            if !block_index.may_hold(key_hash(key))? {
                continue;
            }
            let Some(block) = block_for(block_index, key) else {
                continue;
            };
            if data.as_ref().map(|(n, _)| *n) != Some(block.number) {
                data = Some((
                    block.number,
                    self.read_data_with(block, Some(&mut *scratch))?,
                ));
            }
            let (_, records) = data.as_ref().expect("block loaded");
            out[slot] = find(records, key, |found| {
                if presence[slot] {
                    Selected::Presence(matches!(found, Found::Value(_)))
                } else {
                    Selected::Record(found.clone())
                }
            });
        }
        Ok(out)
    }
}

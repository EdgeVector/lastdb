//! Exact value lengths from live locations, with cold-open memory admission.

use super::*;
use crate::read_receipt_io::lock as read_lock;

#[derive(Clone, Copy)]
/// Exact stored-body size and conservative cold-open requirements.
pub struct PointValueSize {
    /// Encoded body length from the authoritative location.
    pub bytes: u64,
    /// The outer codec can inflate, or its header needs conservative handling.
    pub may_inflate: bool,
    /// Allocation allowance needed before opening this physical group.
    pub open_peak: u64,
}

impl LastStore {
    /// Slot `i` gives the current body length of `storage_keys[i]`. Callers
    /// that need a stable length-to-read interval must hold their write fence.
    pub fn point_value_lengths<F: FnMut(u64) -> Result<()>>(
        &self,
        collection: &str,
        storage_keys: &[String],
        mut admit: F,
    ) -> Result<Vec<Option<PointValueSize>>> {
        let mut groups: BTreeMap<ShardKey, Vec<(usize, &String)>> = BTreeMap::new();
        for (slot, id) in storage_keys.iter().enumerate() {
            groups
                .entry(self.point_key(collection, id))
                .or_default()
                .push((slot, id));
        }
        let mut lengths = vec![None; storage_keys.len()];
        for (key, entries) in groups {
            let directory = self.handle_dir(&key.0, key.1, key.2);
            let open_peak = cold_metadata::opening_bound(&directory)?;
            admit(open_peak)?;
            let Some(handle) = self.open_point_group_unpublished(key, true)? else {
                continue;
            };
            let shard = read_lock(&handle).expect("poison");
            for (slot, id) in entries {
                if let Some(location) = shard.lookup(id)? {
                    lengths[slot] = Some(PointValueSize {
                        bytes: location
                            .record_len()
                            .checked_sub(7 + id.len() as u64)
                            .ok_or_else(|| {
                                Error::Corrupt("record body length is invalid".into())
                            })?,
                        may_inflate: point_prefix::may_inflate(
                            &shard,
                            id,
                            location,
                            location.record_len() - (7 + id.len() as u64),
                        )?,
                        open_peak,
                    });
                }
            }
        }
        Ok(lengths)
    }
}

//! Explicit admin key traversal, with admission before a legacy cold open.

use super::*;

impl LastStore {
    /// Operator key-only traversal. Admit cold-group metadata before its open.
    pub fn walk_all_group_keys<F: FnMut(u64) -> Result<()>>(
        &self,
        collection: &str,
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        mut admit: F,
    ) -> Result<PhysicalRangePage> {
        let read_scope = crate::read_receipt::enter_child();
        let handles = self.walk_all_group_handles(collection, AllGroupsPurpose::Admin)?;
        let wanted = cursor.map(|position| (position.shard, position.group_id));
        let mut index = wanted.map_or(0, |key| {
            handles
                .binary_search(&key)
                .unwrap_or_else(std::convert::identity)
        });
        let mut after = cursor
            .filter(|position| {
                handles.get(index).copied() == Some((position.shard, position.group_id))
            })
            .and_then(|position| position.after_id.clone());
        let mut rows = Vec::new();
        let mut visited = 0u64;
        let mut next_cursor = None;
        while index < handles.len() && visited < 64 && rows.len() < limit {
            let (shard, group) = handles[index];
            admit(cold_metadata::opening_bound(
                &self.handle_dir(collection, shard, group),
            )?)?;
            let window = keysidecar::KeyWindow {
                start: "",
                end: None,
                after_id: after.as_deref(),
                limit: limit - rows.len() + 1,
            };
            let source =
                self.group_key_source_with_window(collection, shard, group, Some(window))?;
            let ids = self.list_range_keys_in_handle(
                &source,
                ("", None),
                after.as_deref(),
                limit - rows.len() + 1,
            )?;
            let more = ids.len() > limit - rows.len();
            let selected: Vec<_> = ids.into_iter().take(limit - rows.len()).collect();
            let last = selected.last().cloned();
            rows.extend(selected.into_iter().map(|key| (key, Vec::new())));
            visited += 1;
            if more {
                next_cursor = Some(PhysicalRangeCursor {
                    shard,
                    group_id: group,
                    after_id: last,
                });
                break;
            }
            index += 1;
            after = None;
            next_cursor = handles
                .get(index)
                .map(|(shard, group)| PhysicalRangeCursor {
                    shard: *shard,
                    group_id: *group,
                    after_id: None,
                });
        }
        Ok(PhysicalRangePage {
            rows,
            next_cursor,
            row_handle: None,
            handles_visited: visited,
            cold_shard_loads: read_scope.snapshot().group_opens,
        })
    }
}

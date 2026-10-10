//! Batched existence probes used by the rekey phases.

use super::*;

impl AtomStore {
    /// Batched existence probe with the rekey's error framing, tolerant of the
    /// empty key list several phases legitimately produce.
    pub(in crate::db_operations::admin_db) async fn rekey_exists(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.raw()
            .exists_items(keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{ctx}: {e}")))
    }

    /// Existence probe that always resolves the **live** group.
    ///
    /// [`crate::storage::traits::KvStore::exists_many`] is allowed to answer a
    /// *cold* group from the in-memory key-index cache or the on-disk key
    /// sidecar instead of loading the group. Its answer is therefore a function
    /// of what happens to be resident, not only of what is durable — the right
    /// trade for a page-at-a-time scan of millions of tips, and the wrong one
    /// for the last check before a delete.
    ///
    /// The singular `exists` takes the live handle per key, so it answers from
    /// the group's real index. That costs one group load per key, which is why
    /// only the candidate set reaches this path: by then it is the handful of
    /// tips a whole pass judged repairable, not the millions it walked.
    pub(in crate::db_operations::admin_db) async fn rekey_exists_live(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        let mut found = Vec::with_capacity(keys.len());
        for key in keys {
            found.push(
                self.raw()
                    .exists_item(key)
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("{ctx}: {e}")))?,
            );
        }
        Ok(found)
    }

    /// Existence probe whose *present* answers are confirmed on the live group.
    ///
    /// Cheap tier first, then one live probe over exactly the keys the cheap
    /// tier called present, ANDed back in by [`narrow_present_with_live`]. The
    /// result is therefore never more permissive than either tier alone.
    ///
    /// This is the shape a delete gate needs, at a cost a delete gate can
    /// afford. `rekey_exists_live` alone would spend one group load per key of
    /// every page the walk touches; this spends one only per key the cheap tier
    /// already nominated for deletion. Its callers restrict it further to the
    /// `already` population — slots this page did not write — so a first-pass
    /// migration, where every slot is written, pays nothing at all and
    /// `rekey_page_round_trips_do_not_scale_with_page_size` still holds.
    ///
    /// The remaining `false`s are left alone deliberately: for both callers a
    /// cheap false "absent" retains a key rather than reclaiming it, which is
    /// the conservative direction. See [`ExistsAuthority`].
    pub(in crate::db_operations::admin_db) async fn rekey_exists_confirmed(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        let cached = self.rekey_exists(keys, ctx).await?;
        let live_positions: Vec<usize> = cached
            .iter()
            .enumerate()
            .filter(|(_, &present)| present)
            .map(|(pos, _)| pos)
            .collect();
        if live_positions.is_empty() {
            return Ok(cached);
        }
        let live_keys: Vec<String> = live_positions
            .iter()
            .map(|&pos| keys[pos].clone())
            .collect();
        let live = self.rekey_exists_live(&live_keys, ctx).await?;
        Ok(narrow_present_with_live(&cached, &live_positions, &live))
    }

    /// Route an existence probe to the tier `authority` asks for.
    pub(in crate::db_operations::admin_db) async fn exists_with_authority(
        &self,
        keys: &[String],
        ctx: &str,
        authority: ExistsAuthority,
    ) -> Result<Vec<bool>, SchemaError> {
        match authority {
            ExistsAuthority::Cached => self.rekey_exists(keys, ctx).await,
            ExistsAuthority::Live => self.rekey_exists_live(keys, ctx).await,
        }
    }
}

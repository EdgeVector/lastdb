//! Translate an unchanged org's wire instance into this peer's isolated copy.

use super::*;

impl SyncEngine {
    pub(super) async fn local_generation_entry(
        &self,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<Option<LogEntry>> {
        let Some(target) = target else {
            return Ok(None);
        };
        let mut local = entry.clone();
        crate::sync::cloud_purge_peer::scope_alias::rebind_entry(
            self.store.as_ref(),
            &target.prefix,
            &mut local,
        )
        .await?;
        Ok(Some(local))
    }

    pub(super) fn legacy_physical_molecule_uuid(key: &str) -> Option<String> {
        let base_key =
            crate::sync::org_sync::strip_storage_prefix(key).map_or(key, |(_, base_key)| base_key);
        // A partition-prefixed atom body carries an embedded `mk:` segment.
        // Its Delete removes the atom body, not the molecule tip.
        if crate::atom::atom_key_codec::uuid_of(base_key).is_some() {
            return None;
        }
        molecule_uuid_from_record_key(key)
    }

    pub(super) fn legacy_delete_barrier(entry: &LogEntry, key: &str) -> DeleteBarrier {
        DeleteBarrier {
            mk_key: key.to_string(),
            written_at: entry.timestamp_ms.saturating_mul(1_000_000),
            logical_counter: 0,
            device_id: entry.device_id.clone(),
            mutation_uuid: String::new(),
            kind: DeleteKind::LegacyPhysical,
            displaced_atom_uuid: None,
            cloud_sequence: Some(entry.seq),
            tip_before_nanos: None,
        }
    }
}

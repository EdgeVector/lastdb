//! Collect atom UUIDs from current tips, history and conflicts.

use super::*;

impl AtomStore {
    /// Collect atom UUIDs from current tips, history events and conflicts.
    pub(in super::super) async fn collect_referenced_atom_uuids(
        &self,
        storage_prefix: Option<&str>,
        dry_run: bool,
    ) -> Result<HashSet<String>, SchemaError> {
        // Every scan below streams in bounded pages (see
        // [`Self::for_each_row_under_prefix`]). The reference set itself is
        // unavoidably whole-store — it is the answer — but it holds uuid
        // strings, not plane bytes: on the primary this owner measures that is
        // ~1.4M uuids against a `mk:` plane of 3.82 GiB and an `atom:` plane
        // of 3.68 GiB. Materializing those planes to build it was the cost,
        // not the set.
        let mut refs = HashSet::new();

        // Tips: mk: → PerKeyRecord.entry.atom_uuid (current head only)
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        self.for_each_row_under_prefix(&mk_prefix, "gc scan mk", dry_run, |_k, v| {
            if let Ok(val) = serde_json::from_slice::<Value>(v) {
                if let Some(u) = val.pointer("/entry/atom_uuid").and_then(|x| x.as_str()) {
                    refs.insert(u.to_string());
                }
            }
        })
        .await?;

        // History events still present after clear-history.
        //
        // Only the *head* atom of each remaining history row is required for
        // current tips/as-of-at-latest. Prior heads (`old_atom_uuid`) are the
        // bulk of orphaned historical field values (e.g. pre-migration Photo
        // file_bytes). We intentionally do not pin them — use a fuller backup
        // if you need deep history reconstruction.
        let hist_prefix = build_storage_key(storage_prefix, "history\0");
        self.for_each_row_under_prefix(&hist_prefix, "gc scan history", dry_run, |_k, v| {
            if let Ok(ev) = serde_json::from_slice::<MutationEvent>(v) {
                refs.insert(ev.new_atom_uuid);
                if let Some(loser) = ev.conflict_loser_atom {
                    refs.insert(loser);
                }
            }
        })
        .await?;

        // Conflict records may name atom UUIDs
        let conflict_prefix = build_storage_key(storage_prefix, "conflict\0");
        self.for_each_row_under_prefix(&conflict_prefix, "gc scan conflict", dry_run, |_k, v| {
            collect_atom_uuid_strings(v, &mut refs);
        })
        .await?;

        Ok(refs)
    }
}

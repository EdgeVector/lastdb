//! Mutation-history clear.

use super::*;

/// Approximate on-disk size of one history row: key plus serialized event.
fn history_row_bytes(storage_key: &str, ev: &MutationEvent) -> u64 {
    storage_key.len() as u64 + serde_json::to_vec(ev).map_or(0, |v| v.len() as u64)
}

/// True for a history row that keeps a conflicting write: a merge-conflict
/// event, a conflict loser, or an imported Put kept out of the tip. Tom's rule
/// (2026-10-13): when two devices write the same key, each write stays. A
/// history clear never selects such a row, and its loser atom keeps its edge.
fn keeps_conflicting_write(ev: &MutationEvent) -> bool {
    ev.is_conflict
        || ev.conflict_loser_atom.is_some()
        || ev.kind == crate::atom::MutationEventKind::SuppressedPut
}

/// Pick the history rows to delete for one molecule, with per-row byte sizes.
///
/// `keep == 0` selects every row; otherwise the newest `keep` events per
/// field key survive.
fn plan_history_deletes(
    events_with_keys: &[(String, MutationEvent)],
    keep: usize,
) -> (Vec<String>, HashMap<String, u64>) {
    let mut to_delete: Vec<String> = Vec::new();
    let mut key_bytes: HashMap<String, u64> = HashMap::new();

    if keep == 0 {
        // Latest-only: purge every history row for this molecule.
        for (storage_key, ev) in events_with_keys {
            key_bytes.insert(storage_key.clone(), history_row_bytes(storage_key, ev));
            to_delete.push(storage_key.clone());
        }
        return (to_delete, key_bytes);
    }

    // Group by field_key; keep newest `keep` events per key.
    let mut by_key: HashMap<String, Vec<(String, i64)>> = HashMap::new();
    for (storage_key, ev) in events_with_keys {
        let fk = format!(
            "{}|{}",
            ev.field_key.hash.as_deref().unwrap_or(""),
            ev.field_key.range.as_deref().unwrap_or("")
        );
        let ts = ev.timestamp.timestamp_nanos_opt().unwrap_or(0);
        by_key
            .entry(fk)
            .or_default()
            .push((storage_key.clone(), ts));
        key_bytes.insert(storage_key.clone(), history_row_bytes(storage_key, ev));
    }

    for (_fk, mut rows) in by_key {
        rows.sort_by_key(|b| std::cmp::Reverse(b.1)); // newest first
        if rows.len() > keep {
            for (k, _) in rows.into_iter().skip(keep) {
                to_delete.push(k);
            }
        }
    }
    (to_delete, key_bytes)
}

impl AtomStore {
    /// Delete mutation-history rows, keeping the newest `keep_last_per_key`
    /// events per (molecule, field_key).
    ///
    /// - **`keep_last_per_key == 0`**: full purge — delete **all** history rows
    ///   for the selected schemas. Latest-only storage: current tips (`mk:`)
    ///   and atoms remain; time-travel / `as_of` over deleted events is gone.
    /// - **`keep_last_per_key >= 1`**: keep the newest N events per field key.
    ///
    /// Does **not** delete current tips or atoms (safe; may leave orphan atoms
    /// until a later GC). Does **not** select a conflict row (see
    /// [`keeps_conflicting_write`]): those rows, and the loser atoms their
    /// edges keep alive, stay.
    pub async fn clear_mutation_history(
        &self,
        schema_field_molecules: &[(String, Vec<String>)],
        keep_last_per_key: usize,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<HistoryClearReport, SchemaError> {
        let keep = keep_last_per_key;
        let mut report = HistoryClearReport {
            dry_run,
            keep_last_per_key: keep,
            schemas_touched: 0,
            history_rows_deleted: 0,
            history_bytes_freed_approx: 0,
            conflict_rows_kept: 0,
            per_schema: Vec::new(),
        };

        for (schema_name, mols) in schema_field_molecules {
            let mut schema_deleted = 0u64;
            let mut schema_bytes = 0u64;
            for mol in mols {
                let events_with_keys: Vec<(String, MutationEvent)> = {
                    let base = crate::atom::molecule_key_codec::history_molecule_prefix(mol);
                    self.scan_exact_storage_prefix(&base, storage_prefix, "clear_history load")
                        .await?
                };
                let (conflicts, events_with_keys): (Vec<_>, Vec<_>) = events_with_keys
                    .into_iter()
                    .partition(|(_, ev)| keeps_conflicting_write(ev));
                report.conflict_rows_kept += conflicts.len() as u64;
                if events_with_keys.is_empty() {
                    continue;
                }

                let (to_delete, key_bytes) = plan_history_deletes(&events_with_keys, keep);

                for k in &to_delete {
                    schema_deleted += 1;
                    schema_bytes += key_bytes.get(k).copied().unwrap_or(0);
                }

                if !dry_run && !to_delete.is_empty() {
                    self.raw().batch_delete_keys(to_delete).await.map_err(|e| {
                        SchemaError::InvalidData(format!("clear_history delete {schema_name}: {e}"))
                    })?;
                }
            }
            if schema_deleted > 0 {
                report.schemas_touched += 1;
                report.history_rows_deleted += schema_deleted;
                report.history_bytes_freed_approx += schema_bytes;
                report.per_schema.push(SchemaHistoryClearStat {
                    schema_name: schema_name.clone(),
                    history_rows_deleted: schema_deleted,
                    history_bytes_freed_approx: schema_bytes,
                });
            }
        }

        if !dry_run && report.history_rows_deleted > 0 {
            let _ = self.flush().await;
        }
        report
            .per_schema
            .sort_by_key(|b| std::cmp::Reverse(b.history_rows_deleted));
        Ok(report)
    }
}

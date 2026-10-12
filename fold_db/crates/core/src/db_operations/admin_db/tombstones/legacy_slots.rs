//! Discovery of legacy tombstone slots.

use super::super::super::atom_store::PerKeyRecord;
use super::*;

impl AtomStore {
    /// Discover one bounded page of tombstone-content `mk:` rows by storage
    /// cursor. This deliberately never reconstructs API keys: BlindV1 hashes
    /// are one-way. The returned opaque slots may only be passed to the guarded
    /// storage-slot purge path.
    pub(crate) async fn scan_legacy_tombstone_slots(
        &self,
        molecules: &HashSet<String>,
        max_keys: usize,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<LegacyTombstoneScan, SchemaError> {
        let pass_started = std::time::Instant::now();
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut scan = LegacyTombstoneScan {
            keys_scanned: 0,
            keys_unreadable: 0,
            atoms_fetched: 0,
            atoms_missing: 0,
            slots: Vec::new(),
            more_remaining: false,
            next_after_key: None,
        };
        let mut pending: Vec<(String, String, PerKeyRecord)> = Vec::new();
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .scan_mk_page(&page_start, &mk_end, "range scan legacy tombstones")
                .await?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                let base_key = key
                    .strip_prefix(&mk_prefix)
                    .map(|suffix| format!("{}{}", molecule_key_codec::MK_PREFIX, suffix));
                let Some(base_key) = base_key else {
                    continue;
                };
                let Some(molecule) = base_key
                    .strip_prefix(molecule_key_codec::MK_PREFIX)
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(molecule, _)| molecule.to_string())
                else {
                    continue;
                };
                if !molecules.contains(&molecule) {
                    continue;
                }
                if scan.keys_scanned as usize >= max_keys
                    || (scan.keys_scanned > 0 && pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    scan.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                scan.keys_scanned += 1;
                scan.next_after_key = Some(key.clone());
                let Ok(record) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                    scan.keys_unreadable += 1;
                    continue;
                };
                pending.push((base_key, molecule, record));
                if pending.len() >= FETCH_CHUNK {
                    self.classify_legacy_tombstone_slots(&mut pending, &mut scan, storage_prefix)
                        .await?;
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        self.classify_legacy_tombstone_slots(&mut pending, &mut scan, storage_prefix)
            .await?;
        Ok(scan)
    }

    pub(in crate::db_operations::admin_db) async fn classify_legacy_tombstone_slots(
        &self,
        pending: &mut Vec<(String, String, PerKeyRecord)>,
        scan: &mut LegacyTombstoneScan,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let atom_slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = pending
            .iter()
            .map(|(_, _, record)| (record.entry.atom_uuid.as_str(), None))
            .collect();
        let atoms = self.get_atoms_located(&atom_slots, storage_prefix).await?;
        scan.atoms_fetched += atoms.len() as u64;
        for ((base_key, molecule, _), atom) in std::mem::take(pending).into_iter().zip(atoms) {
            let Some(atom) = atom else {
                scan.atoms_missing += 1;
                continue;
            };
            if !crate::atom::is_tombstone_value(atom.content()) {
                continue;
            }
            let Some((storage_hash, storage_range)) =
                molecule_key_codec::decode_hash_range(&base_key, &molecule)
            else {
                scan.keys_unreadable += 1;
                continue;
            };
            scan.slots.push(LegacyTombstoneSlot {
                molecule_uuid: molecule,
                storage_hash,
                storage_range,
            });
        }
        Ok(())
    }
}

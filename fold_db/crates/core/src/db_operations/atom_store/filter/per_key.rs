//! Per-key point lookups and prefix/range scans.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

use super::super::types::PerKeyRecord;
use super::super::AtomStore;
use super::super::HashKeyLookupRecord;
use super::PageFill;

impl AtomStore {
    /// Check one exact Delete barrier for each returned molecule row. A range
    /// read never walks the barriers; it checks only the keys it already found.
    pub(crate) async fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<(String, PerKeyRecord)>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let final_keys: Vec<String> = rows
            .iter()
            .map(|(base, _)| build_storage_key(storage_prefix, base))
            .collect();
        let barrier_keys: Vec<String> = final_keys
            .iter()
            .map(|key| crate::atom::delete_barrier::delete_barrier_key(key.as_bytes()))
            .collect();
        let durable = self
            .main_store
            .get_items::<crate::atom::delete_barrier::DeleteBarrier>(&barrier_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("batch read Delete barriers: {error}"))
            })?;
        let mut visible = Vec::with_capacity(rows.len());
        for (((base, record), final_key), durable) in rows.into_iter().zip(final_keys).zip(durable)
        {
            if durable
                .as_ref()
                .is_some_and(|barrier| !barrier.matches_key(final_key.as_bytes()))
            {
                return Err(SchemaError::InvalidData(
                    "durable Delete barrier key identity differs from the molecule key".into(),
                ));
            }
            let pending = self.pending_delete_barrier(&final_key);
            let winner = match (pending, durable) {
                (Some(pending), Some(durable)) if pending.is_newer_than(&durable) => Some(pending),
                (Some(_), Some(durable)) => Some(durable),
                (Some(pending), None) => Some(pending),
                (None, durable) => durable,
            };
            if winner.is_none_or(|barrier| !barrier.blocks_tip(&record.entry)) {
                visible.push((base, record));
            }
        }
        Ok(visible)
    }

    /// Fetch a single per-key record by its already-encoded base key (e.g.
    /// [`molecule_key_codec::hash_range_record_key`]) — the O(1) point lookup.
    /// `None` when the key is absent.
    pub(crate) async fn get_per_key(
        &self,
        base_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        let record = self
            .get_per_key_unfiltered(base_key, storage_prefix)
            .await?;
        let Some((key, record)) = record else {
            return Ok(None);
        };
        let storage_key = build_storage_key(storage_prefix, base_key);
        if self
            .winning_delete_barrier(&storage_key)
            .await?
            .is_some_and(|barrier| barrier.blocks_tip(&record.entry))
        {
            return Ok(None);
        }
        Ok(Some((key, record)))
    }

    /// Cleanup must inspect the physical tip even after a winning Delete
    /// barrier hides it from readers.
    pub(crate) async fn get_per_key_unfiltered(
        &self,
        base_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        let storage_key = build_storage_key(storage_prefix, base_key);
        let rec: Option<PerKeyRecord> =
            self.main_store.get_item(&storage_key).await.map_err(|e| {
                SchemaError::InvalidData(format!("read per-key record {storage_key}: {e}"))
            })?;
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_key)
        else {
            return Ok(rec.map(|record| (base_key.to_string(), record)));
        };
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(rec.map(|record| (base_key.to_string(), record)));
        };
        let base = self
            .generation_slot_for_record_key(base_key, &pointer.generation, storage_prefix)
            .await?;
        let merged = Self::merge_generation_slot(rec, base);
        let deletion = self
            .generation_delete_for_record_key(base_key, storage_prefix)
            .await?;
        Ok(Self::apply_generation_delete(merged, deletion).map(|r| (base_key.to_string(), r)))
    }

    /// Batch-read exact `mk:` keys for one page.
    ///
    /// `base_keys` are already encoded
    /// ([`molecule_key_codec::hash_range_record_key`]). This does not re-encode
    /// API hash/range candidates: a storage-form page key passed through
    /// [`Self::load_per_key_records_for_slots`] would be blinded again, and
    /// that helper also reads deletes when no generation pointer exists.
    ///
    /// One `get_items` reads every base key. Each molecule pays one generation
    /// pointer read. Generation slots and sparse deletes are read only when
    /// that pointer exists, matching [`Self::get_per_key`]. A molecule with no
    /// pointer does not read deletes, so a delete without an active generation
    /// cannot hide a record. Output slot `i` is the merged record for
    /// `base_keys[i]`, or `None` when the key is absent.
    pub(crate) async fn load_exact_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        base_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        if base_keys.is_empty() {
            return Ok(Vec::new());
        }
        let storage_keys: Vec<String> = base_keys
            .iter()
            .map(|base| build_storage_key(storage_prefix, base))
            .collect();
        let mut hit = self
            .main_store
            .get_items::<PerKeyRecord>(&storage_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("batch read per-key records: {e}")))?;

        let mut slots_by_molecule: std::collections::BTreeMap<&str, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (slot, base_key) in base_keys.iter().enumerate() {
            if let Some(molecule_uuid) =
                molecule_key_codec::molecule_uuid_from_storage_key(base_key)
            {
                slots_by_molecule
                    .entry(molecule_uuid)
                    .or_default()
                    .push(slot);
            }
        }

        for (molecule_uuid, slots) in slots_by_molecule {
            let Some(pointer) = self
                .active_molecule_generation(molecule_uuid, storage_prefix)
                .await?
            else {
                continue;
            };
            let mut generation_keys = Vec::new();
            let mut generation_slots = Vec::new();
            for &slot in &slots {
                if let Some(key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
                    molecule_uuid,
                    &pointer.generation,
                    &base_keys[slot],
                ) {
                    generation_keys.push(build_storage_key(storage_prefix, &key));
                    generation_slots.push(slot);
                }
            }
            let generation = self
                .main_store
                .get_items::<super::super::MoleculeGenerationSlot>(&generation_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "batch read generation records for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for (slot, base) in generation_slots.into_iter().zip(generation) {
                if base.is_some() {
                    hit[slot] = Self::merge_generation_slot(hit[slot].take(), base);
                }
            }

            let mut delete_keys = Vec::new();
            let mut delete_slots = Vec::new();
            for &slot in &slots {
                if let Some(key) =
                    molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                        molecule_uuid,
                        &base_keys[slot],
                    )
                {
                    delete_keys.push(build_storage_key(storage_prefix, &key));
                    delete_slots.push(slot);
                }
            }
            let deletes = self
                .main_store
                .get_items::<super::super::MoleculeGenerationDelete>(&delete_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "batch read generation deletes for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for (slot, deletion) in delete_slots.into_iter().zip(deletes) {
                if deletion.is_some() {
                    hit[slot] = Self::apply_generation_delete(hit[slot].take(), deletion);
                }
            }
        }
        Ok(hit)
    }

    /// One grouped read for many candidate spellings of many caller keys.
    ///
    /// `groups[i]` lists the dual-read spellings of caller key `i`, in the
    /// order [`Self::get_per_key`] would try them. The first spelling that is
    /// present and not hidden by a Delete barrier wins. A later spelling of
    /// that same row is ignored.
    ///
    /// Present spellings are read by one [`Self::load_exact_per_key_records`]
    /// and checked by one [`Self::filter_delete_barriers_for_records`]. Keys
    /// that share a hash group share one unpublished open. The group does
    /// not stay. An empty candidate list reads nothing.
    pub(crate) async fn load_first_present_candidate(
        &self,
        storage_prefix: Option<&str>,
        groups: &[Vec<String>],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        let mut seen = std::collections::HashSet::<&str>::new();
        let mut unique = Vec::new();
        for group in groups {
            for key in group {
                if seen.insert(key.as_str()) {
                    unique.push(key.clone());
                }
            }
        }
        if unique.is_empty() {
            return Ok(vec![None; groups.len()]);
        }
        let loaded = self
            .load_exact_per_key_records(storage_prefix, &unique)
            .await?;
        let present: Vec<(String, PerKeyRecord)> = unique
            .into_iter()
            .zip(loaded)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect();
        let visible = self
            .filter_delete_barriers_for_records(present, storage_prefix)
            .await?;
        let visible_by_key: std::collections::HashMap<String, PerKeyRecord> =
            visible.into_iter().collect();
        let mut out = Vec::with_capacity(groups.len());
        for group in groups {
            let mut hit = None;
            for key in group {
                if let Some(record) = visible_by_key.get(key) {
                    hit = Some(record.clone());
                    break;
                }
            }
            out.push(hit);
        }
        Ok(out)
    }

    /// Fetch a derived unique-hash marker for `HashRangeFilter::HashKey`.
    ///
    /// Returns `None` when the hash-key lookup index is retired, or for missing
    /// / ambiguous markers, so callers fall back to the authoritative `mk:`
    /// prefix scan.
    pub(crate) async fn get_unique_hash_key_record(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        if !super::super::helpers::HASH_RANGE_HASH_KEY_LOOKUP_ENABLED {
            // `mhk:` co-write retired — never prefer historical markers.
            let _ = (molecule_uuid, api_hash, storage_prefix);
            return Ok(None);
        }
        // Single storage form (plain or blind — no dual-read).
        let candidates = self
            .key_codec_for_molecule(molecule_uuid)
            .storage_hash_read_candidates(molecule_uuid, api_hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        for storage_hash in candidates {
            let base_key = molecule_key_codec::hash_key_lookup_key(molecule_uuid, &storage_hash);
            let storage_key = build_storage_key(storage_prefix, &base_key);
            let marker: Option<HashKeyLookupRecord> =
                self.main_store.get_item(&storage_key).await.map_err(|e| {
                    SchemaError::InvalidData(format!("read hash-key marker {storage_key}: {e}"))
                })?;
            let Some(marker) = marker else {
                continue;
            };
            match (marker.range, marker.record) {
                (Some(range), Some(record)) => {
                    return Ok(Some((
                        molecule_key_codec::hash_range_record_key(
                            molecule_uuid,
                            &storage_hash,
                            &range,
                        ),
                        record,
                    )));
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Batch-fetch per-key molecule records for a known set of `(hash, range)`
    /// slots — the co-key multi-field list path's secondary-field fan-out.
    ///
    /// One `get_many` over the storage layer (single `spawn_blocking` hop)
    /// instead of N sequential `get_per_key` calls. Missing keys yield no
    /// entry (sparse fields). Order of `keys` is preserved for present hits
    /// only by walking the input list.
    pub(crate) async fn load_per_key_records_for_slots(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<Vec<(String, String, PerKeyRecord)>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // One get_many of storage-form keys. Dual-read across molecule-UUID
        // spellings (write-form base64url and legacy hex) and across hash/range
        // HMAC keys bound to each spelling.
        let mut out: Vec<(String, String, PerKeyRecord)> = Vec::with_capacity(keys.len());
        let codec = self.key_codec_for_molecule(molecule_uuid);
        let mut storage_keys: Vec<String> = Vec::new();
        let mut slot_of: Vec<usize> = Vec::new();
        for (i, (api_hash, api_range)) in keys.iter().enumerate() {
            let candidates = codec
                .api_hash_range_record_keys_for_read(molecule_uuid, api_hash, api_range)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            for base in candidates {
                storage_keys.push(build_storage_key(storage_prefix, &base));
                slot_of.push(i);
            }
        }
        let fetched = self
            .main_store
            .get_items::<PerKeyRecord>(&storage_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "batch read per-key records for molecule {molecule_uuid}: {e}"
                ))
            })?;
        let mut hit: Vec<Option<PerKeyRecord>> = vec![None; keys.len()];
        let mut hit_key: Vec<Option<String>> = vec![None; keys.len()];
        for ((slot, storage_key), rec) in slot_of
            .iter()
            .copied()
            .zip(storage_keys.iter())
            .zip(fetched)
        {
            if hit[slot].is_none() {
                if let Some(rec) = rec {
                    hit[slot] = Some(rec);
                    hit_key[slot] = Some(storage_key.clone());
                }
            }
        }
        if let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        {
            let mut generation_keys = Vec::new();
            let mut generation_slot_of = Vec::new();
            for (slot, storage_key) in storage_keys.iter().enumerate() {
                let strip = build_storage_key(storage_prefix, "");
                let bare = storage_key.strip_prefix(&strip).unwrap_or(storage_key);
                if let Some(key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
                    molecule_uuid,
                    &pointer.generation,
                    bare,
                ) {
                    generation_keys.push(build_storage_key(storage_prefix, &key));
                    generation_slot_of.push((slot_of[slot], storage_key.clone()));
                }
            }
            let generation = self
                .main_store
                .get_items::<super::super::MoleculeGenerationSlot>(&generation_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "batch read generation records for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for ((slot, storage_key), base) in generation_slot_of.into_iter().zip(generation) {
                if base.is_some() {
                    hit[slot] = Self::merge_generation_slot(hit[slot].take(), base);
                    hit_key[slot] = Some(storage_key);
                }
            }
        }
        let strip = build_storage_key(storage_prefix, "");
        let mut delete_keys = Vec::new();
        let mut delete_slot_of = Vec::new();
        for (candidate, slot) in storage_keys.iter().zip(slot_of.iter().copied()) {
            let bare = candidate.strip_prefix(&strip).unwrap_or(candidate);
            if let Some(key) = molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                bare,
            ) {
                delete_keys.push(build_storage_key(storage_prefix, &key));
                delete_slot_of.push(slot);
            }
        }
        let deletes = self
            .main_store
            .get_items::<super::super::MoleculeGenerationDelete>(&delete_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "batch read generation deletes for molecule {molecule_uuid}: {error}"
                ))
            })?;
        for (slot, deletion) in delete_slot_of.into_iter().zip(deletes) {
            if deletion.is_some() {
                hit[slot] = Self::apply_generation_delete(hit[slot].take(), deletion);
            }
        }
        let present_keys: Vec<String> = hit
            .iter()
            .enumerate()
            .filter_map(|(slot, rec)| rec.as_ref().and(hit_key[slot].as_ref()).cloned())
            .collect();
        let barriers = self.winning_delete_barriers(&present_keys).await?;
        for (slot, ((hash, range), rec)) in keys.iter().zip(hit).enumerate() {
            if let Some(rec) = rec {
                let final_key = hit_key[slot].as_deref().ok_or_else(|| {
                    SchemaError::InvalidData("per-key record has no storage key".into())
                })?;
                if barriers
                    .get(final_key)
                    .is_none_or(|barrier| !barrier.blocks_tip(&rec.entry))
                {
                    out.push((hash.clone(), range.clone(), rec));
                }
            }
        }
        Ok(out)
    }

    pub(crate) fn strip_per_key_storage_prefix(
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Vec<(String, PerKeyRecord)> {
        let strip = build_storage_key(storage_prefix, "");
        rows.into_iter()
            .map(|(k, r)| (k.strip_prefix(&strip).unwrap_or(&k).to_string(), r))
            .collect()
    }

    /// Union of prefix scans, deduped by decoded `(hash, range)`.
    ///
    /// Dual-read of molecule-UUID encodings produces two `mk:{M}:` prefixes
    /// that are not collation-compatible. Merge on the decoded slot so a
    /// mixed-encoding field stays complete, and keep the first spelling
    /// (write-form) when both exist.
    pub(crate) async fn scan_per_key_prefixes(
        &self,
        base_prefixes: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let mut merged: Vec<(String, PerKeyRecord)> = Vec::new();
        let mut seen: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for base_prefix in base_prefixes {
            let scan_prefix = build_storage_key(storage_prefix, base_prefix);
            let scanned: Vec<(String, PerKeyRecord)> = self
                .main_store
                .scan_items_with_prefix(&scan_prefix)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("scan per-key prefix {scan_prefix}: {e}"))
                })?;
            let stripped = Self::strip_per_key_storage_prefix(storage_prefix, scanned);
            for (key, rec) in stripped {
                let slot = molecule_key_codec::decode_hash_range_any(&key).or_else(|| {
                    // Non-mk prefixes (tests / opaque M) still belong in the
                    // result; they cannot collide with a decoded slot.
                    Some((key.clone(), String::new()))
                });
                let Some(slot) = slot else {
                    continue;
                };
                if seen.insert(slot) {
                    merged.push((key, rec));
                }
            }
        }
        let Some(molecule_uuid) = base_prefixes
            .first()
            .and_then(|prefix| molecule_key_codec::molecule_uuid_from_storage_key(prefix))
        else {
            return self
                .filter_delete_barriers_for_records(merged, storage_prefix)
                .await;
        };
        // UUID read candidates can produce several prefixes. Each generation
        // belongs to its exact molecule spelling, so merge each bounded prefix
        // into the already-deduplicated live result.
        let mut out = merged;
        for base_prefix in base_prefixes {
            if molecule_key_codec::molecule_uuid_from_storage_key(base_prefix)
                != Some(molecule_uuid)
            {
                continue;
            }
            out = self
                .merge_generation_prefix_rows(molecule_uuid, base_prefix, storage_prefix, out)
                .await?;
        }
        self.filter_delete_barriers_for_records(out, storage_prefix)
            .await
    }

    /// Scan **at most `limit`** per-key records under the molecule's record
    /// prefix, in ascending key order — the bounded read behind a paginated list
    /// query (`HashRangeFilter::Page`). Returns `(base_key, record)` pairs with
    /// the optional `{storage_prefix}:` prefix stripped.
    ///
    /// Because the per-key records are stored in key order and the page is the
    /// front `offset+limit` of that order, fetching only the first `limit`
    /// records (caller passes `offset + limit`) loads the page without touching
    /// the rest of the field — the read cost is independent of field cardinality.
    /// This is only correct where the storage key order matches the field's page
    /// order (Hash and Range fields, whose record key is the raw key segment);
    /// the HashRange page order differs from its `esc(hash)\0range` storage order
    /// and is handled separately (see `load_filtered_hash_range`).
    pub(crate) async fn scan_per_key_prefix_paged(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let scan_prefix = build_storage_key(storage_prefix, base_prefix);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_with_prefix_paged(&scan_prefix, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key prefix {scan_prefix}: {e}"))
            })?;
        Ok(Self::strip_per_key_storage_prefix(storage_prefix, scanned))
    }

    /// Scan the per-key records whose base key falls in `start..end` (the
    /// encoded base keys, e.g. [`molecule_key_codec::hash_range_record_key`])
    /// — a bounded `O(matches)` ordered range scan. Returns `(base_key, record)`
    /// pairs with the optional `{storage_prefix}:` prefix stripped.
    pub(crate) async fn scan_per_key_range(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let start = build_storage_key(storage_prefix, base_start);
        let end = build_storage_key(storage_prefix, base_end);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_in_range(&start, &end)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("scan per-key range {start}..{end}: {e}"))
            })?;
        let live = Self::strip_per_key_storage_prefix(storage_prefix, scanned);
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_start)
        else {
            return self
                .filter_delete_barriers_for_records(live, storage_prefix)
                .await;
        };
        let merged = self
            .merge_generation_range_rows(molecule_uuid, base_start, base_end, storage_prefix, live)
            .await?;
        self.filter_delete_barriers_for_records(merged, storage_prefix)
            .await
    }

    /// Scan forward under `base_prefix` until `want` rows that `fill` keeps
    /// have been collected, or the prefix is exhausted.
    ///
    /// `after` is an **exclusive** lower bound (a keyset cursor); `None` starts
    /// at the head of the prefix.
    ///
    /// This is the bounded read behind every paginated list. It exists because
    /// [`Self::scan_per_key_prefix_paged`] bounds on STORED rows: a caller that
    /// asks for `limit` rows and then drops the tombstoned ones is left with
    /// `limit × live_fraction` and no way to tell that from a genuinely short
    /// page. Refilling here — rather than at the caller — keeps the escalation
    /// where the key order lives, so each extra scan resumes from the last key
    /// seen instead of re-walking the prefix from the head.
    ///
    /// Cost tracks the stored rows the window spans, which is what any correct
    /// answer must read; a molecule with no tombstones does exactly one scan.
    pub(crate) async fn scan_per_key_window_filled(
        &self,
        base_prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        use crate::schema::types::field::FilterUtils;

        if want == 0 {
            return Ok(Vec::new());
        }
        let base_end = FilterUtils::create_prefix_end(base_prefix);
        let mut cursor: Option<String> = after.map(ToString::to_string);
        let mut kept: Vec<(String, PerKeyRecord)> = Vec::new();

        loop {
            let ask = fill.chunk_for(want - kept.len());
            let (batch, exhausted) = match cursor.as_deref() {
                None => {
                    let rows = self
                        .scan_per_key_prefix_paged(base_prefix, storage_prefix, ask)
                        .await?;
                    let exhausted = rows.len() < ask;
                    (rows, exhausted)
                }
                Some(after) => {
                    // The range primitive is inclusive on `start`; ask for one
                    // extra so dropping the cursor row still yields `ask` new
                    // rows, and read exhaustion from the raw count.
                    let probe = ask.saturating_add(1);
                    let rows = self
                        .scan_per_key_range_paged(after, &base_end, storage_prefix, probe)
                        .await?;
                    let exhausted = rows.len() < probe;
                    let advanced: Vec<(String, PerKeyRecord)> = rows
                        .into_iter()
                        .filter(|(key, _)| key.as_str() > after)
                        .collect();
                    (advanced, exhausted)
                }
            };

            // An ascending scan from an inclusive `after` can only repeat that
            // one key, so a non-exhausted batch always advances the cursor.
            // Breaking on an empty batch keeps that an invariant, not a hang.
            if batch.is_empty() {
                break;
            }
            // The storage scan can read ahead to amortize sparse tombstones.
            // Check Delete barriers only for rows this page consumes. A live
            // five-row page must not point-read barriers for every row in the
            // read-ahead batch. Advance the cursor past only examined rows.
            let mut rows = batch.into_iter();
            while kept.len() < want {
                let need = want - kept.len();
                let mut candidates = Vec::new();
                for (key, record) in rows.by_ref() {
                    cursor = Some(key.clone());
                    if fill.keeps(&record) {
                        candidates.push((key, record));
                        if candidates.len() == need {
                            break;
                        }
                    }
                }
                if candidates.is_empty() {
                    break;
                }
                kept.extend(
                    self.filter_delete_barriers_for_records(candidates, storage_prefix)
                        .await?,
                );
            }
            if kept.len() >= want || exhausted {
                break;
            }
        }

        kept.truncate(want);
        Ok(kept)
    }

    /// Keys-only page of **live** record identities on one molecule.
    ///
    /// Walks `mk:{M}:` with [`PageFill::LiveRows`] so tombstoned tips do not
    /// occupy the window. Decodes hash/range from the storage key. Does **not**
    /// fetch `atom:` bodies.
    ///
    /// `after` is an exclusive keyset cursor (a previous page's last storage
    /// key). It must sit under this molecule's prefix.
    pub async fn list_live_record_keys(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        self.list_live_record_keys_filtered(molecule, limit, after, storage_prefix, None)
            .await
    }

    /// Keys-only page, optionally restricted to one API hash partition.
    ///
    /// `hash_filter` is the **API** hash (not the storage token). The codec
    /// blinds it when the home uses BlindV1, then scans
    /// `mk:{M}:{esc(storage_hash)}\0` — O(log M) under that hash, not a
    /// schema-wide walk. No atom bodies except BlindV1's per-key API-hash
    /// recovery, which `list_live_record_keys` already pays.
    pub async fn list_live_record_keys_filtered(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let prefix = match hash_filter {
            Some(api_hash) => self
                .key_codec()
                .api_hash_range_scan_prefix_for_hash(molecule, api_hash)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
            None => molecule_key_codec::molecule_record_prefix(molecule),
        };
        if let Some(cursor) = after {
            let ok = cursor.starts_with(&prefix) || cursor.contains(&format!(":{prefix}"));
            if !ok {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's key molecule".into(),
                ));
            }
        }

        let want = limit.saturating_add(1);
        let rows = self
            .scan_per_key_window_filled(&prefix, after, storage_prefix, want, PageFill::LiveRows)
            .await?;
        let has_more = rows.len() > limit;
        let kept = if has_more { &rows[..limit] } else { &rows[..] };
        let next_cursor = has_more
            .then(|| kept.last().map(|(key, _)| key.clone()))
            .flatten();

        let codec = self.key_codec();
        let blind = codec.encoding() == crate::atom::HashKeyEncoding::BlindV1;
        let mut keys = Vec::with_capacity(kept.len());
        for (key, rec) in kept {
            let decoded = molecule_key_codec::decode_hash_range(key, molecule).or_else(|| {
                let bare = key
                    .rsplit_once(":mk:")
                    .map_or_else(|| key.clone(), |(_, rest)| format!("mk:{rest}"));
                molecule_key_codec::decode_hash_range(&bare, molecule)
            });
            let (storage_hash, storage_range) =
                decoded.unwrap_or_else(|| (key.clone(), String::new()));
            let range = crate::crypto::E2eKeys::ope_decode_range_plaintext(&storage_range)
                .unwrap_or_else(|| storage_range.clone());
            let hash = if blind {
                match self
                    .get_atom_by_uuid(rec.entry.atom_uuid.as_str(), storage_prefix)
                    .await?
                {
                    Some(atom) => {
                        let api = atom
                            .content()
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_default();
                        if !api.is_empty()
                            && codec
                                .storage_hash(molecule, &api)
                                .is_ok_and(|blinded| blinded == storage_hash)
                        {
                            api
                        } else {
                            storage_hash
                        }
                    }
                    None => storage_hash,
                }
            } else {
                storage_hash
            };
            keys.push((hash, range));
        }
        Ok((keys, next_cursor, has_more))
    }

    /// Scan at most `limit` per-key records in `base_start..base_end`, with the
    /// optional org prefix stripped from returned keys. This is the bounded
    /// range primitive behind keyset continuation pages.
    pub(crate) async fn scan_per_key_range_paged(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let start = build_storage_key(storage_prefix, base_start);
        let end = build_storage_key(storage_prefix, base_end);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_in_range_paged(&start, &end, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key range {start}..{end}: {e}"))
            })?;
        Ok(Self::strip_per_key_storage_prefix(storage_prefix, scanned))
    }
}

//! Molecule key listings and hash-bucket debugging.

use super::*;

impl AtomStore {
    /// Read-only list of one molecule's live `mk:` storage keys.
    ///
    /// Point-scoped to `mk:{M}:` (has a partition separator — not a full-store
    /// sweep). Emits the storage key, decoded hash/range when the unified
    /// encoding parses, and marks rows whose decoded pair collides with another
    /// row in the same molecule. No atom bodies are fetched.
    ///
    /// This is the diagnostic that separates H1 (two logical keys, one order
    /// entry) from H2 (one logical key stored as two encodings) for the
    /// order-log shortfall investigation.
    pub async fn list_molecule_keys(
        &self,
        molecule: &str,
        max_keys: Option<usize>,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeKeysReport, SchemaError> {
        let mol_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule),
        );
        let mol_end = FilterUtils::create_prefix_end(&mol_prefix)?;
        let mut page_start = mol_prefix.clone();
        let mut skip_head: Option<String> = None;
        let mut rows_out: Vec<MoleculeKeyRow> = Vec::new();
        let mut seen: HashMap<(String, String), usize> = HashMap::new();
        let mut more = false;
        const PAGE: usize = 512;

        loop {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mol_end.as_bytes(), PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan molecule keys: {e}")))?;
            let page_len = rows.len();
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            for (k, _) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|m| rows_out.len() >= m) {
                    more = true;
                    break;
                }
                let (hash, range, collision) = decode_molecule_key_row(&key, molecule, &mut seen);
                rows_out.push(MoleculeKeyRow {
                    storage_key: key,
                    hash,
                    range,
                    collision,
                });
            }
            if more {
                break;
            }
            if page_len < PAGE {
                break;
            }
            skip_head = Some(page_start.clone());
        }

        Ok(summarize_molecule_keys(molecule, rows_out, seen, more))
    }

    /// Dump one molecule's raw `mk:` rows under one API HashKey.
    ///
    /// Unlike [`Self::list_molecule_keys`], this method never walks the full
    /// molecule. It maps `api_hash` through the in-process molecule key bundle
    /// and reads only the exact hash partition. The node route that exposes
    /// this method requires an explicit development-only environment gate.
    pub async fn debug_molecule_hash_bucket(
        &self,
        molecule: &str,
        api_hash: &str,
        max_keys: usize,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeHashBucketReport, SchemaError> {
        if molecule.is_empty() {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires a molecule".into(),
            ));
        }
        if api_hash.is_empty() {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires an API hash".into(),
            ));
        }
        if max_keys == 0 {
            return Err(SchemaError::InvalidData(
                "debug molecule hash bucket requires max_keys > 0".into(),
            ));
        }

        // Warm an existing per-molecule bundle without minting one. A missing
        // bundle is a legacy molecule and correctly falls back to node keys.
        self.load_molecule_key_bundle(molecule).await?;
        let codec = self.key_codec_for_molecule(molecule)?;
        let prefix = codec
            .api_hash_range_scan_prefix_for_hash(molecule, api_hash)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let prefixes = [prefix];

        let want = max_keys.saturating_add(1);
        let mut raw_rows = self
            .scan_distinct_per_key_prefixes(&prefixes, storage_prefix, want)
            .await?;
        let more_remaining = raw_rows.len() > max_keys;
        raw_rows.truncate(max_keys);

        let mut entries = Vec::with_capacity(raw_rows.len());
        for (storage_key, record) in raw_rows {
            let Some((storage_hash, storage_range)) =
                molecule_key_codec::decode_hash_range_any(&storage_key)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode molecule key {storage_key}"
                )));
            };
            // A raw debug report retains a row whose selected decoder refuses.
            let api_range = codec.api_range(molecule, &storage_range).ok();
            let atom_content = self
                .get_atom_by_uuid(&record.entry.atom_uuid, storage_prefix)
                .await?
                .map(|atom| atom.content().clone());
            entries.push(MoleculeHashBucketRow {
                storage_key,
                storage_hash,
                storage_range,
                api_range,
                tip: record.entry,
                metadata: record.meta,
                atom_content,
            });
        }

        Ok(MoleculeHashBucketReport {
            molecule: molecule.to_string(),
            api_hash: api_hash.to_string(),
            storage_prefixes: prefixes.into(),
            rows: entries.len() as u64,
            more_remaining,
            entries,
        })
    }

    /// Read up to `want` distinct rows across possibly overlapping prefixes.
    ///
    /// A raw-row budget is incorrect here. A later prefix can repeat rows from
    /// an earlier prefix before it reaches a new row. Resume each prefix until
    /// it is exhausted or the distinct-row target is full.
    pub(super) async fn scan_distinct_per_key_prefixes(
        &self,
        prefixes: &[String],
        storage_prefix: Option<&str>,
        want: usize,
    ) -> Result<Vec<(String, crate::db_operations::atom_store::PerKeyRecord)>, SchemaError> {
        if want == 0 {
            return Ok(Vec::new());
        }

        let mut distinct = BTreeMap::new();
        for prefix in prefixes {
            let end = FilterUtils::create_prefix_end(prefix)?;
            let mut cursor: Option<String> = None;
            loop {
                if distinct.len() >= want {
                    break;
                }
                let remaining = want - distinct.len();
                let (batch, exhausted) = match cursor.as_deref() {
                    None => {
                        let rows = self
                            .scan_per_key_prefix_paged(prefix, storage_prefix, remaining)
                            .await?;
                        let exhausted = rows.len() < remaining;
                        (rows, exhausted)
                    }
                    Some(after) => {
                        let probe = remaining.saturating_add(1);
                        let rows = self
                            .scan_per_key_range_paged(after, &end, storage_prefix, probe)
                            .await?;
                        let exhausted = rows.len() < probe;
                        let advanced = rows
                            .into_iter()
                            .filter(|(key, _)| key.as_str() > after)
                            .collect();
                        (advanced, exhausted)
                    }
                };

                let Some((last, _)) = batch.last() else {
                    break;
                };
                cursor = Some(last.clone());
                for (key, record) in batch {
                    distinct.entry(key).or_insert(record);
                }
                if exhausted {
                    break;
                }
            }
            if distinct.len() >= want {
                break;
            }
        }
        Ok(distinct.into_iter().take(want).collect())
    }
}

/// Decode one `mk:` storage key to its (hash, range) pair and note it in `seen`.
///
/// The returned flag is true on the second and later sightings of a pair.
fn decode_molecule_key_row(
    key: &str,
    molecule: &str,
    seen: &mut HashMap<(String, String), usize>,
) -> (Option<String>, Option<String>, bool) {
    let decoded = molecule_key_codec::decode_hash_range(key, molecule).or_else(|| {
        // Storage-prefix form: strip outer prefix then decode bare mk: key.
        let bare = key
            .rsplit_once(":mk:")
            .map_or_else(|| key.to_string(), |(_, rest)| format!("mk:{rest}"));
        molecule_key_codec::decode_hash_range(&bare, molecule)
    });
    match decoded {
        Some((h, r)) => {
            let n = seen.entry((h.clone(), r.clone())).or_insert(0);
            *n += 1;
            (Some(h), Some(r), *n > 1)
        }
        None => (None, None, false),
    }
}

/// Second pass: mark every row that participates in a multi-row logical
/// identity, not only the second+ sightings (the first occurrence also
/// collides when n>1 after the walk), then total the report.
fn summarize_molecule_keys(
    molecule: &str,
    mut rows_out: Vec<MoleculeKeyRow>,
    seen: HashMap<(String, String), usize>,
    more: bool,
) -> MoleculeKeysReport {
    let multi: HashSet<(String, String)> = seen
        .into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|(k, _)| k)
        .collect();
    for row in &mut rows_out {
        if let (Some(h), Some(r)) = (&row.hash, &row.range) {
            row.collision = multi.contains(&(h.clone(), r.clone()));
        }
    }
    let collisions = rows_out.iter().filter(|r| r.collision).count() as u64;
    let unique = rows_out
        .iter()
        .filter_map(|r| match (&r.hash, &r.range) {
            (Some(h), Some(r)) => Some((h.as_str(), r.as_str())),
            _ => None,
        })
        .collect::<HashSet<_>>()
        .len() as u64;
    let undecodable = rows_out.iter().filter(|r| r.hash.is_none()).count() as u64;

    MoleculeKeysReport {
        molecule: molecule.to_string(),
        keys: rows_out.len() as u64,
        unique_keys: unique + undecodable,
        collision_rows: collisions,
        more_remaining: more,
        rows: rows_out,
    }
}

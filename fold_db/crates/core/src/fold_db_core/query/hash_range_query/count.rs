use super::{full_span_page, HashRangeQueryProcessor};
use crate::schema::types::field::{
    fetch_atoms_with_key_metadata_async_with_prefix, FieldValue, FieldVariant, HashRangeFilter,
    KeyedAtomMatch,
};
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};
use std::collections::{HashMap, HashSet};

impl HashRangeQueryProcessor {
    /// Count the distinct **live** rows a query over `schema` / `fields` would
    /// return -- the exact `total_count` for the paginated list path.
    ///
    /// **Tombstone-correct.** Resolves each candidate key's current value and
    /// excludes tombstones, matching the page materialization. Prefers the
    /// schema key field so count cost is not multiplied by field count. Also
    /// unions share-namespace keys. After that, applies the same named-layout
    /// retain as the page so a shared-molecule HashKey does not count sibling
    /// layouts the page dropped.
    ///
    /// `filter` scopes the count to the same rows the page path will read.
    /// `None` counts the schema's whole live row set. Passing a key-restricted
    /// filter counts that **partition** — which is what a `HashKey` read's
    /// `total_count` has to mean, or the caller pages a partition against the
    /// schema's row count and `has_more` never goes false. Only key-restricted
    /// filters belong here: `Page` / `PageAfter` bound how many rows come back
    /// rather than which ones exist, so counting under one would report the
    /// page size as the total.
    pub async fn count_rows(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        debug_assert!(
            filter.is_none_or(HashRangeFilter::is_key_restricted),
            "count_rows takes a key-restricted filter or none; \
             a scan-bounding filter would report the page size as the total"
        );
        let received_from_namespaces: Vec<String> = self.collect_received_from_namespaces().await;

        let count_field_names = Self::count_field_names(schema, fields);
        // The read path renames storage keys to API keys only when the primary
        // IS the schema's declared hash key field (`recover_api_page_keys`), so
        // the twin collapse below is gated on exactly that condition — and on
        // the molecule those keys live in, captured while the field is in hand.
        let key_hash_field = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_deref())
            .filter(|hash_field| count_field_names.iter().any(|name| name == hash_field))
            .map(ToString::to_string);
        let mut key_field_molecule: Option<String> = None;
        let mut key_field_storage_prefix: Option<String> = None;
        let mut keys: std::collections::HashSet<KeyValue> = std::collections::HashSet::new();
        for field_name in &count_field_names {
            let Some(field) = schema.runtime_fields.get_mut(field_name) else {
                continue;
            };
            if key_hash_field.as_deref() == Some(field_name.as_str()) {
                key_field_molecule = field.common().molecule_uuid().cloned();
                key_field_storage_prefix = field.common().storage_prefix().map(ToString::to_string);
            }

            // Cheap count: enumerate molecule keys + filter `KeyMetadata.tombstoned`
            // without fetching atom bodies. The tombstone gate is applied on the
            // returned metadata inside `live_field_keys_flag_only` — passing
            // `include_tombstones = false` down is not sufficient on its own.
            let own = self.live_field_keys_flag_only(field, None, filter).await?;
            keys.extend(own);

            for namespace in &received_from_namespaces {
                let shared = self
                    .live_field_keys_flag_only(field, Some(namespace.as_str()), filter)
                    .await?;
                keys.extend(shared);
            }
        }

        // Mirror the read path's multi-key-expand fallback. A sibling layout
        // can have no atoms under its own key field while sharing projected
        // field hashes with the layout that received the writes. Only fall
        // back when the preferred key spine is empty; a non-empty key spine is
        // authoritative and must not be unioned with sparse projection keys.
        if keys.is_empty() {
            if let Some(fallback) = Self::count_fallback_field(schema, fields) {
                if let Some(field) = schema.runtime_fields.get(&fallback) {
                    keys.extend(self.live_field_keys_flag_only(field, None, filter).await?);
                    for namespace in &received_from_namespaces {
                        keys.extend(
                            self.live_field_keys_flag_only(field, Some(namespace.as_str()), filter)
                                .await?,
                        );
                    }
                }
            }
        }

        // A resident write can publish its tip before the durable molecule
        // index contains the new key. The page path overlays that tip before
        // it hydrates rows, but this count runs first and supplies the page
        // window. Merge the resident key set here so a fresh partition cannot
        // be counted as empty and then windowed away.
        if let (Some(molecule_uuid), Some((hash, start, end))) = (
            key_field_molecule.as_deref(),
            Self::resident_partition_interval(filter),
        ) {
            let resident = self.db_ops.resident();
            if crate::db_operations::resident_read::tip_reads_resident_first(
                key_field_storage_prefix.as_deref(),
            ) {
                let start_key = crate::resident::ResidentMoleculeKey::new(&hash, &start);
                let end_key = end
                    .as_deref()
                    .map(|end| crate::resident::ResidentMoleculeKey::new(&hash, end));
                let snapshot = resident.resident_key_set_range(
                    molecule_uuid,
                    Some(&start_key),
                    end_key.as_ref(),
                );
                keys.extend(
                    snapshot
                        .keys
                        .into_iter()
                        .map(|key| KeyValue::new(Some(key.hash), Some(key.range))),
                );
                for key in snapshot.tombstones {
                    keys.remove(&KeyValue::new(Some(key.hash), Some(key.range)));
                }
            }
        }

        if let Some(molecule_uuid) = key_field_molecule {
            if self.db_ops.atoms().key_codec().encoding() != crate::atom::HashKeyEncoding::Plain {
                Self::collapse_legacy_plain_twins(&mut keys, |api_hash| {
                    self.db_ops
                        .atoms()
                        .storage_hash(&molecule_uuid, api_hash)
                        .ok()
                });
            }
        }

        // Skip Deletes ack by stamping a resident schema-key tombstone and
        // flushing the hard erase later. Count must match query_internal.
        let resident = self.db_ops.resident();
        let schema_name = schema.name.as_str();
        keys.retain(|kv| {
            let hash = kv.hash.as_deref().unwrap_or("");
            let range = kv.range.as_deref().unwrap_or("");
            !resident.is_schema_key_tombstoned(schema_name, hash, range)
        });

        // Mirror the page path's named-layout retain. Shared-molecule
        // graph-edge schemas union inbound and outbound tips under one
        // HashKey; count used to report that union while the page dropped
        // sibling-layout tips, so `has_more` never went false.
        self.retain_count_to_named_layout(
            schema,
            key_hash_field.as_deref(),
            filter,
            &received_from_namespaces,
            &mut keys,
        )
        .await?;

        Ok(keys.len())
    }

    /// Return the resident key interval for a partition count.
    ///
    /// The read resolver has the same interval rule. Keep this count-side
    /// copy explicit because the count must make its window decision before
    /// the page resolver runs.
    fn resident_partition_interval(
        filter: Option<&HashRangeFilter>,
    ) -> Option<(String, String, Option<String>)> {
        match filter? {
            HashRangeFilter::HashKey(hash) => Some((hash.clone(), String::new(), None)),
            HashRangeFilter::HashRangeRange { hash, start, end } => {
                Some((hash.clone(), start.clone(), Some(end.max(start).clone())))
            }
            HashRangeFilter::HashRangePrefix { hash, prefix } => {
                let mut upper = prefix.clone();
                let end = loop {
                    let Some(ch) = upper.pop() else { break None };
                    let next = match ch as u32 + 1 {
                        0xD800 => 0xE000,
                        n => n,
                    };
                    if let Some(next) = char::from_u32(next) {
                        upper.push(next);
                        break Some(upper);
                    }
                };
                Some((hash.clone(), prefix.clone(), end))
            }
            _ => None,
        }
    }

    /// Drop the legacy plain-key half of every mixed-encoding twin, so the
    /// count describes the rows the read path will actually serve.
    ///
    /// A record written before hash blinding has a plain-key molecule entry;
    /// rewritten after, it gains a blinded one. Both resolve to the same
    /// record, and the read path knows it: `rename_page_to_api_keys` maps the
    /// blinded key back to its API form, lands on the plain key already in the
    /// page, and **deliberately** serves one row — "serving them as two rows is
    /// the duplicate a caller cannot explain".
    ///
    /// The count never learned that rule. It counts storage keys, so it
    /// over-reports by exactly the number of twinned records, and since
    /// `has_more` is `served < total_count`, a client draining the set never
    /// sees it go false. Measured on the primary's brain `Task` partition
    /// (`4a67db42…`), 2026-08-17: `total_count` 16, `returned_count` 15,
    /// `has_more` true at `limit=2000` — a complete answer that reports itself
    /// incomplete. `brain reindex --child-task-index` refuses outright on that
    /// mismatch, which is the check working.
    ///
    /// The test is the inverse of the one `recover_api_page_keys` already runs
    /// and needs no atom bodies: a key is a legacy twin when blinding its own
    /// hash yields a *different* hash that the key set also holds under the
    /// same range. One HMAC per key, and it can only collapse a pair the read
    /// path is already collapsing. Matching ranges is required, not incidental
    /// — the recovered API key keeps the blinded entry's range, so a twin with
    /// a different range renames to a key of its own and stays two rows.
    ///
    /// Returns how many keys were dropped.
    fn collapse_legacy_plain_twins(
        keys: &mut std::collections::HashSet<KeyValue>,
        blind: impl Fn(&str) -> Option<String>,
    ) -> usize {
        let legacy: Vec<KeyValue> = keys
            .iter()
            .filter(|key| {
                let Some(hash) = key.hash.as_deref() else {
                    return false;
                };
                let Some(blinded) = blind(hash) else {
                    return false;
                };
                blinded != hash && keys.contains(&KeyValue::new(Some(blinded), key.range.clone()))
            })
            .cloned()
            .collect();
        for key in &legacy {
            keys.remove(key);
        }
        legacy.len()
    }

    /// Apply the page path's named-layout retain to the counted key set.
    ///
    /// Graph-edge keys need the named hash-field body. Membership-index keys
    /// (`#` count ≥ 2) stay without a body load, matching
    /// [`super::HashRangeQueryProcessor::retain_named_layout_primary`].
    async fn retain_count_to_named_layout(
        &self,
        schema: &Schema,
        hash_field_name: Option<&str>,
        filter: Option<&HashRangeFilter>,
        received_from_namespaces: &[String],
        keys: &mut HashSet<KeyValue>,
    ) -> Result<(), SchemaError> {
        let filter_owned = filter.cloned();
        let Some(wanted) = Self::named_layout_hash(&filter_owned) else {
            return Ok(());
        };
        let wanted = wanted.to_string();
        if keys.iter().all(Self::named_layout_membership_bypass) {
            return Ok(());
        }
        let Some(field_name) = hash_field_name else {
            keys.retain(Self::named_layout_membership_bypass);
            return Ok(());
        };
        let Some(field) = schema.runtime_fields.get(field_name) else {
            keys.retain(Self::named_layout_membership_bypass);
            return Ok(());
        };

        let need: HashSet<KeyValue> = keys
            .iter()
            .filter(|key| !Self::named_layout_membership_bypass(key))
            .cloned()
            .collect();

        let mut vals = HashMap::new();
        Self::merge_named_layout_values(
            &mut vals,
            fetch_atoms_with_key_metadata_async_with_prefix(
                &self.db_ops,
                self.live_field_matches_flag_only(field, None, filter)
                    .await?
                    .into_iter()
                    .filter(|(key, _, _, _, _)| need.contains(key)),
                field.common().storage_prefix(),
                field.common().molecule_uuid().map(String::as_str),
                Some(schema.name.as_str()),
                Some(field_name),
            )
            .await?,
        );
        for namespace in received_from_namespaces {
            Self::merge_named_layout_values(
                &mut vals,
                fetch_atoms_with_key_metadata_async_with_prefix(
                    &self.db_ops,
                    self.live_field_matches_flag_only(field, Some(namespace.as_str()), filter)
                        .await?
                        .into_iter()
                        .filter(|(key, _, _, _, _)| need.contains(key)),
                    Some(namespace.as_str()),
                    field.common().molecule_uuid().map(String::as_str),
                    Some(schema.name.as_str()),
                    Some(field_name),
                )
                .await?,
            );
        }

        keys.retain(|key| Self::named_layout_keep(&wanted, key, vals.get(key)));
        Ok(())
    }

    fn merge_named_layout_values(
        dest: &mut HashMap<KeyValue, FieldValue>,
        incoming: HashMap<KeyValue, FieldValue>,
    ) {
        for (key, value) in incoming {
            dest.entry(key).or_insert(value);
        }
    }

    /// Live keys for one field under an optional storage prefix, using the
    /// molecule tombstone flag only (no atom body loads).
    async fn live_field_keys_flag_only(
        &self,
        field: &FieldVariant,
        namespace: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) -> Result<Vec<KeyValue>, SchemaError> {
        Ok(self
            .live_field_matches_flag_only(field, namespace, filter)
            .await?
            .into_iter()
            .map(|(kv, _, _, _, _)| kv)
            .collect())
    }

    /// Tombstone-filtered molecule matches for one field (no atom body loads).
    async fn live_field_matches_flag_only(
        &self,
        field: &FieldVariant,
        namespace: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
        let mut working = field.cloned_without_molecule();
        if let Some(ns) = namespace {
            working
                .common_mut()
                .set_storage_prefix(Some(ns.to_string()));
        }
        let matches = working
            .collect_matches(
                &self.db_ops,
                Some(filter.cloned().unwrap_or_else(full_span_page)),
                None,
                false, // ask storage to drop KeyMetadata.tombstoned
            )
            .await?;
        // Re-apply the tombstone gate on the returned metadata rather than
        // trusting the `include_tombstones = false` we just passed down.
        //
        // That argument reaches `refresh_for_read`, which uses it only on the
        // NARROWED path; a molecule with no per-key header falls back to
        // `refresh_from_db`, a full materialize that takes no tombstone
        // argument at all. So on the fallback the gate is silently skipped and
        // every deleted key is counted live, while the page path drops them on
        // `rec.meta.tombstoned` — the count and the rows then disagree by
        // exactly the deleted set, with no signal that they did.
        //
        // Measured on the live primary before this fix, fkanban `Milestone`
        // projecting its own hash field: `total_count` 14,008, `returned_count`
        // 0, `unresolved_rows` 0. Every one of those keys is tombstoned;
        // `include_tombstones: true` returns them. A caller sees a table with
        // fourteen thousand rows that serves none of them.
        //
        // `collect_matches` already carries `KeyMetadata` per match (the third
        // tuple slot) and the page path already gates on it, so filtering here
        // costs nothing extra and uses the same signal the read does.
        Ok(matches
            .into_iter()
            .filter(|(_, _, key_meta, _, _)| !key_meta.as_ref().is_some_and(|meta| meta.tombstoned))
            .collect())
    }

    /// Pick the field set whose live keys ARE the query's row set.
    ///
    /// This has to name the same field the read uses as its key spine, because
    /// [`count_rows`](Self::count_rows) UNIONS the key sets of whatever it is
    /// given. The read is a co-key plan: `cokey_primary_field` picks the
    /// declared key field whenever it exists; that primary alone establishes
    /// the page of keys, and secondaries only fill values into keys the primary
    /// already produced. So a union over several projected fields counts keys
    /// that can never become rows.
    ///
    /// Returning every selected field is what made `total_count` exceed the
    /// table. Measured on the live primary, fkanban `Milestone` — 13,965
    /// records, neither projection naming the key field:
    ///
    /// ```text
    /// fields = ["slug"]           total_count = 14,191
    /// fields = ["slug","title"]   total_count = 22,216   <- union of two key sets
    /// ```
    ///
    /// A count that rises when you ask for another column, and that can exceed
    /// the number of rows in the table, is not counting rows. Mirroring the
    /// read's primary keeps `total_count` and `has_more` describing one row set.
    fn count_field_names(schema: &Schema, fields: &[String]) -> Vec<String> {
        let key_field_name = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
            .filter(|field_name| schema.runtime_fields.contains_key(*field_name));

        if let Some(key_field_name) = key_field_name {
            return vec![key_field_name.clone()];
        }

        // No key field to count by. Take the read's primary — its first
        // selected field — not the union of all of them.
        if let Some(primary) = fields.first() {
            return vec![primary.clone()];
        }

        // Unprojected AND keyless: there is no primary to mirror, so the whole
        // live row set is the honest answer.
        crate::record_molecule::declared_runtime_field_names(schema)
    }

    /// Projection fallback for a multi-key sibling whose own key field has no
    /// atoms. This is consulted only after the preferred key spine counted no
    /// rows, matching `query_cokey_with_sources`.
    fn count_fallback_field(schema: &Schema, fields: &[String]) -> Option<String> {
        let primary = Self::count_field_names(schema, fields).into_iter().next()?;
        if fields.iter().any(|field| field == &primary) {
            return None;
        }
        fields
            .iter()
            .find(|field| {
                field.as_str() != primary.as_str()
                    && schema.runtime_fields.contains_key(field.as_str())
            })
            .cloned()
    }
}

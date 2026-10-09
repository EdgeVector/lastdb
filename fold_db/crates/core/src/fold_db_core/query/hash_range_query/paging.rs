//! API page-key recovery and named-layout key handling.

use super::*;

impl HashRangeQueryProcessor {
    /// Recover API-form page keys from a **storage-form** primary walk.
    ///
    /// A paged / unfiltered walk returns each key in storage form. Under
    /// [`HashKeyEncoding::BlindV1`](crate::atom::HashKeyEncoding::BlindV1) the
    /// hash segment is `HMAC(index_key, molecule_uuid, api_hash)` — one-way and
    /// domain-separated **per molecule**, and every field is its own molecule
    /// (pinned by `one_api_hash_blinds_differently_per_field`). Secondary
    /// fields blind again when they resolve a slot
    /// (`load_per_key_records_for_slots` → `storage_hash`), so handing them the
    /// primary's storage-form hash blinds an already-blinded token. It names no
    /// slot in any other field's molecule, so the lookup misses for **every**
    /// row and the field is emitted as absent — a blank that a caller cannot
    /// tell apart from a genuine empty value.
    ///
    /// The plaintext hash is recoverable without any new persistence: for a
    /// schema with a declared hash key field, that field's decrypted *value*
    /// **is** the API hash. Re-blinding the value under the primary's molecule
    /// and requiring it to equal the segment the walk returned makes the
    /// recovery self-verifying — one HMAC per row, no guessing. A row that does
    /// not round-trip is left exactly as it is today, which is what keeps
    /// legacy plain-key rows (already API-form, so they blind to something
    /// else) working unchanged.
    ///
    /// Returns `storage key → API key` for the recovered rows only; empty when
    /// the home is plain-keyed, the schema declares no hash key field, or the
    /// co-key primary is not that field.
    pub(super) fn recover_api_page_keys(
        &self,
        schema: &Schema,
        primary_name: &str,
        primary_vals: &HashMap<KeyValue, FieldValue>,
    ) -> HashMap<KeyValue, KeyValue> {
        if self.db_ops.atoms().key_codec().encoding() == crate::atom::HashKeyEncoding::Plain {
            return HashMap::new();
        }
        let is_key_field = schema
            .key
            .as_ref()
            .and_then(|k| k.hash_field.as_deref())
            .is_some_and(|hash_field| hash_field == primary_name);
        if !is_key_field {
            return HashMap::new();
        }
        let Some(mol_uuid) = schema
            .runtime_fields
            .get(primary_name)
            .and_then(|f| f.common().molecule_uuid().cloned())
        else {
            return HashMap::new();
        };

        let mut recovered = HashMap::new();
        for (storage_key, value) in primary_vals {
            let (Some(storage_hash), Some(api_hash)) =
                (storage_key.hash.as_deref(), value.value.as_str())
            else {
                continue;
            };
            if self
                .db_ops
                .atoms()
                .storage_hash(&mol_uuid, api_hash)
                .is_ok_and(|blinded| blinded == storage_hash)
            {
                recovered.insert(
                    storage_key.clone(),
                    KeyValue::new(Some(api_hash.to_string()), storage_key.range.clone()),
                );
            }
        }
        recovered
    }

    /// Rename a resolved primary page from storage-form keys to API-form.
    ///
    /// This is the single place the two key spaces meet. Everything downstream —
    /// the secondary fan-out, the two-pass predicate path, and the `key.hash` the
    /// caller receives — reads the result, so a key that is not renamed here is a
    /// key that escapes: [`Self::recover_api_page_keys`] was already computed
    /// before this existed, but only the fan-out consumed it, which is why the
    /// blank-secondary symptom got fixed while the emitted key stayed unreadable.
    ///
    /// Returns the renamed rows, the page's keys, and the key sources rekeyed to
    /// match. A plain-key home renames nothing: the two forms are identical there
    /// and [`Self::recover_api_page_keys`] returns empty.
    ///
    /// **Collapsing:** a record with both a legacy plain-key entry and a blinded
    /// one — mixed-encoding writers forked the key space — has two storage keys
    /// that rename onto one API key, and the two rows become one. That is
    /// deliberate: they resolve to the same atom, and serving them as two rows is
    /// the duplicate a caller cannot explain. The recovered row wins, being the
    /// current-encoding entry, and the rule is total because only a
    /// recovered/not-recovered pair can collide (two rows of the same form
    /// sharing an API key are the same storage key).
    pub(super) fn rename_page_to_api_keys(
        &self,
        schema: &Schema,
        primary_name: &str,
        primary_vals: HashMap<KeyValue, FieldValue>,
        key_sources: HashMap<KeyValue, KeySource>,
    ) -> (
        HashMap<KeyValue, FieldValue>,
        Vec<KeyValue>,
        HashMap<KeyValue, KeySource>,
    ) {
        // A plain-key home stores the API form, so every key it hands back is
        // already addressable and there is nothing to recover or to warn about.
        let blinded =
            self.db_ops.atoms().key_codec().encoding() != crate::atom::HashKeyEncoding::Plain;

        let api_keys = self.recover_api_page_keys(schema, primary_name, &primary_vals);
        if api_keys.is_empty() {
            let page_keys: Vec<KeyValue> = primary_vals.keys().cloned().collect();
            // Nothing recovered. On a plain home that is the normal, correct
            // case. On a blinded home it means the walk's storage-form keys
            // escape as-is — the `Milestone`-shaped sibling-layout read, where
            // the declared key field has no atoms and the plan falls back to a
            // projected field, so `recover_api_page_keys` cannot run at all.
            // Those keys are HMAC tokens wearing a plaintext key's shape.
            let emitted = Self::countable_keys(&page_keys);
            if blinded {
                crate::db_operations::note_page_key_forms(0, emitted);
            } else {
                crate::db_operations::note_page_key_forms(emitted, 0);
            }
            return (primary_vals, page_keys, key_sources);
        }

        let mut renamed: HashMap<KeyValue, FieldValue> = HashMap::with_capacity(primary_vals.len());
        let mut sources: HashMap<KeyValue, KeySource> = HashMap::with_capacity(key_sources.len());
        // Slots filled by a recovered (round-trip-verified) key. Tracked here
        // rather than re-derived from `api_keys` afterwards so the count stays
        // O(page) — a 1000-row page must not pay a quadratic scan to be labelled.
        let mut verified_slots: std::collections::HashSet<KeyValue> =
            std::collections::HashSet::with_capacity(api_keys.len());
        for (storage_key, value) in primary_vals {
            let source = key_sources.get(&storage_key).cloned().unwrap_or(None);
            match api_keys.get(&storage_key) {
                // Recovered: takes the slot unconditionally, so it wins a collision
                // however the two entries happen to be iterated.
                Some(api_key) => {
                    sources.insert(api_key.clone(), source);
                    if renamed.insert(api_key.clone(), value).is_none() {
                        verified_slots.insert(api_key.clone());
                    }
                }
                // Not recovered — already API-form (a legacy plain key) or a row
                // whose value did not round-trip. Yields to a recovered row on the
                // same key.
                None => {
                    if let std::collections::hash_map::Entry::Vacant(slot) =
                        renamed.entry(storage_key.clone())
                    {
                        slot.insert(value);
                        sources.insert(storage_key, source);
                    }
                }
            }
        }

        let page_keys: Vec<KeyValue> = renamed.keys().cloned().collect();
        // A recovered key round-tripped through `storage_hash`, so it is
        // verified addressable. An unrecovered one on a blinded home is either
        // a legacy plain key or a row whose value did not round-trip; we cannot
        // tell which, so it is reported as NOT verified. Under-claiming is the
        // safe direction: a caller that re-reads a legacy key loses one probe,
        // where a caller that trusts a token gets an empty answer it reads as
        // missing data.
        let recovered = page_keys
            .iter()
            .filter(|k| k.hash.is_some() && verified_slots.contains(*k))
            .count() as u64;
        let emitted = Self::countable_keys(&page_keys);
        let unverified = emitted.saturating_sub(recovered);
        if blinded {
            crate::db_operations::note_page_key_forms(recovered, unverified);
        } else {
            crate::db_operations::note_page_key_forms(emitted, 0);
        }
        tracing::debug!(
            recovered = api_keys.len(),
            page_keys = page_keys.len(),
            "HashRangeQueryProcessor: renamed page to API-form keys"
        );
        (renamed, page_keys, sources)
    }

    /// Page keys that actually carry a hash worth classifying.
    ///
    /// A key with no hash component (a range-only schema) has nothing that
    /// could be mistaken for an addressable partition, so counting it would
    /// dilute the page's key form toward `Mixed` for no reader benefit.
    pub(super) fn countable_keys(keys: &[KeyValue]) -> u64 {
        keys.iter().filter(|k| k.hash.is_some()).count() as u64
    }

    /// Hash of a key-restricted filter that names one partition.
    ///
    /// Used to drop sibling-layout tips that share a molecule with this
    /// schema's hash field (same storage hash, different field value).
    pub(super) fn named_layout_hash(filter: &Option<HashRangeFilter>) -> Option<&str> {
        match filter.as_ref()? {
            HashRangeFilter::HashKey(hash)
            | HashRangeFilter::HashRangePrefix { hash, .. }
            | HashRangeFilter::HashRangeRange { hash, .. }
            | HashRangeFilter::HashRangeKey { hash, .. }
            | HashRangeFilter::HashRangePattern { hash, .. } => Some(hash.as_str()),
            _ => None,
        }
    }

    pub(super) fn field_value_is_hash(fv: &FieldValue, hash: &str) -> bool {
        match &fv.value {
            Value::String(s) => s == hash,
            other => other.as_str() == Some(hash),
        }
    }

    /// Membership indexes (BoardCards) use a three-part range
    /// (`column#position#slug`). Graph-edge planes use a two-part range
    /// (`type#slug`). Count of `#` is the layout discriminator — not a
    /// schema-name special case.
    pub(super) fn named_layout_membership_bypass(key: &KeyValue) -> bool {
        key.range
            .as_deref()
            .is_some_and(|range| range.bytes().filter(|&b| b == b'#').count() >= 2)
    }

    /// Keep a shared-molecule tip when the named hash-field equals the filter
    /// hash, or when the range is a membership index (see
    /// [`Self::named_layout_membership_bypass`]).
    ///
    /// `fv` is `None` when count has not hydrated the hash-field body. Then
    /// only the membership bypass keeps the key — graph-edge keys must wait
    /// for a value.
    pub(super) fn named_layout_keep(wanted: &str, key: &KeyValue, fv: Option<&FieldValue>) -> bool {
        fv.is_some_and(|fv| Self::field_value_is_hash(fv, wanted))
            || Self::named_layout_membership_bypass(key)
    }

    pub(super) fn retain_named_layout_primary(
        filter: &Option<HashRangeFilter>,
        primary_vals: &mut HashMap<KeyValue, FieldValue>,
        page_keys: &mut Vec<KeyValue>,
        key_sources: &mut HashMap<KeyValue, KeySource>,
    ) {
        let Some(wanted) = Self::named_layout_hash(filter) else {
            return;
        };
        // Graph-edge planes share a molecule and a two-part range (`type#slug`).
        // Keep a tip when the named hash-field equals the filter hash.
        // Membership indexes (BoardCards) use a three-part range
        // (`column#position#slug`). After a multi-key expand the catalog
        // hash_field can be a payload (`milestone`) while rows stay under the
        // original partition. Dropping those tips empties every board read.
        primary_vals.retain(|k, fv| Self::named_layout_keep(wanted, k, Some(fv)));
        page_keys.retain(|k| primary_vals.contains_key(k));
        key_sources.retain(|k, _| primary_vals.contains_key(k));
    }

    /// Primary field for co-key: the schema key field whenever one exists,
    /// otherwise the first projected field.
    ///
    /// The primary defines the row set; it is not merely a projected column.
    /// Choosing a sparse projected field here silently turns a projection into
    /// a row filter and makes `total_count` agree with the truncated answer.
    pub(super) fn cokey_primary_field(schema: &Schema, selected: &[String]) -> String {
        if let Some(key_field) = schema
            .key
            .as_ref()
            .and_then(|k| k.hash_field.as_ref().or(k.range_field.as_ref()))
        {
            if schema.runtime_fields.contains_key(key_field.as_str()) {
                return key_field.clone();
            }
        }
        selected[0].clone()
    }
}

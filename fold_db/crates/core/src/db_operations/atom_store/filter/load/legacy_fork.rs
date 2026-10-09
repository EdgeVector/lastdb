use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

impl AtomStore {
    /// Drop `mk:` keys left behind in an older hash encoding whose
    /// current-encoding twin is also present.
    ///
    /// A home that ever wrote under a different `HashKeyEncoding` has, for some
    /// records, TWO `mk:` entries: the legacy one keyed by the plaintext hash,
    /// and the current one keyed by its blinded form. They carry the same
    /// record, and the read boundary already knows it — `rename_page_to_api_keys`
    /// renames the blinded entry back to its plaintext API key, lands on the
    /// same key the legacy entry is already stored under, and documents that
    /// "the two rows become one, and the recovered row wins".
    ///
    /// That collapse only fires when both twins happen to fall in the SAME
    /// page. Across pages, each twin is delivered as its own row under the same
    /// API key, so a drain returns the record twice and the enumeration's size
    /// is the storage-key count rather than the row count. Measured on the live
    /// primary, `Papercut.slug`: 1039 `mk:` keys, 608 records, 431 forked — a
    /// three-page drain served 1039 rows of which 431 were repeats, and
    /// `total_count` (1039) described the storage keys while the caller
    /// advanced its offset by rows. `Reference.slug`: 2920 keys, 1040 forked.
    ///
    /// Deciding it here, where the whole key listing is in hand, is what makes
    /// the count and the page agree: `count_rows` and the page window both
    /// enumerate through this path, so both see one entry per record.
    ///
    /// Dropping the legacy twin rather than the current one matches the
    /// precedence the read already applies. It is also the only safe direction:
    /// writes go to the current encoding, so a forked record's legacy entry is
    /// frozen at whatever it held when the encoding changed.
    ///
    /// **A key with no twin is never dropped**, so a record that exists ONLY in
    /// legacy form still reads. And a home with no residue pays one shape check
    /// per key and allocates nothing — `forked_legacy_keys` returns empty
    /// before any HMAC or set is built.
    pub(super) fn forked_legacy_keys<'a>(
        &self,
        molecule_uuid: &str,
        entries: impl Iterator<Item = (&'a str, &'a str)> + Clone,
    ) -> std::collections::HashSet<(String, String)> {
        let mut forked = std::collections::HashSet::new();
        if self.key_codec().encoding() == crate::atom::HashKeyEncoding::Plain {
            // Nothing to fork onto: storage form IS API form.
            return forked;
        }
        // Cheap pre-pass. On a home that never changed encoding this is the
        // whole cost, and it is one length+alphabet check per key.
        let mut legacy = entries
            .clone()
            .filter(|(hash, _)| !hash.is_empty() && !self.key_codec().looks_like_storage_hash(hash))
            .peekable();
        if legacy.peek().is_none() {
            return forked;
        }
        let present: std::collections::HashSet<(&str, &str)> = entries.collect();
        for (hash, range) in legacy {
            let Ok(twin) = self.storage_hash(molecule_uuid, hash) else {
                continue;
            };
            if twin != hash && present.contains(&(twin.as_str(), range)) {
                forked.insert((hash.to_string(), range.to_string()));
            }
        }
        forked
    }

    /// [`Self::forked_legacy_keys`] for a caller that holds the RAW key
    /// listing and deliberately never materializes every decoded pair.
    ///
    /// [`Self::list_hash_range_keys_page_window`] selects its window through a
    /// bounded max-heap so peak decoded-pair memory is the page, not the field
    /// (`hashrange_list_query_index_load_does_not_scale_with_field_size`).
    /// Handing it a set built from every decoded pair would undo that, so this
    /// keeps only the LEGACY-form pairs — `O(residue)`, and empty on a home
    /// that never changed encoding — and takes a second decode pass to learn
    /// which of their twins exist. Same rule, same drop direction; only the
    /// memory profile differs.
    pub(super) fn forked_legacy_page_keys(
        &self,
        molecule_uuid: &str,
        keys: &[String],
        scan_prefix: &str,
    ) -> std::collections::HashSet<(String, String)> {
        let mut forked = std::collections::HashSet::new();
        if self.key_codec().encoding() == crate::atom::HashKeyEncoding::Plain {
            return forked;
        }
        let decode = |k: &String| {
            let suffix = k.strip_prefix(scan_prefix).unwrap_or(k.as_str());
            molecule_key_codec::decode_hash_range_suffix(suffix)
        };

        // Pass 1: the legacy-form keys and the twin each would collapse onto.
        let mut want_twin: std::collections::HashMap<(String, String), (String, String)> =
            std::collections::HashMap::new();
        for k in keys {
            let Some((hash, range)) = decode(k) else {
                continue;
            };
            if hash.is_empty() || self.key_codec().looks_like_storage_hash(&hash) {
                continue;
            }
            let Ok(twin) = self.storage_hash(molecule_uuid, &hash) else {
                continue;
            };
            if twin == hash {
                continue;
            }
            want_twin.insert((twin, range.clone()), (hash, range));
        }
        if want_twin.is_empty() {
            return forked;
        }

        // Pass 2: which of those twins are actually stored. A legacy key whose
        // twin is absent is the only copy of its record and stays.
        for k in keys {
            let Some(pair) = decode(k) else {
                continue;
            };
            if let Some(legacy) = want_twin.get(&pair) {
                forked.insert(legacy.clone());
            }
        }
        forked
    }
}

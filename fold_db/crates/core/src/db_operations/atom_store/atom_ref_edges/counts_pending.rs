use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

// Live-count reads, pending refs and bounded active-edge counts.
impl AtomStore {
    /// Read one atom's durable committed live-reference count.
    ///
    /// A missing row means zero only for an atom body that has not gained a
    /// committed tip. New atom bodies receive an explicit zero row at birth.
    pub async fn atom_live_ref_count(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<u64, SchemaError> {
        let key = atom_live_ref_count_key(atom_uuid, storage_prefix);
        self.main_store
            .get_item::<AtomLiveRefCount>(&key)
            .await
            // An old atom can have live tips without a complete counter.
            // Treat it as retained until an exact count is rebuilt.
            .map(|value| {
                let count = value.unwrap_or_default();
                if count.exact {
                    count.live_refs
                } else {
                    u64::MAX
                }
            })
            .map_err(|error| {
                SchemaError::InvalidData(format!("load atom live reference count: {error}"))
            })
    }

    /// Add an explicit zero counter beside a newly stored atom body.
    /// Existing counters are never reset when content-addressed writes dedupe.
    pub(crate) async fn missing_atom_ref_count_items(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
        let keyed_atoms: Vec<(String, String)> = atom_uuids
            .iter()
            .map(|atom_uuid| {
                (
                    atom_live_ref_count_key(atom_uuid, storage_prefix),
                    atom_uuid.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .collect();
        let keys: Vec<String> = keyed_atoms.iter().map(|(key, _)| key.clone()).collect();
        let stored = self
            .main_store
            .get_items::<AtomLiveRefCount>(&keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "probe atom live reference counts at birth: {error}"
                ))
            })?;
        let mut items = Vec::new();
        for ((key, atom_uuid), count) in keyed_atoms.into_iter().zip(stored) {
            if count.is_none() {
                // Pre-count atoms can already have live tips. Preserve them
                // until an offline rebuild can establish an exact count.
                let legacy = self
                    .has_active_atom_refs(&atom_uuid, storage_prefix)
                    .await?;
                items.push((
                    key.into_bytes(),
                    serde_json::to_vec(&AtomLiveRefCount {
                        live_refs: 0,
                        exact: !legacy,
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize initial atom live reference count: {error}"
                        ))
                    })?,
                ));
                if !legacy {
                    items.extend(Self::new_atom_gc_candidate_items(
                        &atom_uuid,
                        storage_prefix,
                        unix_nanos(),
                    )?);
                }
            }
        }
        Ok(items)
    }

    /// Build the exact pending-hold key for one in-flight operation.
    pub(crate) fn pending_atom_ref_key(
        atom_uuid: &str,
        source: &str,
        storage_prefix: Option<&str>,
    ) -> String {
        let atom_token = stable_token(atom_uuid.as_bytes());
        let source_token = stable_token(source.as_bytes());
        build_storage_key(
            storage_prefix,
            &format!("{ATOM_REF_PENDING_PREFIX}{atom_token}\0{source_token}"),
        )
    }

    /// Build one pending marker item. It retains crash evidence without an
    /// early count increment.
    pub(crate) fn pending_atom_ref_item(
        atom_uuid: &str,
        source: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(String, Value), SchemaError> {
        let marker = PendingAtomRef {
            atom_uuid: atom_uuid.to_string(),
            source: source.to_string(),
            created_at_unix_nanos: unix_nanos(),
        };
        Ok((
            Self::pending_atom_ref_key(atom_uuid, source, storage_prefix),
            serde_json::to_value(marker).map_err(|error| {
                SchemaError::InvalidData(format!("serialize pending atom reference: {error}"))
            })?,
        ))
    }

    /// True when one exact in-flight operation still holds the atom.
    pub async fn has_pending_atom_ref(
        &self,
        atom_uuid: &str,
        source: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        self.main_store
            .exists_item(&Self::pending_atom_ref_key(
                atom_uuid,
                source,
                storage_prefix,
            ))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe pending atom reference: {error}"))
            })
    }

    /// True when any in-flight operation holds this atom.
    pub async fn has_any_pending_atom_refs(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &format!(
                "{ATOM_REF_PENDING_PREFIX}{}\0",
                stable_token(atom_uuid.as_bytes())
            ),
        );
        self.main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe pending atom references: {error}"))
            })
    }

    /// Return whether the compact v2 partition contains an active edge.
    ///
    /// This reads at most two rows: the co-located count and one edge. A
    /// malformed marker returns an error, never a false zero. A purge caller
    /// must retain the candidate atom on that error.
    pub async fn has_active_atom_refs(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let prefix = atom_ref_v2_partition_prefix(atom_content_sha256, storage_prefix)?;
        let rows = self
            .main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 2)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("scan compact atom reverse edges: {error}"))
            })?;
        match rows
            .iter()
            .find(|(key, _)| !is_atom_live_ref_count_key(key))
        {
            None => Ok(false),
            Some((_, value)) if value.as_slice() == ATOM_REF_V2_ACTIVE_MARKER => Ok(true),
            Some((key, value)) => Err(SchemaError::InvalidData(format!(
                "compact atom reverse edge has invalid marker at {}: expected one byte 0x01, got {} byte(s)",
                String::from_utf8_lossy(key),
                value.len()
            ))),
        }
    }

    /// Count at most `limit` compact edges for one atom.
    ///
    /// This method uses one lookahead row to report truncation. It belongs on
    /// audit paths, not the purge request path.
    pub async fn count_active_atom_refs_bounded(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<AtomRefV2Count, SchemaError> {
        let prefix = atom_ref_v2_partition_prefix(atom_content_sha256, storage_prefix)?;
        let page_limit = limit.saturating_add(2);
        let rows = self
            .main_store
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), page_limit)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("count compact atom reverse edges: {error}"))
            })?;
        let edges: Vec<_> = rows
            .iter()
            .filter(|(key, _)| !is_atom_live_ref_count_key(key))
            .collect();
        for (key, value) in &edges {
            if value.as_slice() != ATOM_REF_V2_ACTIVE_MARKER {
                return Err(SchemaError::InvalidData(format!(
                    "compact atom reverse edge has invalid marker at {}: expected one byte 0x01, got {} byte(s)",
                    String::from_utf8_lossy(key),
                    value.len()
                )));
            }
        }
        Ok(AtomRefV2Count {
            active_edges: u64::try_from(edges.len().min(limit)).unwrap_or(u64::MAX),
            truncated: edges.len() > limit,
        })
    }
}

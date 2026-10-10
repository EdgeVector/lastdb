use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

// Edge key builders, history edge items and active-edge lookups.
impl AtomStore {
    pub(super) fn atom_ref_v2_mutations_for_v1_items(
        &self,
        items: &[(String, Value)],
        storage_prefix: Option<&str>,
    ) -> Result<CompactEdgePlan, SchemaError> {
        let mut active_edges = HashMap::new();
        let mut inactive_edges = HashMap::new();
        let mut converted_v1_keys = BTreeSet::new();
        for (key, value) in items {
            if !is_v1_atom_ref_edge_key(key, storage_prefix) {
                continue;
            }
            let edge: AtomRefEdge = serde_json::from_value(value.clone()).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "decode v1 atom reverse edge while writing compact v2 at {key}: {error}"
                ))
            })?;
            let Ok(v2_key) = edge.storage_key_v2(storage_prefix) else {
                continue;
            };
            converted_v1_keys.insert(key.clone());
            if edge.active {
                active_edges.insert(v2_key.into_bytes(), edge);
            } else {
                inactive_edges.insert(v2_key.into_bytes(), edge);
            }
        }
        for key in active_edges.keys() {
            inactive_edges.remove(key);
        }
        Ok(CompactEdgePlan {
            active_edges,
            inactive_edges,
            converted_v1_keys,
        })
    }

    /// Build the exact shadow-edge key for one live tip.
    pub(crate) fn atom_ref_tip_edge_key(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> String {
        tip_edge(molecule_uuid, disk_hash, disk_range, entry, true).storage_key(storage_prefix)
    }

    pub(in super::super) fn live_tip_ref_edge(
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        entry: &AtomEntry,
    ) -> AtomRefEdge {
        tip_edge(molecule_uuid, disk_hash, disk_range, entry, true)
    }

    /// Build the exact compact edge key for one live tip.
    pub(crate) fn atom_ref_tip_edge_v2_key(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> Result<String, SchemaError> {
        tip_edge(molecule_uuid, disk_hash, disk_range, entry, true).storage_key_v2(storage_prefix)
    }

    #[cfg(feature = "sharing")]
    pub(crate) fn inactive_atom_ref_tip_edge_item(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> Result<(String, Value), SchemaError> {
        let edge = tip_edge(molecule_uuid, disk_hash, disk_range, entry, false);
        let key = edge.storage_key(storage_prefix);
        let value = serde_json::to_value(edge)
            .map_err(|error| SchemaError::InvalidData(format!("serialize atom edge: {error}")))?;
        Ok((key, value))
    }

    /// Build every reverse edge for one authoritative `history:` row.
    pub(crate) fn mutation_history_atom_ref_edge_items(
        &self,
        event_key: &str,
        event: &MutationEvent,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        mutation_history_edge_items(event_key, event, storage_prefix)
    }

    /// Build the exact reverse-edge keys removed with one `history:` row.
    pub(crate) fn mutation_history_atom_ref_edge_keys(
        &self,
        event_key: &str,
        event: &MutationEvent,
        storage_prefix: Option<&str>,
    ) -> Vec<String> {
        mutation_history_edges(event_key, event)
            .into_iter()
            .map(|edge| edge.storage_key(storage_prefix))
            .collect()
    }

    /// Build the compact reverse-edge keys removed with one `history:` row.
    pub(crate) fn mutation_history_atom_ref_v2_edge_keys(
        &self,
        event_key: &str,
        event: &MutationEvent,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        Ok(mutation_history_edges(event_key, event)
            .into_iter()
            .filter_map(|edge| edge.storage_key_v2(storage_prefix).ok())
            .collect())
    }

    /// Build the exact shadow-edge key for one archived tip version.
    pub(crate) fn atom_ref_tip_version_edge_key(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        version_id: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> String {
        history_edge(molecule_uuid, disk_hash, disk_range, version_id, entry)
            .storage_key(storage_prefix)
    }

    /// Build the exact compact edge key for one archived tip version.
    pub(crate) fn atom_ref_tip_version_edge_v2_key(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        version_id: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> Result<String, SchemaError> {
        history_edge(molecule_uuid, disk_hash, disk_range, version_id, entry)
            .storage_key_v2(storage_prefix)
    }

    /// Build the derived edge keys for tip-version source rows that a prune
    /// will remove. The caller deletes each source row before these keys.
    pub(crate) async fn atom_ref_tip_version_delete_keys(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        tv_keys: &[String],
    ) -> Result<Vec<Vec<u8>>, SchemaError> {
        let mut out = Vec::with_capacity(tv_keys.len() * 2);
        for full_key in tv_keys {
            let Some(at) = full_key
                .rfind("tv\0")
                .or_else(|| full_key.rfind(molecule_key_codec::TIP_VERSION_PREFIX))
            else {
                continue;
            };
            let version_id = &full_key[at + 3..];
            if version_id.is_empty() {
                continue;
            }
            let storage_prefix = full_key[..at].strip_suffix(':').filter(|p| !p.is_empty());
            let entry: Option<AtomEntry> =
                self.main_store.get_item(full_key).await.map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "load tip version before atom edge delete: {e}"
                    ))
                })?;
            let Some(entry) = entry else {
                continue;
            };
            let edge = history_edge(molecule_uuid, disk_hash, disk_range, version_id, &entry);
            out.push(edge.storage_key(storage_prefix).into_bytes());
            out.push(edge.storage_key_v2(storage_prefix)?.into_bytes());
        }
        Ok(out)
    }

    /// Read only one candidate atom's reverse-edge partition.
    pub(crate) async fn active_atom_ref_edges_for_atom(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<AtomRefEdge>, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::atom_ref_edge_prefix(atom_uuid),
        );
        let partitioned = self
            .main_store
            .scan_items_with_prefix_partition_undecodable::<AtomRefEdge>(&prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan atom reverse edges: {e}")))?;
        if !partitioned.undecodable.is_empty() {
            return Err(SchemaError::InvalidData(format!(
                "atom reverse edge partition contains {} undecodable row(s)",
                partitioned.undecodable.len()
            )));
        }
        let mut edges: Vec<_> = partitioned
            .items
            .into_iter()
            .map(|(_, edge)| edge)
            .filter(|edge| edge.active)
            .collect();
        edges.sort_by_key(|edge| edge.storage_key(None));
        Ok(edges)
    }

    /// Return whether the compact reverse-edge plane contains a live reference.
    ///
    /// A present compact edge is authoritative immediately. Absence is
    /// authoritative after the global completion markers, or when the legacy
    /// v1 plane has no remaining live rows. An incomplete home that still has
    /// v1 rows returns an error so purge retains the candidate atom.
    pub(crate) async fn has_any_active_atom_refs(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        if self
            .has_active_atom_refs(atom_content_sha256, storage_prefix)
            .await?
        {
            return Ok(true);
        }
        if self.atom_ref_v2_reads_ready(storage_prefix).await? {
            return Ok(false);
        }
        match self.atom_ref_v1_keys_remain().await {
            Ok(false) => Ok(false),
            Ok(true) | Err(_) => Err(SchemaError::InvalidData(
                "compact atom reverse-edge absence requires exact global completion markers"
                    .to_string(),
            )),
        }
    }

    /// Check the immutable global v2 read gate with two bounded point reads.
    pub async fn atom_ref_v2_reads_ready(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let cache_key = storage_prefix.map(str::to_string);
        if self
            .atom_ref_v2_read_ready
            .lock()
            .map_err(|error| {
                SchemaError::InvalidData(format!("lock compact atom read gate: {error}"))
            })?
            .contains(&cache_key)
        {
            return Ok(true);
        }
        let markers = [
            (
                molecule_key_codec::ATOM_REF_V2_COMPLETE_KEY,
                "tip replay completion",
            ),
            (
                molecule_key_codec::ATOM_REF_V2_HISTORY_COMPLETE_KEY,
                "history completion",
            ),
        ];
        for (key, label) in markers {
            let key = build_storage_key(storage_prefix, key);
            let Some(value) =
                self.main_store
                    .inner()
                    .get(key.as_bytes())
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "read compact atom reverse-edge {label} marker: {error}"
                        ))
                    })?
            else {
                return Ok(false);
            };
            if value.as_slice() != ATOM_REF_V2_COMPLETE_MARKER {
                return Err(SchemaError::InvalidData(format!(
                    "compact atom reverse-edge {label} marker is invalid"
                )));
            }
        }
        self.atom_ref_v2_read_ready
            .lock()
            .map_err(|error| {
                SchemaError::InvalidData(format!("lock compact atom read gate: {error}"))
            })?
            .insert(cache_key);
        Ok(true)
    }

    /// Build all edge transitions for one molecule write.
    pub(crate) fn pending_atom_ref_edge_items(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut out = Vec::with_capacity(records.len() + data.pending_replaced_tips().len() * 2);

        for (record_key, record) in records {
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(record_key, molecule_uuid)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode molecule record while indexing atom references for {molecule_uuid}"
                )));
            };
            push_edge_item(
                &mut out,
                tip_edge(molecule_uuid, &disk_hash, &disk_range, &record.entry, true),
                storage_prefix,
            )?;
        }

        for (api_hash, api_range, old, archived_version_id) in data.pending_replaced_tips() {
            let disk_hash = self.storage_hash(molecule_uuid, api_hash)?;
            let disk_range = self.storage_range(molecule_uuid, api_range)?;
            push_edge_item(
                &mut out,
                tip_edge(molecule_uuid, &disk_hash, &disk_range, old, false),
                storage_prefix,
            )?;
            if let Some(version_id) = archived_version_id {
                push_edge_item(
                    &mut out,
                    history_edge(molecule_uuid, &disk_hash, &disk_range, version_id, old),
                    storage_prefix,
                )?;
            }
        }
        Ok(out)
    }

    /// Read active reverse edges for one candidate atom.
    pub async fn atom_ref_edges_for_atom(
        &self,
        atom_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<AtomRefEdgeLookup, SchemaError> {
        let edges = self
            .active_atom_ref_edges_for_atom(atom_uuid, storage_prefix)
            .await?;
        let complete = self.atom_ref_v2_reads_ready(storage_prefix).await?;
        Ok(AtomRefEdgeLookup { complete, edges })
    }
}

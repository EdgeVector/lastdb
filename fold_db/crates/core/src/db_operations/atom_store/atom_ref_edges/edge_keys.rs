use super::*;

// Edge key builders, history edge items and active-edge lookups.
impl AtomStore {
    pub(super) fn compact_edge_mutations_for_items(
        &self,
        items: &[(String, Value)],
        storage_prefix: Option<&str>,
    ) -> Result<CompactEdgePlan, SchemaError> {
        let mut active_edges = HashMap::new();
        let mut inactive_edges = HashMap::new();
        let mut source_edge_keys = BTreeSet::new();
        for (key, value) in items {
            if !is_atom_ref_edge_item_key(key, storage_prefix) {
                continue;
            }
            let edge: AtomRefEdge = serde_json::from_value(value.clone()).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "decode canonical atom reverse edge at {key}: {error}"
                ))
            })?;
            let v2_key = edge.storage_key_v2(storage_prefix)?;
            if v2_key != *key {
                return Err(SchemaError::InvalidData(
                    "atom reference key identity mismatch".into(),
                ));
            }
            source_edge_keys.insert(key.clone());
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
            source_edge_keys,
        })
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

    pub(crate) fn inactive_atom_ref_tip_edge_item(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        entry: &AtomEntry,
        storage_prefix: Option<&str>,
    ) -> Result<(String, Value), SchemaError> {
        let edge = tip_edge(molecule_uuid, disk_hash, disk_range, entry, false);
        let key = edge.storage_key_v2(storage_prefix)?;
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

    /// Build the compact reverse-edge keys removed with one `history:` row.
    pub(crate) fn mutation_history_atom_ref_v2_edge_keys(
        &self,
        event_key: &str,
        event: &MutationEvent,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        mutation_history_edges(event_key, event)
            .into_iter()
            .map(|edge| edge.storage_key_v2(storage_prefix))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Return whether the compact reverse-edge plane contains a live reference.
    ///
    /// A present scoped edge keeps the atom immediately. Absence authorizes
    /// removal only after the home-wide completion proof. A missing proof
    /// returns an error; an older reference plane cannot grant this authority.
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
        if self.atom_ref_v2_reads_ready().await? {
            return Ok(false);
        }
        Err(SchemaError::InvalidData(
            "compact atom reverse-edge absence requires exact global completion markers"
                .to_string(),
        ))
    }

    /// Check both global current-format proofs in one bounded batch read.
    /// Data prefixes never select a different proof: fresh startup stamps the
    /// home once, and all current writers maintain references in every scope.
    pub async fn atom_ref_v2_reads_ready(&self) -> Result<bool, SchemaError> {
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
        let keys = markers
            .iter()
            .map(|(key, _)| key.as_bytes().to_vec())
            .collect();
        let values = self.main_store.inner().get_many(keys).await?;
        if values.len() != markers.len() {
            return Err(SchemaError::InvalidData(
                "reference proof read returned misaligned slots".into(),
            ));
        }
        for ((_, label), value) in markers.into_iter().zip(values) {
            let Some(value) = value else { return Ok(false) };
            if value.as_slice() != ATOM_REF_V2_COMPLETE_MARKER {
                return Err(SchemaError::InvalidData(format!(
                    "compact atom reference {label} marker is invalid"
                )));
            }
        }
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
        let mut out = Vec::with_capacity(records.len() + data.pending_replaced_tips().len());

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

        for (api_hash, api_range, old) in data.pending_replaced_tips() {
            let disk_hash = self.storage_hash(molecule_uuid, api_hash)?;
            let disk_range = self.storage_range(molecule_uuid, api_range)?;
            push_edge_item(
                &mut out,
                tip_edge(molecule_uuid, &disk_hash, &disk_range, old, false),
                storage_prefix,
            )?;
        }
        Ok(out)
    }
}

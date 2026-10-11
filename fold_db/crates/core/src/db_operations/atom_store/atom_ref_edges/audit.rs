use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

impl AtomStore {
    /// Audit every authoritative edge for one molecule with point reads.
    pub async fn audit_atom_ref_v2_molecule(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<AtomRefV2MoleculeAuditReport, SchemaError> {
        let (expected, slots_audited, history_rows_audited) = self
            .expected_atom_ref_v2_keys_for_molecule(molecule_uuid, storage_prefix)
            .await?;
        let mut indexed_active_edges = 0u64;
        let mut missing_edges = 0u64;
        let mut invalid_live_keys = 0u64;
        let mut false_zero_atoms = BTreeSet::new();
        let keys: Vec<Vec<u8>> = expected.keys().map(|key| key.as_bytes().to_vec()).collect();
        for chunk in keys.chunks(64) {
            let values = self
                .main_store
                .inner()
                .get_many(chunk.to_vec())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "audit compact atom reverse edges for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for (key, value) in chunk.iter().zip(values) {
                let key_text = String::from_utf8_lossy(key).into_owned();
                match value {
                    Some(value) if value.as_slice() == ATOM_REF_V2_ACTIVE_MARKER => {
                        indexed_active_edges = indexed_active_edges.saturating_add(1);
                    }
                    Some(_) => {
                        invalid_live_keys = invalid_live_keys.saturating_add(1);
                        if let Some(atom) = expected.get(&key_text) {
                            false_zero_atoms.insert(atom.clone());
                        }
                    }
                    None => {
                        missing_edges = missing_edges.saturating_add(1);
                        if let Some(atom) = expected.get(&key_text) {
                            false_zero_atoms.insert(atom.clone());
                        }
                    }
                }
            }
        }
        Ok(AtomRefV2MoleculeAuditReport {
            expected_active_edges: expected.len() as u64,
            indexed_active_edges,
            missing_edges,
            invalid_live_keys,
            false_zero_reference_atoms: false_zero_atoms.len() as u64,
            slots_audited,
            history_rows_audited,
        })
    }

    pub(super) async fn expected_atom_ref_v2_keys_for_molecule(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(HashMap<String, String>, u64, u64), SchemaError> {
        let prefix = build_storage_key(storage_prefix, &format!("mk:{molecule_uuid}:"));
        let end = FilterUtils::create_prefix_end(&prefix);
        let rows = self
            .main_store
            .inner()
            .scan_range(prefix.as_bytes(), end.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "audit compact molecule slots for {molecule_uuid}: {error}"
                ))
            })?;
        let slots_audited = rows.len() as u64;
        let mut expected = HashMap::new();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                return Err(SchemaError::InvalidData(format!(
                    "compact molecule row has the wrong storage prefix: {full_key}"
                )));
            };
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                return Err(SchemaError::InvalidData(format!(
                    "cannot decode compact molecule row {full_key}"
                )));
            };
            let record: PerKeyRecord = serde_json::from_slice(&value).map_err(|error| {
                SchemaError::InvalidData(format!("decode compact molecule row {full_key}: {error}"))
            })?;
            for edge in self
                .reference_edges_for_record(
                    molecule_uuid,
                    &disk_hash,
                    &disk_range,
                    &record,
                    storage_prefix,
                )
                .await?
            {
                expected.insert(edge.storage_key_v2(storage_prefix)?, edge.atom_uuid);
            }
        }

        let history_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::history_molecule_prefix(molecule_uuid),
        );
        let history_end = FilterUtils::create_prefix_end(&history_prefix);
        let history_rows = self
            .main_store
            .inner()
            .scan_range(history_prefix.as_bytes(), history_end.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "audit compact molecule history for {molecule_uuid}: {error}"
                ))
            })?;
        let history_rows_audited = history_rows.len() as u64;
        for (key, value) in history_rows {
            let event_key = String::from_utf8_lossy(&key).into_owned();
            let event: MutationEvent = serde_json::from_slice(&value).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "decode compact mutation history {event_key}: {error}"
                ))
            })?;
            if event.molecule_uuid != molecule_uuid {
                return Err(SchemaError::InvalidData(format!(
                    "history row {event_key} belongs to molecule {}, expected {molecule_uuid}",
                    event.molecule_uuid
                )));
            }
            for edge in mutation_history_edges(&event_key, &event) {
                expected.insert(edge.storage_key_v2(storage_prefix)?, edge.atom_uuid);
            }
        }
        Ok((expected, slots_audited, history_rows_audited))
    }

    pub(super) async fn reference_edges_for_record(
        &self,
        molecule_uuid: &str,
        disk_hash: &str,
        disk_range: &str,
        record: &PerKeyRecord,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<AtomRefEdge>, SchemaError> {
        let mut edges = vec![tip_edge(
            molecule_uuid,
            disk_hash,
            disk_range,
            &record.entry,
            true,
        )];
        let mut version_id = record.entry.prev_tip_id.clone();
        let mut seen = BTreeSet::new();
        for _ in 0..MAX_TIP_CHAIN_WALK {
            if version_id.is_empty() {
                return Ok(edges);
            }
            if !seen.insert(version_id.clone()) {
                return Err(SchemaError::InvalidData(format!(
                    "tip-version cycle while reconstructing atom refs for {molecule_uuid}"
                )));
            }
            let Some(entry) = self.get_tip_version(&version_id, storage_prefix).await? else {
                return Ok(edges);
            };
            edges.push(history_edge(
                molecule_uuid,
                disk_hash,
                disk_range,
                &version_id,
                &entry,
            ));
            version_id = entry.prev_tip_id.clone();
        }
        Err(SchemaError::InvalidData(format!(
            "tip-version chain exceeded {MAX_TIP_CHAIN_WALK} rows for {molecule_uuid}"
        )))
    }
}

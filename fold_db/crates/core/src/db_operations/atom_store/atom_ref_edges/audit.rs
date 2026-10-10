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

    /// Compare the full compact plane with authoritative rows on an isolated copy.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn audit_atom_ref_v2_edges_on_isolated_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<AtomRefV2AuditReport, SchemaError> {
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let rows = self
            .main_store
            .inner()
            .scan_range(mk_prefix.as_bytes(), mk_end.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("audit compact scan mk: {error}")))?;
        let slots_audited = rows.len() as u64;
        let mut expected = HashMap::new();
        let mut molecules = BTreeSet::new();
        for (key, value) in rows {
            let full_key = String::from_utf8_lossy(&key).into_owned();
            let Some(base_key) = strip_storage_prefix(storage_prefix, &full_key) else {
                continue;
            };
            let Some(rest) = base_key.strip_prefix("mk:") else {
                continue;
            };
            let Some((molecule_uuid, _)) = rest.split_once(':') else {
                continue;
            };
            let Some((disk_hash, disk_range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                continue;
            };
            molecules.insert(molecule_uuid.to_string());
            let record: PerKeyRecord = serde_json::from_slice(&value).map_err(|error| {
                SchemaError::InvalidData(format!("audit compact decode {full_key}: {error}"))
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

        let history_prefix = build_storage_key(storage_prefix, "history:");
        let history_end = FilterUtils::create_prefix_end(&history_prefix);
        let history_rows = self
            .main_store
            .inner()
            .scan_range(history_prefix.as_bytes(), history_end.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit compact scan history: {error}"))
            })?;
        let history_rows_audited = history_rows.len() as u64;
        for (key, value) in history_rows {
            let event_key = String::from_utf8_lossy(&key).into_owned();
            let event: MutationEvent = serde_json::from_slice(&value).map_err(|error| {
                SchemaError::InvalidData(format!("audit compact decode {event_key}: {error}"))
            })?;
            molecules.insert(event.molecule_uuid.clone());
            for edge in mutation_history_edges(&event_key, &event) {
                expected.insert(edge.storage_key_v2(storage_prefix)?, edge.atom_uuid);
            }
        }

        let edge_prefix = build_storage_key(storage_prefix, ATOM_REF_V2_PREFIX);
        let edge_end = FilterUtils::create_prefix_end(&edge_prefix);
        let indexed_rows = self
            .main_store
            .inner()
            .scan_range(edge_prefix.as_bytes(), edge_end.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit compact scan aref:v2: {error}"))
            })?;
        let mut indexed = BTreeSet::new();
        let mut invalid_live_keys = 0u64;
        for (key, value) in indexed_rows {
            if is_atom_live_ref_count_key(&key) {
                continue;
            }
            if value.as_slice() == ATOM_REF_V2_ACTIVE_MARKER {
                indexed.insert(String::from_utf8_lossy(&key).into_owned());
            } else {
                invalid_live_keys = invalid_live_keys.saturating_add(1);
            }
        }
        let expected_keys: BTreeSet<_> = expected.keys().cloned().collect();
        let missing: Vec<_> = expected_keys.difference(&indexed).cloned().collect();
        let unexpected_edges = indexed.difference(&expected_keys).count() as u64;
        let false_zero_reference_atoms = missing
            .iter()
            .filter_map(|key| expected.get(key))
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let complete_key =
            build_storage_key(storage_prefix, molecule_key_codec::ATOM_REF_V2_COMPLETE_KEY);
        let mut complete = self
            .main_store
            .exists_item(&complete_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit compact completeness marker: {error}"))
            })?
            && self.atom_ref_v2_history_complete(storage_prefix).await?;
        if complete {
            for molecule_uuid in &molecules {
                let manifest = self
                    .atom_ref_v2_molecule_manifest(molecule_uuid, storage_prefix)
                    .await?;
                if !manifest.is_some_and(|manifest| {
                    manifest.replay_complete
                        && manifest.version >= ATOM_REF_MANIFEST_VERSION_HISTORY
                }) {
                    complete = false;
                    break;
                }
            }
        }
        Ok(AtomRefV2AuditReport {
            expected_active_edges: expected_keys.len() as u64,
            indexed_active_edges: indexed.len() as u64,
            missing_edges: missing.len() as u64,
            unexpected_edges,
            invalid_live_keys,
            false_zero_reference_atoms,
            complete,
            molecules_audited: molecules.len() as u64,
            slots_audited,
            history_rows_audited,
        })
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

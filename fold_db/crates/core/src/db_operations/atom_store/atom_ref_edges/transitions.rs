use super::super::atom_ref_gc::AtomGcCandidate;
use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

// Source transitions, v2 commit and live-count mutation planning.
impl AtomStore {
    /// Commit one existing JSON put batch with compact edge changes.
    ///
    /// Active compact edges precede every source put. Compact edge deletes
    /// follow every source put. This preserves the safe crash-prefix order of
    /// [`AtomRefV2Transition`] while it also supports existing multi-row write
    /// batches. An active edge in the same batch cancels a matching delete.
    ///
    /// SHA-256 atom identities write compact v2 keys and omit the v1 JSON row.
    /// Non-hex test identities keep the v1 JSON row so existing fixtures still
    /// store.
    pub(crate) async fn batch_put_items_with_atom_ref_v2(
        &self,
        items: Vec<(String, Value)>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let _coverage = self.invalidate_coverage_during(
            items.iter().filter_map(|(key, _)| {
                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key)
            }),
            storage_prefix,
        );
        let plan = self.atom_ref_v2_mutations_for_v1_items(&items, storage_prefix)?;
        let CompactEdgePlan {
            active_edges,
            inactive_edges,
            converted_v1_keys,
        } = plan;
        if active_edges.is_empty() && inactive_edges.is_empty() {
            return self
                .main_store
                .batch_put_items(items)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("commit source-row put batch: {error}"))
                });
        }

        let mut count_atoms: Vec<String> = active_edges
            .values()
            .chain(inactive_edges.values())
            .filter(|edge| edge.edge_type == AtomRefEdgeType::Tip)
            .map(|edge| edge.atom_uuid.clone())
            .collect();
        count_atoms.sort_unstable();
        count_atoms.dedup();
        let _count_guards = self.lock_atom_ref_counts(&count_atoms).await;

        let active_edge_puts: Vec<KvMutation> = active_edges
            .keys()
            .cloned()
            .map(|key| KvMutation::put(key, ATOM_REF_V2_ACTIVE_MARKER))
            .collect();
        let inactive_edge_deletes: Vec<KvMutation> = inactive_edges
            .keys()
            .cloned()
            .map(KvMutation::delete)
            .collect();
        let count_puts = self
            .atom_live_ref_count_mutations(&active_edges, &inactive_edges, storage_prefix)
            .await?;

        let mut mutations = Vec::with_capacity(
            active_edge_puts.len() + count_puts.len() + items.len() + inactive_edge_deletes.len(),
        );
        mutations.extend(active_edge_puts.iter().cloned());
        mutations.extend(count_puts);
        for (key, value) in &items {
            if converted_v1_keys.contains(key) {
                continue;
            }
            let value = serde_json::to_vec(value).map_err(|error| {
                SchemaError::InvalidData(format!("serialize source-row put batch: {error}"))
            })?;
            mutations.push(KvMutation::put(key.as_bytes().to_vec(), value));
        }
        mutations.extend(inactive_edge_deletes.iter().cloned());
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "commit source rows with compact atom reverse edges: {error}"
                ))
            })
    }

    pub(in super::super) async fn atom_live_ref_count_mutations(
        &self,
        active_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        inactive_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let molecule_weights = self
            .tip_molecule_weights(active_edges, inactive_edges, storage_prefix)
            .await?;
        let deltas = self
            .tip_transition_deltas(active_edges, inactive_edges, &molecule_weights)
            .await?;

        let mut atoms: Vec<_> = deltas.into_iter().collect();
        atoms.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let count_keys: Vec<String> = atoms
            .iter()
            .map(|(atom_uuid, _)| atom_live_ref_count_key(atom_uuid, storage_prefix))
            .collect();
        let stored_counts = self
            .main_store
            .get_items::<AtomLiveRefCount>(&count_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load atom live reference counts before transition: {error}"
                ))
            })?;
        if stored_counts.len() != atoms.len() {
            return Err(SchemaError::InvalidData(
                "atom live reference count batch returned misaligned slots".into(),
            ));
        }
        if let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        {
            runtime.record_read_stage("atom_ref_live_counts", atoms.len());
        }
        let candidate_atoms: Vec<String> = atoms
            .iter()
            .filter(|(_, delta)| *delta != 0)
            .map(|(atom_uuid, _)| atom_uuid.clone())
            .collect();
        let candidates = if candidate_atoms.is_empty() {
            Vec::new()
        } else {
            self.load_atom_gc_candidates(&candidate_atoms, storage_prefix)
                .await?
        };
        if candidates.len() != candidate_atoms.len() {
            return Err(SchemaError::InvalidData(
                "atom GC candidate batch returned misaligned slots".into(),
            ));
        }
        if let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        {
            runtime.record_read_stage("atom_ref_gc_candidates", candidate_atoms.len());
        }
        let mut candidates = candidates.into_iter();
        let mut mutations = Vec::with_capacity(atoms.len());
        for (((atom_uuid, delta), key), stored) in
            atoms.into_iter().zip(count_keys).zip(stored_counts)
        {
            if delta == 0 {
                continue;
            }
            self.apply_live_ref_delta(
                &mut mutations,
                &atom_uuid,
                delta,
                key,
                (
                    stored,
                    candidates.next().expect("validated candidate batch slots"),
                ),
                storage_prefix,
            )
            .await?;
        }
        Ok(mutations)
    }

    /// Weight of each tip molecule: one, plus its catalog refs when present.
    async fn tip_molecule_weights(
        &self,
        active_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        inactive_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<String, u64>, SchemaError> {
        let mut molecule_weights = HashMap::new();
        let mut molecules: BTreeSet<String> = BTreeSet::new();
        molecules.extend(
            active_edges
                .values()
                .chain(inactive_edges.values())
                .filter(|edge| edge.edge_type == AtomRefEdgeType::Tip)
                .map(|edge| edge.molecule_uuid.clone()),
        );
        let catalog_refs_present = self.database_catalog_refs_present(storage_prefix).await?;
        if catalog_refs_present {
            let molecules: Vec<String> = molecules.into_iter().collect();
            for (molecule, catalog_refs) in self
                .database_catalog_ref_counts_for_molecules(&molecules, storage_prefix)
                .await?
            {
                molecule_weights.insert(molecule, 1_u64.saturating_add(catalog_refs));
            }
        } else {
            molecule_weights.extend(molecules.into_iter().map(|molecule| (molecule, 1)));
        }
        Ok(molecule_weights)
    }

    /// Net live-ref change per atom for the tip edges whose existence flips.
    async fn tip_transition_deltas(
        &self,
        active_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        inactive_edges: &HashMap<Vec<u8>, AtomRefEdge>,
        molecule_weights: &HashMap<String, u64>,
    ) -> Result<HashMap<String, i128>, SchemaError> {
        let mut deltas: HashMap<String, i128> = HashMap::new();
        let probes: Vec<(bool, &AtomRefEdge, Vec<u8>)> = active_edges
            .iter()
            .filter(|(_, edge)| edge.edge_type == AtomRefEdgeType::Tip)
            .map(|(key, edge)| (true, edge, key.clone()))
            .chain(
                inactive_edges
                    .iter()
                    .filter(|(_, edge)| edge.edge_type == AtomRefEdgeType::Tip)
                    .map(|(key, edge)| (false, edge, key.clone())),
            )
            .collect();
        let edge_exists = self
            .main_store
            .inner()
            .exists_many(probes.iter().map(|(_, _, key)| key.clone()).collect())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "probe atom references before count transition: {error}"
                ))
            })?;
        if let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        {
            runtime.record_read_stage("atom_ref_edge_probes", probes.len());
        }
        for ((activate, edge, _), exists) in probes.into_iter().zip(edge_exists) {
            if activate == exists {
                continue;
            }
            let weight = molecule_weights
                .get(&edge.molecule_uuid)
                .copied()
                .unwrap_or(1);
            let delta = if activate {
                i128::from(weight)
            } else {
                -i128::from(weight)
            };
            *deltas.entry(edge.atom_uuid.clone()).or_default() += delta;
        }
        Ok(deltas)
    }

    /// Append the count and GC-candidate mutations for one atom's delta.
    async fn apply_live_ref_delta(
        &self,
        mutations: &mut Vec<KvMutation>,
        atom_uuid: &str,
        delta: i128,
        key: String,
        loaded: (Option<AtomLiveRefCount>, Option<AtomGcCandidate>),
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let (stored, candidate) = loaded;
        if stored.is_none()
            && (delta.is_negative() || self.has_active_atom_refs(atom_uuid, storage_prefix).await?)
        {
            // A legacy tip can lack a counter. Retain its atom until a
            // complete count rebuild; absence must not mean zero.
            return Self::mark_live_ref_count_unknown(
                mutations,
                atom_uuid,
                key,
                candidate,
                storage_prefix,
            );
        }
        let stored = stored.unwrap_or_default();
        if !stored.exact {
            mutations.extend(Self::atom_gc_candidate_mutations_for_loaded_count(
                atom_uuid,
                u64::MAX,
                u64::MAX,
                candidate,
                storage_prefix,
            )?);
            return Ok(());
        }
        let before = stored.live_refs;
        let after = i128::from(before) + delta;
        if !(0..=i128::from(u64::MAX)).contains(&after) {
            // An old atom can have a seeded zero counter and uncounted
            // tips. An underflow makes its count unknown, never zero.
            if after.is_negative() {
                return Self::mark_live_ref_count_unknown(
                    mutations,
                    atom_uuid,
                    key,
                    candidate,
                    storage_prefix,
                );
            }
            return Err(SchemaError::InvalidData(format!(
                "atom live reference count transition is out of range for {atom_uuid}: {before} {delta:+}"
            )));
        }
        let value = serde_json::to_vec(&AtomLiveRefCount {
            live_refs: after as u64,
            exact: true,
        })
        .map_err(|error| {
            SchemaError::InvalidData(format!(
                "serialize atom live reference count transition: {error}"
            ))
        })?;
        mutations.push(KvMutation::put(key.into_bytes(), value));
        mutations.extend(Self::atom_gc_candidate_mutations_for_loaded_count(
            atom_uuid,
            before,
            after as u64,
            candidate,
            storage_prefix,
        )?);
        Ok(())
    }

    /// Store an inexact zero count and mark the atom as a GC candidate.
    fn mark_live_ref_count_unknown(
        mutations: &mut Vec<KvMutation>,
        atom_uuid: &str,
        key: String,
        candidate: Option<AtomGcCandidate>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let value = serde_json::to_vec(&AtomLiveRefCount {
            live_refs: 0,
            exact: false,
        })
        .map_err(|error| {
            SchemaError::InvalidData(format!(
                "serialize legacy atom live reference count: {error}"
            ))
        })?;
        mutations.push(KvMutation::put(key.into_bytes(), value));
        mutations.extend(Self::atom_gc_candidate_mutations_for_loaded_count(
            atom_uuid,
            u64::MAX,
            u64::MAX,
            candidate,
            storage_prefix,
        )?);
        Ok(())
    }
}

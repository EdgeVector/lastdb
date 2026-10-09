use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

impl AtomStore {
    /// Prepare the atom paths added by one database-catalog schema copy.
    /// Pending rows land before the catalog becomes live and do not alter the
    /// committed count.
    pub(crate) async fn prepare_catalog_atom_refs(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<CatalogAtomRefPlan, SchemaError> {
        self.catalog_atom_ref_plan(
            db_locator,
            schema_name,
            schema,
            storage_prefix,
            CatalogAtomRefTransitionKind::Add,
        )
        .await
    }

    /// Prepare the paths removed after one catalog membership disappears.
    pub(crate) async fn prepare_catalog_atom_ref_removal(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
    ) -> Result<CatalogAtomRefPlan, SchemaError> {
        self.catalog_atom_ref_plan(
            db_locator,
            schema_name,
            schema,
            storage_prefix,
            CatalogAtomRefTransitionKind::Remove,
        )
        .await
    }

    pub(super) async fn catalog_atom_ref_plan(
        &self,
        db_locator: &str,
        schema_name: &str,
        schema: &Schema,
        storage_prefix: Option<&str>,
        kind: CatalogAtomRefTransitionKind,
    ) -> Result<CatalogAtomRefPlan, SchemaError> {
        let create_pending = kind == CatalogAtomRefTransitionKind::Add;
        let mut fields: Vec<(String, String)> = schema
            .field_molecule_uuids
            .as_ref()
            .into_iter()
            .flat_map(|molecules| molecules.iter())
            .map(|(field, molecule)| (field.clone(), molecule.clone()))
            .collect();
        fields.sort_unstable();
        let mut molecule_edges = Vec::new();
        let mut atom_deltas = HashMap::new();
        let mut pending_items = Vec::new();
        let mut pending_keys = Vec::new();
        for (field, molecule_uuid) in fields {
            let edge = super::super::MoleculeRefEdge::database_catalog_field(
                db_locator,
                schema_name,
                &field,
                &molecule_uuid,
            );
            let edge_exists = self
                .main_store
                .exists_item(&edge.storage_key(storage_prefix))
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "probe database-catalog molecule reference: {error}"
                    ))
                })?;
            if edge_exists == create_pending {
                continue;
            }
            if let Some(molecule) = self
                .load_molecule_per_key(&molecule_uuid, storage_prefix)
                .await?
            {
                for (hash, range, entry, _) in molecule.per_key_records() {
                    *atom_deltas.entry(entry.atom_uuid.clone()).or_default() += 1;
                    if create_pending {
                        let source = format!("{}:tip:{hash}:{range}", edge.source);
                        let item =
                            Self::pending_atom_ref_item(&entry.atom_uuid, &source, storage_prefix)?;
                        pending_keys.push(item.0.clone());
                        pending_items.push(item);
                    }
                }
            }
            molecule_edges.push(edge);
        }
        let transition_id = catalog_atom_ref_transition_id(db_locator, schema_name);
        let plan = CatalogAtomRefPlan {
            storage_prefix: storage_prefix.map(str::to_string),
            molecule_edges,
            atom_deltas,
            pending_keys,
            transition_id: transition_id.clone(),
        };
        let transition = CatalogAtomRefTransition {
            db_locator: db_locator.to_string(),
            schema_name: schema_name.to_string(),
            kind,
            plan: plan.clone(),
        };
        let _transition_guard = self.catalog_atom_ref_transition_lock.lock().await;
        let mut registry = self.catalog_atom_ref_transition_registry().await?;
        registry.transitions.insert(transition_id, transition);
        pending_items.push((
            CATALOG_ATOM_REF_TRANSITIONS_KEY.to_string(),
            serde_json::to_value(registry).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "serialize database-catalog atom reference transition registry: {error}"
                ))
            })?,
        ));
        self.main_store
            .batch_put_items(pending_items)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "persist database-catalog atom reference transition: {error}"
                ))
            })?;
        self.main_store.inner().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!(
                "flush database-catalog atom reference transition: {error}"
            ))
        })?;
        Ok(plan)
    }

    /// Commit a live catalog's prepared paths and clear the pending holds.
    pub(crate) async fn commit_catalog_atom_refs(
        &self,
        plan: CatalogAtomRefPlan,
    ) -> Result<(), SchemaError> {
        self.commit_catalog_atom_ref_plan(plan, true).await
    }

    /// Remove a deleted catalog's prepared paths.
    pub(crate) async fn commit_catalog_atom_ref_removal(
        &self,
        plan: CatalogAtomRefPlan,
    ) -> Result<(), SchemaError> {
        self.commit_catalog_atom_ref_plan(plan, false).await
    }

    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub(super) async fn commit_catalog_atom_ref_plan(
        &self,
        plan: CatalogAtomRefPlan,
        add: bool,
    ) -> Result<(), SchemaError> {
        let _transition_guard = self.catalog_atom_ref_transition_lock.lock().await;
        let CatalogAtomRefPlan {
            storage_prefix,
            molecule_edges,
            atom_deltas,
            pending_keys,
            transition_id,
        } = plan;
        let mut transition_registry = self.catalog_atom_ref_transition_registry().await?;
        transition_registry.transitions.remove(&transition_id);
        let has_molecule_edges = !molecule_edges.is_empty();
        let mut molecule_deltas: HashMap<String, u64> = HashMap::new();
        for edge in &molecule_edges {
            *molecule_deltas
                .entry(edge.molecule_uuid.clone())
                .or_default() += 1;
        }
        let mut atom_deltas: Vec<(String, u64)> = atom_deltas.into_iter().collect();
        atom_deltas.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let atoms: Vec<String> = atom_deltas
            .iter()
            .map(|(atom_uuid, _)| atom_uuid.clone())
            .collect();
        let _count_guards = self.lock_atom_ref_counts(&atoms).await;
        let count_keys: Vec<String> = atoms
            .iter()
            .map(|atom_uuid| atom_live_ref_count_key(atom_uuid, storage_prefix.as_deref()))
            .collect();
        let stored_counts = self
            .main_store
            .get_items::<AtomLiveRefCount>(&count_keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load atom counts for database-catalog transition: {error}"
                ))
            })?;
        let mut mutations = Vec::new();
        for (((atom_uuid, delta), key), stored) in
            atom_deltas.into_iter().zip(count_keys).zip(stored_counts)
        {
            if stored.is_none()
                && self
                    .has_active_atom_refs(&atom_uuid, storage_prefix.as_deref())
                    .await?
            {
                mutations.push(KvMutation::put(
                    key.into_bytes(),
                    serde_json::to_vec(&AtomLiveRefCount {
                        live_refs: 0,
                        exact: false,
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize legacy database-catalog atom count: {error}"
                        ))
                    })?,
                ));
                mutations.extend(
                    self.atom_gc_candidate_mutations_for_count(
                        &atom_uuid,
                        u64::MAX,
                        storage_prefix.as_deref(),
                    )
                    .await?,
                );
                continue;
            }
            let stored = stored.unwrap_or_default();
            if !stored.exact {
                mutations.extend(
                    self.atom_gc_candidate_mutations_for_count(
                        &atom_uuid,
                        u64::MAX,
                        storage_prefix.as_deref(),
                    )
                    .await?,
                );
                continue;
            }
            let before = stored.live_refs;
            let after = if add {
                before.checked_add(delta)
            } else {
                before.checked_sub(delta)
            };
            let Some(after) = after else {
                // A legacy counter may omit older references. Keep the atom
                // and clear any zero-count GC candidate.
                mutations.push(KvMutation::put(
                    key.into_bytes(),
                    serde_json::to_vec(&AtomLiveRefCount {
                        live_refs: 0,
                        exact: false,
                    })
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize database-catalog atom reference count: {error}"
                        ))
                    })?,
                ));
                mutations.extend(
                    self.atom_gc_candidate_mutations_for_count(
                        &atom_uuid,
                        u64::MAX,
                        storage_prefix.as_deref(),
                    )
                    .await?,
                );
                continue;
            };
            mutations.push(KvMutation::put(
                key.into_bytes(),
                serde_json::to_vec(&AtomLiveRefCount {
                    live_refs: after,
                    exact: true,
                })
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "serialize database-catalog atom reference count: {error}"
                    ))
                })?,
            ));
            mutations.extend(
                self.atom_gc_candidate_mutations_for_count(
                    &atom_uuid,
                    after,
                    storage_prefix.as_deref(),
                )
                .await?,
            );
        }
        let mut molecules: Vec<(String, u64)> = molecule_deltas.into_iter().collect();
        molecules.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let molecule_ids: Vec<String> = molecules
            .iter()
            .map(|(molecule_uuid, _)| molecule_uuid.clone())
            .collect();
        let molecule_counts = self
            .database_catalog_ref_counts_for_molecules(&molecule_ids, storage_prefix.as_deref())
            .await?;
        for (molecule_uuid, delta) in molecules {
            let before = molecule_counts.get(&molecule_uuid).copied().unwrap_or(0);
            let after = if add {
                before.checked_add(delta)
            } else {
                before.checked_sub(delta)
            }
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "database-catalog molecule reference count is out of range for {molecule_uuid}"
                ))
            })?;
            mutations.push(KvMutation::put(
                molecule_catalog_ref_count_key(&molecule_uuid, storage_prefix.as_deref())
                    .into_bytes(),
                serde_json::to_vec(&MoleculeCatalogRefCount { live_refs: after }).map_err(
                    |error| {
                        SchemaError::InvalidData(format!(
                            "serialize database-catalog molecule reference count: {error}"
                        ))
                    },
                )?,
            ));
        }
        for edge in molecule_edges {
            let key = edge.storage_key(storage_prefix.as_deref()).into_bytes();
            if add {
                mutations.push(KvMutation::put(
                    key,
                    serde_json::to_vec(&edge).map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize database-catalog molecule reference: {error}"
                        ))
                    })?,
                ));
            } else {
                mutations.push(KvMutation::delete(key));
            }
        }
        if add && has_molecule_edges {
            mutations.push(KvMutation::put(
                Self::database_catalog_ref_present_key(storage_prefix.as_deref()).into_bytes(),
                ATOM_REF_V2_ACTIVE_MARKER,
            ));
        }
        mutations.extend(
            pending_keys
                .into_iter()
                .map(|key| KvMutation::delete(key.into_bytes())),
        );
        mutations.push(catalog_atom_ref_transition_registry_mutation(
            &transition_registry,
        )?);
        if mutations.is_empty() {
            return Ok(());
        }
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "commit database-catalog atom reference transition: {error}"
                ))
            })
    }

    pub(crate) async fn abort_catalog_atom_refs(
        &self,
        plan: &CatalogAtomRefPlan,
    ) -> Result<(), SchemaError> {
        let _transition_guard = self.catalog_atom_ref_transition_lock.lock().await;
        let mut registry = self.catalog_atom_ref_transition_registry().await?;
        registry.transitions.remove(&plan.transition_id);
        let mut mutations: Vec<KvMutation> = plan
            .pending_keys
            .iter()
            .map(|key| KvMutation::delete(key.as_bytes().to_vec()))
            .collect();
        mutations.push(catalog_atom_ref_transition_registry_mutation(&registry)?);
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "clear pending database-catalog atom references: {error}"
                ))
            })
    }

    /// Resolve catalog transitions that crossed the `db_catalog` and `main`
    /// durability domains before the process stopped.
    pub(crate) async fn recover_catalog_atom_ref_transitions(
        &self,
        catalog: &DbCatalogStore,
    ) -> Result<(), SchemaError> {
        let transitions = self
            .catalog_atom_ref_transition_registry()
            .await?
            .transitions;
        for transition in transitions.into_values() {
            let membership = catalog
                .get(&transition.db_locator, &transition.schema_name)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "resolve database-catalog atom reference transition: {error}"
                    ))
                })?;
            let membership_matches_plan = membership
                .as_ref()
                .is_some_and(|entry| entry.instance_id == transition.plan.storage_prefix);
            match transition.kind {
                CatalogAtomRefTransitionKind::Add if membership_matches_plan => {
                    self.commit_catalog_atom_refs(transition.plan).await?;
                }
                CatalogAtomRefTransitionKind::Remove if membership.is_none() => {
                    self.commit_catalog_atom_ref_removal(transition.plan)
                        .await?;
                }
                _ => self.abort_catalog_atom_refs(&transition.plan).await?,
            }
        }
        Ok(())
    }

    pub(super) async fn catalog_atom_ref_transition_registry(
        &self,
    ) -> Result<CatalogAtomRefTransitionRegistry, SchemaError> {
        self.main_store
            .get_item(CATALOG_ATOM_REF_TRANSITIONS_KEY)
            .await
            .map(Option::unwrap_or_default)
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load database-catalog atom reference transition registry: {error}"
                ))
            })
    }

    /// Load the durable catalog-path weight for each molecule with exact
    /// point reads. Old homes populate a missing counter once from the bounded
    /// molecule edge partition, then all tip writes use the cached value.
    pub(super) async fn database_catalog_ref_counts_for_molecules(
        &self,
        molecule_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<String, u64>, SchemaError> {
        let keys: Vec<String> = molecule_uuids
            .iter()
            .map(|molecule_uuid| molecule_catalog_ref_count_key(molecule_uuid, storage_prefix))
            .collect();
        let stored = self
            .main_store
            .get_items::<MoleculeCatalogRefCount>(&keys)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load database-catalog molecule reference counts: {error}"
                ))
            })?;
        let mut counts = HashMap::with_capacity(molecule_uuids.len());
        let mut backfill = Vec::new();
        for ((molecule_uuid, key), stored) in molecule_uuids.iter().zip(keys).zip(stored) {
            let needs_backfill = stored.is_none();
            let count = match stored {
                Some(count) => count.live_refs,
                None => self
                    .molecule_ref_edges_for_molecule(molecule_uuid, storage_prefix)
                    .await?
                    .edges
                    .into_iter()
                    .filter(|edge| edge.edge_type == "database-catalog-field")
                    .count() as u64,
            };
            counts.insert(molecule_uuid.clone(), count);
            if needs_backfill {
                backfill.push((
                    key,
                    serde_json::to_value(MoleculeCatalogRefCount { live_refs: count }).map_err(
                        |error| {
                            SchemaError::InvalidData(format!(
                                "serialize database-catalog molecule reference count: {error}"
                            ))
                        },
                    )?,
                ));
            }
        }
        if !backfill.is_empty() {
            self.main_store
                .batch_put_items(backfill)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "backfill database-catalog molecule reference counts: {error}"
                    ))
                })?;
        }
        Ok(counts)
    }
}

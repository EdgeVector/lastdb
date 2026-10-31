use super::*;

impl DbOperations {
    /// Point-get `hcu:mols`. Absent, unreadable, or an old stamp that fails to
    /// deserialize is `None`. This does not derive a stamp and does not walk
    /// `conflict\0`.
    async fn load_home_conflict_index(
        &self,
        storage_prefix: Option<&str>,
    ) -> Option<HomeConflictIndex> {
        self.conflict_index_from_memory(storage_prefix).await
    }

    /// Stamp plus events newer than `folded_seq`.
    ///
    /// The only reads are the point get and, when the stamp exists, the event
    /// range. A miss returns [`HomeConflictAnnotation::Unknown`]. It does not
    /// call [`Self::get_unresolved_conflicts`] or
    /// [`Self::home_has_unresolved_conflicts`].
    pub async fn read_home_conflict_annotation(
        &self,
        storage_prefix: Option<&str>,
    ) -> HomeConflictAnnotation {
        if home_conflict_index_disabled() {
            return HomeConflictAnnotation::Disabled;
        }
        let Some(index) = self.load_home_conflict_index(storage_prefix).await else {
            return HomeConflictAnnotation::Unknown;
        };
        let events = match self
            .home_conflict_events_after(storage_prefix, &index.folded_seq)
            .await
        {
            Ok(rows) => rows.into_iter().map(|(_, event)| event).collect::<Vec<_>>(),
            // The range failed, so the stamp may be behind an append. Do not
            // call the page clean.
            Err(_) => {
                return HomeConflictAnnotation::Incomplete {
                    molecules: index.molecules.into_iter().collect(),
                };
            }
        };
        let molecules = union_molecules(&index.molecules, &events);
        if index.complete && molecules.len() <= HOME_CONFLICT_INDEX_MAX_MOLECULES {
            HomeConflictAnnotation::Known { molecules }
        } else {
            HomeConflictAnnotation::Incomplete { molecules }
        }
    }

    /// List all unresolved sync conflicts, optionally filtered by molecule UUID.
    /// When `storage_prefix` is `Some`, scans org-prefixed keys.
    pub async fn get_unresolved_conflicts(
        &self,
        molecule_uuid: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<SyncConflict>, SchemaError> {
        // Per-molecule path: honour the id-list cache. Empty → O(1). Non-empty
        // → point-get each id (no prefix sweep). Missing/legacy → fall through
        // to one scan and self-heal.
        if let Some(mol) = molecule_uuid {
            let index_key = build_storage_key(storage_prefix, &molecule_conflict_index_key(mol));
            if let Some(ids) = self
                .atoms()
                .raw()
                .get_item::<Vec<String>>(&index_key)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("read conflict index {mol}: {e}")))?
            {
                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let original_len = ids.len();
                let mut out = Vec::with_capacity(original_len);
                let mut live_ids = Vec::with_capacity(original_len);
                let mut gone = Vec::new();
                for id in ids {
                    let key = build_storage_key(
                        storage_prefix,
                        &lastdb_model::kind_partition::anchored("conflict", &id),
                    );
                    match self
                        .atoms()
                        .raw()
                        .get_item::<SyncConflict>(&key)
                        .await
                        .map_err(|e| SchemaError::InvalidData(format!("read conflict {id}: {e}")))?
                    {
                        Some(c) if !c.resolved => {
                            live_ids.push(id);
                            out.push(c);
                        }
                        // Resolved or missing: drop from the cache so the next
                        // call stays honest without a scan.
                        _ => gone.push(id),
                    }
                }
                if !gone.is_empty() {
                    self.prune_conflict_index(&index_key, &gone).await;
                }
                return Ok(out);
            }
        }

        let base_prefix = match molecule_uuid {
            Some(mol) => lastdb_model::kind_partition::anchored("conflict", &format!("{mol}:")),
            None => lastdb_model::kind_partition::anchored("conflict", ""),
        };
        let prefix = build_storage_key(storage_prefix, &base_prefix);

        // The scan runs outside the recorder gate. The stamp below is written
        // only if no record call wrote a row since this point.
        let epoch = self.atoms().conflict_record_epoch();
        let items: Vec<(String, SyncConflict)> = self
            .atoms()
            .raw()
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to scan conflicts: {e}")))?;

        let conflicts: Vec<SyncConflict> = items
            .into_iter()
            .map(|(_, c)| c)
            .filter(|c| !c.resolved)
            .collect();

        // Self-heal: stamp the id list so the next annotate is point-gets only.
        if let Some(mol) = molecule_uuid {
            let index_key = build_storage_key(storage_prefix, &molecule_conflict_index_key(mol));
            let ids: Vec<String> = conflicts.iter().map(|c| c.id.clone()).collect();
            self.stamp_conflict_index(&index_key, &ids, epoch).await;
        }

        Ok(conflicts)
    }

    /// Fold events at or below the cutoff into `hcu:mols`, then delete only
    /// the keys that were folded.
    ///
    /// Does not call [`Self::home_has_unresolved_conflicts`] or
    /// [`Self::get_unresolved_conflicts`]. Does not list `mcc:` keys. An empty
    /// event range does not write a complete empty stamp.
    pub async fn fold_home_conflict_events(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let range_start = unix_nanos_wide();
        let cutoff_nanos = range_start.saturating_sub(1_000_000_000);
        let cutoff = format!("{cutoff_nanos:020}");
        let loaded = self.load_stamp_for_fold(storage_prefix).await?;
        let Some(existing) = loaded else {
            // Old stamp bytes that do not deserialize. Leave them. Do not
            // re-derive.
            return Ok(());
        };
        let folded_seq = existing
            .as_ref()
            .map(|index| index.folded_seq.clone())
            .unwrap_or_default();
        let rows = self
            .home_conflict_events_after(storage_prefix, &folded_seq)
            .await?;
        let mut fold_events = Vec::new();
        let mut fold_keys = Vec::new();
        for (key, event) in rows {
            let Some(seq) = seq_of_event_key(&key) else {
                continue;
            };
            if seq_in_fold_window(seq, &folded_seq, &cutoff) {
                fold_keys.push(key);
                fold_events.push(event);
            }
        }
        let Some(index) = existing else {
            if fold_events.is_empty() {
                return Ok(());
            }
            let molecules = capped_molecules(&union_molecules(&[], &fold_events));
            self.write_home_conflict_stamp(
                storage_prefix,
                HomeConflictIndex {
                    complete: false,
                    molecules,
                    folded_seq: cutoff,
                },
            )
            .await?;
            return self.delete_event_keys(&fold_keys).await;
        };
        let (complete, molecules) = merged_stamp(&index, &fold_events);
        self.write_home_conflict_stamp(
            storage_prefix,
            HomeConflictIndex {
                complete,
                molecules,
                folded_seq: cutoff,
            },
        )
        .await?;
        self.delete_event_keys(&fold_keys).await
    }

    /// The only caller of [`Self::get_unresolved_conflicts`] with `None`.
    ///
    /// Runs only when `LASTDB_BUILD_CONFLICT_STAMP_ON_COPY` is set, on the
    /// ephemeral copy. Annotate and [`Self::fold_home_conflict_events`] do not
    /// call this. The stamp stays on this home. Do not copy it onto the primary.
    pub async fn build_home_conflict_stamp_on_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if !env_flag::var_truthy("LASTDB_BUILD_CONFLICT_STAMP_ON_COPY") {
            return Ok(());
        }
        let started = unix_nanos_wide();
        let conflicts = self.get_unresolved_conflicts(None, storage_prefix).await?;
        let names: HashSet<String> = conflicts
            .into_iter()
            .map(|conflict| conflict.molecule_uuid)
            .collect();
        let complete = names.len() <= HOME_CONFLICT_INDEX_MAX_MOLECULES;
        let molecules = capped_molecules(&names);
        self.write_home_conflict_stamp(
            storage_prefix,
            HomeConflictIndex {
                complete,
                molecules,
                folded_seq: format!("{started:020}"),
            },
        )
        .await
    }

    /// `Ok(None)` means the stamp bytes exist and do not deserialize. `Ok(Some(None))`
    /// means the key is absent.
    async fn load_stamp_for_fold(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<Option<Option<HomeConflictIndex>>, SchemaError> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        match self.atoms().raw().get_item::<HomeConflictIndex>(&key).await {
            Ok(index) => Ok(Some(index)),
            Err(StorageError::SerializationError(_)) => Ok(None),
            Err(error) => Err(SchemaError::InvalidData(format!(
                "read home conflict stamp: {error}"
            ))),
        }
    }

    async fn home_conflict_events_after(
        &self,
        storage_prefix: Option<&str>,
        folded_seq: &str,
    ) -> Result<Vec<(String, HomeConflictEvent)>, SchemaError> {
        let (start, end) = event_scan_bounds(storage_prefix, folded_seq);
        debug_assert!(start.contains('\0') && !end.contains('\0'));
        let rows = self
            .atoms()
            .raw()
            .scan_items_in_range::<HomeConflictEvent>(&start, &end)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("range home conflict events: {e}")))?;
        Ok(rows
            .into_iter()
            .filter(|(key, _)| seq_of_event_key(key).is_some_and(|seq| folded_seq < seq))
            .collect())
    }

    async fn write_home_conflict_stamp(
        &self,
        storage_prefix: Option<&str>,
        index: HomeConflictIndex,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        self.atoms()
            .raw()
            .put_item(&key, &index)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("write home conflict stamp: {e}")))
    }

    async fn delete_event_keys(&self, keys: &[String]) -> Result<(), SchemaError> {
        for key in keys {
            self.atoms().raw().delete_item(key).await.map_err(|e| {
                SchemaError::InvalidData(format!("delete home conflict event: {e}"))
            })?;
        }
        Ok(())
    }
}

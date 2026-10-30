//! One retained source boundary, bounded pages, and publication-time deltas.
use super::super::read::ReadContext;
use super::*;
use crate::atom::molecule_key_codec::{decode_hash_range_any, molecule_record_prefix};
use crate::resident::core_keys::KeyVisibility;

impl PackRuntime {
    pub(super) fn check_key_encoding(
        &self,
        db_ops: &DbOperations,
        entry: &Entry,
    ) -> Result<(), SchemaError> {
        let expected = encoding_identity(db_ops, &entry.id.1)?;
        {
            let _view = self.view.read().expect("key encoding read boundary");
            let state = entry.state.lock().expect("key encoding identity");
            if state
                .domain
                .as_ref()
                .is_none_or(|domain| domain.identity().encoding_identity == expected)
            {
                return Ok(());
            }
        }
        let _view = self.view.write().expect("key encoding boundary");
        let mut state = entry.state.lock().expect("key encoding identity");
        if state
            .domain
            .as_ref()
            .is_some_and(|domain| domain.identity().encoding_identity != expected)
        {
            if let Some(domain) = state.domain.take() {
                domain.retire("encoding_change").map_err(key_error)?;
            }
            self.core()
                .lock()
                .expect("key encoding detach")
                .detach_keys(entry.id.0.as_deref(), &entry.id.1);
            state.changes.clear();
            state.reason = Some("encoding_change".into());
            entry.phase.send_replace(Phase::Unloaded);
        }
        Ok(())
    }

    pub(super) async fn ensure_key_entry(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        entry: Arc<Entry>,
    ) -> Result<(), SchemaError> {
        let mut receiver = entry.phase.subscribe();
        loop {
            let phase = *receiver.borrow_and_update();
            match phase {
                Phase::Ready | Phase::DiskOnly => return Ok(()),
                Phase::Unloaded => {
                    self.start_key_load(Arc::clone(&db_ops), Arc::clone(&entry))
                        .await?;
                    tokio::task::yield_now().await;
                    continue;
                }
                Phase::Loading => {}
            }
            receiver
                .changed()
                .await
                .map_err(|_| SchemaError::InvalidData("schema key load owner closed".into()))?;
        }
    }

    async fn start_key_load(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        entry: Arc<Entry>,
    ) -> Result<(), SchemaError> {
        // Serialize transient builds before source capture. Another load's
        // page buffers must not cause a permanent key-capacity refusal.
        let permit = Arc::clone(&self.schema_keys.load_slots)
            .acquire_owned()
            .await
            .map_err(|_| SchemaError::InvalidData("schema preload owner closed".into()))?;
        let base = ReadContext::new(self)?;
        let _view = self.view.write().expect("key load publication boundary");
        if *entry.phase.borrow() != Phase::Unloaded {
            return Ok(());
        }
        if _view.revision != base.revision {
            return Ok(());
        }
        let identity = CoreKeyIdentity {
            scope: entry.id.0.clone(),
            molecule: entry.id.1.clone(),
            layout: "hash_range".into(),
            encoding_identity: encoding_identity(&db_ops, &entry.id.1)?,
            membership_rules: "current-candidates-with-exact-visibility-v1".into(),
        };
        let codec = db_ops.atoms().key_codec_for_molecule(&entry.id.1)?;
        let page_charge = match self.reserve_key_work(&entry.id) {
            Ok(charge) => charge,
            Err(error) => {
                entry.state.lock().expect("key work refusal").reason = Some(error.to_string());
                entry.phase.send_replace(Phase::DiskOnly);
                return Ok(());
            }
        };
        let domain = match CoreKeyDomain::new_loading(
            identity,
            codec.range_profile(),
            Arc::clone(&self.schema_keys.budget),
        ) {
            Ok(domain) => domain,
            Err(error) => {
                entry.state.lock().expect("key load refusal").reason = Some(error.to_string());
                entry.phase.send_replace(Phase::DiskOnly);
                return Ok(());
            }
        };
        {
            let mut state = entry.state.lock().expect("key load owner");
            state.domain = Some(Arc::clone(&domain));
            state.reason = None;
        }
        self.core()
            .lock()
            .expect("key load molecule")
            .attach_keys(Arc::clone(&domain));
        entry.phase.send_replace(Phase::Loading);
        self.schema_keys.loads.fetch_add(1, Ordering::Relaxed);
        self.schema_keys.active.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(120),
                runtime.load_key_domain(db_ops, &entry, &domain, base, Arc::new(page_charge)),
            )
            .await;
            let result = result.unwrap_or_else(|_| {
                Err(SchemaError::ReadResourceLimit {
                    detail: "schema key load time limit".into(),
                })
            });
            if let Err(error) = result {
                runtime.fail_key_load(&entry, &domain, &error);
            }
            runtime.schema_keys.active.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(())
    }

    async fn load_key_domain(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        entry: &Arc<Entry>,
        domain: &Arc<CoreKeyDomain>,
        base: Arc<ReadContext>,
        page_charge: Arc<CoreKeyReservation>,
    ) -> Result<(), SchemaError> {
        // The page allowance covers encrypted input, decoded JSON, keys, and
        // copies. It releases before Ready; permanent tree charges stay held.
        let prefix = molecule_record_prefix(&entry.id.1);
        let has_generation = ReadContext::preload_page(self, &base, &page_charge)?
            .run_result(async {
                Ok::<_, SchemaError>(
                    db_ops
                        .atoms()
                        .active_molecule_generation(&entry.id.1, entry.id.0.as_deref())
                        .await?
                        .is_some(),
                )
            })
            .await?;
        {
            let mut state = entry.state.lock().expect("key load generation identity");
            if !state
                .domain
                .as_ref()
                .is_some_and(|owner| Arc::ptr_eq(owner, domain))
            {
                return Ok(());
            }
            state.has_generation = Some(has_generation);
        }
        let mut after = None;
        loop {
            if !owns_load(entry, domain) {
                return Ok(());
            }
            let scope = ReadContext::preload_page(self, &base, &page_charge)?;
            let rows = scope
                .run_result(db_ops.atoms().scan_logical_tip_page(
                    &prefix,
                    after.as_deref(),
                    entry.id.0.as_deref(),
                    256,
                ))
                .await?;
            let full = rows.len() == 256;
            after = rows.last().map(|(key, _)| key.clone());
            let keys = rows
                .into_iter()
                .map(|(key, record)| {
                    let (hash, range) = decode_hash_range_any(&key).ok_or_else(|| {
                        SchemaError::InvalidData("invalid complete molecule key".into())
                    })?;
                    if !publication::supported_coordinate(domain, &hash, &range) {
                        return Err(SchemaError::ReadResourceLimit {
                            detail: "schema preload declines mixed legacy key forms".into(),
                        });
                    }
                    Ok((
                        hash,
                        range,
                        KeyVisibility::from_tombstoned(
                            record.meta.as_ref().is_some_and(|meta| meta.tombstoned),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, SchemaError>>()?;
            domain.insert_batch(&keys).map_err(key_error)?;
            if !full {
                break;
            }
        }
        self.finish_key_load(db_ops, entry, domain, &page_charge)
            .await
    }

    /// Called under the publication boundary. Held domains keep their charge
    /// and are not purge victims. DiskOnly refuses repeated automatic retries.
    fn reserve_key_work(&self, loading: &DomainId) -> StorageResult<CoreKeyReservation> {
        loop {
            if let Ok(charge) = self.schema_keys.budget.reserve_temporary(64 * 1024 * 1024) {
                return Ok(charge);
            }
            let entries = self.schema_keys.entries.lock().expect("key purge entries");
            let victim = entries
                .values()
                .filter(|entry| &entry.id != loading)
                .filter(|entry| *entry.phase.borrow() == Phase::Ready)
                .filter(|entry| {
                    entry
                        .state
                        .lock()
                        .expect("key purge state")
                        .domain
                        .as_ref()
                        .is_some_and(|domain| Arc::strong_count(domain) == 2)
                })
                .min_by_key(|entry| entry.tick.load(Ordering::Relaxed))
                .cloned();
            let Some(victim) = victim else {
                return Err(key_error(
                    "complete key byte allowance refused preload work",
                ));
            };
            let mut state = victim.state.lock().expect("key purge victim");
            if let Some(domain) = state.domain.take() {
                domain.retire("key_purge").map_err(key_error)?;
            }
            self.core()
                .lock()
                .expect("key purge core")
                .detach_keys(victim.id.0.as_deref(), &victim.id.1);
            state.changes.clear();
            state.reason = Some("key_purge".into());
            victim.phase.send_replace(Phase::Unloaded);
        }
    }

    async fn finish_key_load(
        self: &Arc<Self>,
        db_ops: Arc<DbOperations>,
        entry: &Arc<Entry>,
        domain: &Arc<CoreKeyDomain>,
        page_charge: &Arc<CoreKeyReservation>,
    ) -> Result<(), SchemaError> {
        loop {
            let changes = {
                let _view = self.view.write().expect("key load final boundary");
                let mut state = entry.state.lock().expect("key load deltas");
                if !state
                    .domain
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, domain))
                    || *entry.phase.borrow() != Phase::Loading
                {
                    return Ok(());
                }
                if state.changes.is_empty() {
                    domain.mark_ready().map_err(key_error)?;
                    entry.phase.send_replace(Phase::Ready);
                    return Ok(());
                }
                (0..256)
                    .filter_map(|_| state.changes.pop_first())
                    .collect::<Vec<_>>()
            };
            let keys: Vec<_> = changes.iter().map(|(key, _)| key.clone()).collect();
            let base = ReadContext::new(self)?;
            let scope = ReadContext::preload_page(self, &base, page_charge)?;
            let records = scope
                .run_result(
                    db_ops
                        .atoms()
                        .load_exact_per_key_records(entry.id.0.as_deref(), &keys),
                )
                .await?;
            for (key, record) in keys.into_iter().zip(records) {
                let (hash, range) = decode_hash_range_any(&key).ok_or_else(|| {
                    SchemaError::InvalidData("invalid changed molecule key".into())
                })?;
                if let Some(record) = record {
                    if !publication::supported_coordinate(domain, &hash, &range) {
                        return Err(SchemaError::ReadResourceLimit {
                            detail: "schema preload declines mixed legacy key forms".into(),
                        });
                    }
                    domain
                        .insert_batch(&[(
                            hash,
                            range,
                            KeyVisibility::from_tombstoned(
                                record.meta.as_ref().is_some_and(|meta| meta.tombstoned),
                            ),
                        )])
                        .map_err(key_error)?;
                } else {
                    domain.remove(&hash, &range).map_err(key_error)?;
                }
            }
        }
    }

    fn fail_key_load(&self, entry: &Entry, domain: &Arc<CoreKeyDomain>, error: &SchemaError) {
        let _view = self.view.write().expect("key load failure boundary");
        let mut state = entry.state.lock().expect("key load failure");
        if !state
            .domain
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, domain))
        {
            return;
        }
        // Preload is optional. Discard incomplete membership and let the
        // required disk read report its own I/O or decoding errors.
        let _ = domain.retire("load_refused");
        self.core()
            .lock()
            .expect("key load detach")
            .detach_keys(entry.id.0.as_deref(), &entry.id.1);
        state.domain = None;
        state.changes.clear();
        state.has_generation = None;
        state.reason = Some(error.to_string());
        entry.phase.send_replace(Phase::DiskOnly);
    }
}

fn owns_load(entry: &Entry, domain: &Arc<CoreKeyDomain>) -> bool {
    let state = entry.state.lock().expect("key load ownership");
    *entry.phase.borrow() == Phase::Loading
        && state
            .domain
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, domain))
}

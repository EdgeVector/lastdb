use super::*;

impl AtomStore {
    /// Point-read the final v1 drain cursor from the compact plane.
    pub async fn atom_ref_v1_drain_status(&self) -> Result<AtomRefV1DrainStatus, SchemaError> {
        let key = ATOM_REF_V1_DRAIN_CHECKPOINT_KEY;
        let status: Option<AtomRefV1DrainStatus> =
            self.main_store.get_item(key).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load legacy atom reverse-edge drain status: {error}"
                ))
            })?;
        Ok(status.unwrap_or_default())
    }

    /// Return whether the physical v1 plane still has any live row.
    ///
    /// This is a one-row range probe. It does not enumerate the plane.
    pub async fn atom_ref_v1_keys_remain(&self) -> Result<bool, SchemaError> {
        let namespaced = self.namespaced_store.as_ref().ok_or_else(|| {
            SchemaError::InvalidData(
                "legacy atom reverse-edge drain requires the owner namespace store".to_string(),
            )
        })?;
        let v1 = namespaced
            .open_namespace("atom_ref_edges")
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("open legacy atom reverse-edge plane: {error}"))
            })?;
        v1.scan_range_paged(b"", ATOM_REF_V1_DRAIN_END.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe legacy atom reverse-edge rows: {error}"))
            })
    }

    /// Remove one durable page of legacy v1 rows after the final write cut.
    ///
    /// The global gates fail closed. Every delete precedes the v2-plane cursor
    /// update in one ordered durable batch. A crash can therefore repeat a
    /// delete page, but it cannot skip an undeleted v1 key.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn drain_atom_ref_v1_page(
        &self,
        page_limit: usize,
        readiness_prefixes: &[Option<String>],
    ) -> Result<AtomRefV1DrainReport, SchemaError> {
        if readiness_prefixes.is_empty() {
            return Err(SchemaError::InvalidData(
                "legacy atom reverse-edge drain requires a non-empty prefix gate".to_string(),
            ));
        }
        for storage_prefix in readiness_prefixes {
            if !self
                .atom_ref_v2_reads_ready(storage_prefix.as_deref())
                .await?
            {
                return Err(SchemaError::InvalidData(format!(
                    "legacy atom reverse-edge drain requires exact completion markers for prefix {storage_prefix:?}"
                )));
            }
        }

        let mut status = self.atom_ref_v1_drain_status().await?;
        let status_key = ATOM_REF_V1_DRAIN_CHECKPOINT_KEY.to_string();
        let complete_key = ATOM_REF_V1_DRAIN_COMPLETE_KEY.to_string();
        if status.completed {
            self.main_store
                .inner()
                .batch_mutate(vec![
                    compact_json_put_mutation(
                        status_key,
                        &status,
                        "serialize legacy atom reverse-edge drain status",
                    )?,
                    KvMutation::put(complete_key.into_bytes(), ATOM_REF_V2_COMPLETE_MARKER),
                ])
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "repair legacy atom reverse-edge drain markers: {error}"
                    ))
                })?;
            return Ok(AtomRefV1DrainReport {
                keys_deleted: 0,
                completed: true,
                status,
            });
        }

        let namespaced = self.namespaced_store.as_ref().ok_or_else(|| {
            SchemaError::InvalidData(
                "legacy atom reverse-edge drain requires the owner namespace store".to_string(),
            )
        })?;
        let v1 = namespaced
            .open_namespace("atom_ref_edges")
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("open legacy atom reverse-edge plane: {error}"))
            })?;
        let scan = v1
            .scan_range_physical_paged(
                b"",
                ATOM_REF_V1_DRAIN_END.as_bytes(),
                status.cursor.as_ref(),
                page_limit.max(1),
                ATOM_REF_V1_DRAIN_HANDLES_PER_PAGE,
            )
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "scan legacy atom reverse-edge drain page: {error}"
                ))
            })?;
        let rows = scan.rows;
        let keys_deleted = rows.len() as u64;
        let mut mutations = Vec::with_capacity(rows.len() + 2);
        for (key, _) in &rows {
            if !is_any_v1_atom_ref_key(key) {
                return Err(SchemaError::InvalidData(format!(
                    "legacy atom reverse-edge plane contains an unexpected key: {}",
                    String::from_utf8_lossy(key)
                )));
            }
            mutations.push(KvMutation::delete(key.clone()));
        }
        status.keys_deleted = status.keys_deleted.saturating_add(keys_deleted);
        status.cursor = scan.next_cursor;
        status.completed = status.cursor.is_none();
        mutations.push(compact_json_put_mutation(
            status_key,
            &status,
            "serialize legacy atom reverse-edge drain status",
        )?);
        if status.completed {
            mutations.push(KvMutation::put(
                complete_key.into_bytes(),
                ATOM_REF_V2_COMPLETE_MARKER,
            ));
        }
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "write legacy atom reverse-edge drain page: {error}"
                ))
            })?;

        Ok(AtomRefV1DrainReport {
            keys_deleted,
            completed: status.completed,
            status,
        })
    }
}

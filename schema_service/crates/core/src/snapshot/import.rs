use super::*;

impl SchemaServiceState {
    /// Replace every persisted dimension of the registry with the contents
    /// of `envelope`. **In-memory dev backend only.**
    ///
    /// Validates `format_version` and `embedder_version` against the local
    /// binary's capabilities first; on any mismatch the call returns an
    /// error and no state is touched. On the happy path, this clears
    /// the backend's schemas, canonical_fields and apps, writes the
    /// envelope's contents in their place, then refreshes the in-memory
    /// caches and rebuilds the descriptive_name index.
    ///
    /// **Not transactional.** A failure mid-import can leave the
    /// collections out of sync — the dev binary tolerates this because the
    /// remedy is a restart, which starts again from the seeds.
    /// Production never imports.
    ///
    /// **`system_schema_hashes` is not modified.** It was populated by
    /// `seed_builtins()` at boot and reflects which identity hashes the
    /// local binary classifies as system. The snapshot has no opinion on
    /// system-vs-user classification (it's a per-binary policy), so
    /// import leaves the set as-is.
    pub fn import_snapshot(
        &self,
        envelope: SnapshotEnvelope,
    ) -> FoldDbResult<SnapshotImportReport> {
        self.check_snapshot_compatibility(&envelope)?;
        self.persist_snapshot_to_backend(&envelope)?;

        let SnapshotEnvelope {
            captured_at,
            embedder_version,
            schemas: schemas_vec,
            apps: apps_vec,
            canonical_fields: canonical_fields_map,
            embeddings,
            ..
        } = envelope;

        let counts = SnapshotImportReport {
            schemas: schemas_vec.len(),
            canonical_fields: canonical_fields_map.len(),
            embeddings: embeddings.descriptive_names.len()
                + embeddings.fields.len()
                + embeddings.canonical_fields.len(),
            captured_at,
            embedder_version,
        };

        self.replace_registry_caches(schemas_vec, canonical_fields_map, apps_vec, embeddings)?;
        self.rebuild_descriptive_name_index();

        // An import wholesale replaces the registry — clients absolutely
        // need to see the version advance.
        self.bump_state_version();

        tracing::info!(
            target: "schema_service::snapshot",
            schemas = counts.schemas,
            canonical_fields = counts.canonical_fields,
            embeddings = counts.embeddings,
            captured_at = %counts.captured_at,
            embedder_version = %counts.embedder_version,
            "Imported snapshot into the in-memory registry"
        );

        Ok(counts)
    }

    /// Reject an envelope this binary cannot import. No state is touched.
    fn check_snapshot_compatibility(&self, envelope: &SnapshotEnvelope) -> FoldDbResult<()> {
        if envelope.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(FoldDbError::Config(format!(
                "snapshot format_version {} not supported (this binary supports {})",
                envelope.format_version, SNAPSHOT_FORMAT_VERSION
            )));
        }

        let local_embedder_id = self.embedder.embedder_id();
        if envelope.embedder_version != local_embedder_id {
            return Err(FoldDbError::Config(format!(
                "snapshot embedder_version '{}' does not match local embedder '{}' — \
                 importing across embedder versions is unsupported",
                envelope.embedder_version, local_embedder_id
            )));
        }
        Ok(())
    }

    /// Durably wipe and rewrite the registry through the storage backend.
    fn persist_snapshot_to_backend(&self, envelope: &SnapshotEnvelope) -> FoldDbResult<()> {
        // Wipe + rewrite via the storage backend (in-memory for the dev
        // server; S3 backends return "unsupported" and the caller should
        // hydrate cloud via normal write paths instead).
        // `import_snapshot` is sync (callers include non-async paths); run the
        // async backend ops on a dedicated thread like `SchemaServiceState::new`.
        let backend = self.storage.backend().clone();
        let schemas = envelope.schemas.clone();
        let cf_batch: Vec<(String, _)> = envelope
            .canonical_fields
            .iter()
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect();
        let apps = envelope.apps.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("schema-snapshot-import".into())
            .spawn(move || {
                let result = (|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            FoldDbError::Config(format!("snapshot import runtime failed: {e}"))
                        })?;
                    rt.block_on(async {
                        backend.clear_registry_for_snapshot_import().await?;
                        backend.save_schemas(&schemas).await?;
                        backend.save_canonical_fields(&cf_batch).await?;
                        for app in &apps {
                            backend.update_app(app).await?;
                        }
                        Ok::<(), FoldDbError>(())
                    })
                })();
                let _ = tx.send(result);
            })
            .map_err(|e| FoldDbError::Config(format!("snapshot import spawn failed: {e}")))?;
        rx.recv().map_err(|_| {
            FoldDbError::Config("snapshot import thread ended without a result".to_string())
        })??;
        Ok(())
    }

    /// Swap the in-memory registry caches to the imported contents.
    fn replace_registry_caches(
        &self,
        schemas_vec: Vec<Schema>,
        canonical_fields_map: HashMap<String, CanonicalField>,
        apps_vec: Vec<AppRecord>,
        embeddings: SnapshotEmbeddings,
    ) -> FoldDbResult<()> {
        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.clear();
            for schema in schemas_vec {
                schemas.insert(schema.name.clone(), schema);
            }
        }
        {
            let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
            fields.clear();
            fields.extend(canonical_fields_map);
        }
        {
            let mut apps = write_lock(&self.apps, "apps")?;
            apps.clear();
            for app in apps_vec {
                apps.insert(app.app_id.clone(), app);
            }
        }
        {
            let mut e = write_lock(
                &self.descriptive_name_embeddings,
                "descriptive_name_embeddings",
            )?;
            e.clear();
            e.extend(embeddings.descriptive_names);
        }
        {
            let mut e = write_lock(&self.field_embeddings, "field_embeddings")?;
            e.clear();
            e.extend(embeddings.fields);
        }
        {
            let mut e = write_lock(
                &self.canonical_field_embeddings,
                "canonical_field_embeddings",
            )?;
            e.clear();
            e.extend(embeddings.canonical_fields);
        }
        Ok(())
    }
}

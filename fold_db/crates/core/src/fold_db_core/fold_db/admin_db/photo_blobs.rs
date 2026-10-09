//! Photo file-bytes to CAS blob migration.

use super::*;

impl FoldDB {
    /// Move `photos/Photo.file_bytes` (inline base64 JPEG) into the local
    /// `cas_blobs` tree and clear the field on each record.
    ///
    /// CAS key = `sha256:{file_sha256}` (must match decoded bytes). After this,
    /// run [`Self::gc_orphan_atoms`] to delete the large orphaned field atoms.
    #[cfg(feature = "sharing")]
    pub async fn migrate_photo_file_bytes_to_blobs(
        &self,
        dry_run: bool,
    ) -> Result<PhotoBlobMigrateReport, FoldDbError> {
        // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
        use crate::sharing::blob_cas;
        use crate::sharing::delivery_wire::content_addressed_blob;

        let owner_id = self
            .get_node_id()
            .await
            .map_err(|e| FoldDbError::Database(format!("node id: {e}")))?;
        let ctx = AccessContext::owner(owner_id);

        let mut report = PhotoBlobMigrateReport {
            dry_run,
            photos_seen: 0,
            photos_migrated: 0,
            photos_skipped: 0,
            photos_failed: 0,
            bytes_to_cas: 0,
            errors: Vec::new(),
        };

        // Page through all Photo rows via molecule filter.
        let page_size = 50usize;
        let mut offset = 0usize;
        loop {
            let query = Query::new_with_filter(
                "photos/Photo".into(),
                vec![
                    "photo_id".into(),
                    "file_bytes".into(),
                    "file_sha256".into(),
                    "filename".into(),
                    "media_type".into(),
                    "original_filename".into(),
                ],
                Some(HashRangeFilter::Page {
                    offset,
                    limit: page_size,
                }),
            );
            let field_maps = self
                .query_executor()
                .query_with_access(query, &ctx)
                .await
                .map_err(|e| FoldDbError::Database(format!("query photos: {e}")))?;

            // Pivot field → key maps into per-key rows.
            let mut keys: Vec<KeyValue> = field_maps
                .values()
                .flat_map(|m| m.keys().cloned())
                .collect();
            keys.sort_by(|a, b| a.hash.cmp(&b.hash).then(a.range.cmp(&b.range)));
            keys.dedup();
            if keys.is_empty() {
                break;
            }

            for key in &keys {
                report.photos_seen += 1;
                let file_bytes_val = field_maps
                    .get("file_bytes")
                    .and_then(|m| m.get(key))
                    .map(|fv| &fv.value);
                let file_sha = field_maps
                    .get("file_sha256")
                    .and_then(|m| m.get(key))
                    .and_then(|fv| fv.value.as_str())
                    .unwrap_or("")
                    .to_string();
                let media_type = field_maps
                    .get("media_type")
                    .and_then(|m| m.get(key))
                    .and_then(|fv| fv.value.as_str())
                    .map(str::to_string);
                let filename = field_maps
                    .get("filename")
                    .and_then(|m| m.get(key))
                    .and_then(|fv| fv.value.as_str())
                    .or_else(|| {
                        field_maps
                            .get("original_filename")
                            .and_then(|m| m.get(key))
                            .and_then(|fv| fv.value.as_str())
                    })
                    .map(str::to_string);

                let Some(Value::String(b64)) = file_bytes_val else {
                    report.photos_skipped += 1;
                    continue;
                };
                // Already migrated: empty or blob-ref style.
                if b64.is_empty() || b64.starts_with("sha256:") {
                    report.photos_skipped += 1;
                    continue;
                }
                // Skip tiny placeholders.
                if b64.len() < 64 {
                    report.photos_skipped += 1;
                    continue;
                }

                let raw = match decode_photo_b64(b64) {
                    Ok(r) => r,
                    Err(e) => {
                        report.photos_failed += 1;
                        report.errors.push(format!(
                            "decode {}…: {e}",
                            key.hash.as_deref().unwrap_or("?")
                        ));
                        continue;
                    }
                };
                let digest = hex_lower(Sha256::digest(&raw));
                if !file_sha.is_empty() && file_sha != digest {
                    report.photos_failed += 1;
                    report.errors.push(format!(
                        "hash mismatch key={} field={file_sha} decoded={digest}",
                        key.hash.as_deref().unwrap_or("?")
                    ));
                    continue;
                }
                let sha = if file_sha.is_empty() {
                    digest.clone()
                } else {
                    file_sha.clone()
                };

                report.bytes_to_cas += raw.len() as u64;
                if dry_run {
                    report.photos_migrated += 1;
                    continue;
                }

                let pub_key = field_maps
                    .get("file_bytes")
                    .and_then(|m| m.get(key))
                    .and_then(|fv| fv.writer_pubkey.clone())
                    .unwrap_or_default();

                #[cfg(feature = "cloud-sync")]
                if let Some(engine) = self.sync_engine() {
                    let request = crate::fold_db_core::fold_db::PersonalFileBlobWrite {
                        schema_name: "photos/Photo".into(),
                        field_name: "file_bytes".into(),
                        key_value: key.clone(),
                        writer_pubkey: pub_key,
                        mutation_type: MutationType::Update,
                        name: filename.clone(),
                        media_type: media_type.clone(),
                        thumbnail: None,
                        cache_local_plaintext: true,
                        additional_fields: if file_sha.is_empty() {
                            HashMap::from([("file_sha256".into(), Value::String(sha.clone()))])
                        } else {
                            HashMap::new()
                        },
                    };
                    match self
                        .put_personal_file_blob(engine.as_ref(), request, &raw)
                        .await
                    {
                        Ok(_) => {
                            report.photos_migrated += 1;
                        }
                        Err(e) => {
                            report.photos_failed += 1;
                            report.errors.push(format!(
                                "remote blob write {}: {e}",
                                key.hash.as_deref().unwrap_or("?")
                            ));
                        }
                    }
                    continue;
                }

                let blob = content_addressed_blob(&raw, media_type.clone(), filename.clone());
                if let Err(e) = blob_cas::put_blob_in_ops(self.db_ops(), &blob).await {
                    report.photos_failed += 1;
                    report.errors.push(format!("cas put {sha}: {e}"));
                    continue;
                }

                // Clear inline bytes; keep sha. Bytes live at cas_blobs[sha256:{sha}].
                let mut fields: HashMap<String, Value> = HashMap::new();
                fields.insert("file_bytes".into(), Value::String(String::new()));
                if file_sha.is_empty() {
                    fields.insert("file_sha256".into(), Value::String(sha.clone()));
                }

                let mutation = Mutation::new(
                    "photos/Photo".into(),
                    fields,
                    key.clone(),
                    pub_key,
                    MutationType::Update,
                );
                if let Err(e) = self
                    .mutation_manager()
                    .write_mutations_with_access(vec![mutation], &ctx)
                    .await
                {
                    report.photos_failed += 1;
                    report.errors.push(format!(
                        "mutate {}: {e}",
                        key.hash.as_deref().unwrap_or("?")
                    ));
                    continue;
                }
                report.photos_migrated += 1;
            }

            if keys.len() < page_size {
                break;
            }
            offset += keys.len();
            if offset > 10_000 {
                break;
            }
        }

        Ok(report)
    }

    #[cfg(not(feature = "sharing"))]
    #[expect(
        clippy::unused_async,
        reason = "the sharing cfg variant performs async writes and the caller awaits the shared API"
    )]
    pub async fn migrate_photo_file_bytes_to_blobs(
        &self,
        _dry_run: bool,
    ) -> Result<PhotoBlobMigrateReport, FoldDbError> {
        Err(FoldDbError::Database(
            "photo blob migration requires the sharing feature (cas_blobs)".into(),
        ))
    }
}

#[cfg(feature = "sharing")]
pub(super) fn decode_photo_b64(b64: &str) -> Result<Vec<u8>, String> {
    let engine = base64::engine::general_purpose::STANDARD;
    engine
        .decode(b64.trim())
        .or_else(|_| {
            let pad = format!("{}{}", b64.trim(), "=".repeat((4 - b64.len() % 4) % 4));
            engine.decode(pad)
        })
        .map_err(|e| format!("base64: {e}"))
}

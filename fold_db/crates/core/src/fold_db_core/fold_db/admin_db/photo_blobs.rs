//! Photo file-bytes to CAS blob migration.

use super::*;
use crate::schema::types::field::FieldValue;

type PhotoFieldMaps = HashMap<String, HashMap<KeyValue, FieldValue>>;

const PHOTO_SCHEMA: &str = "photos/Photo";
const PHOTO_PAGE_SIZE: usize = 50;
const PHOTO_MAX_OFFSET: usize = 10_000;

/// Outcome of migrating one Photo row.
enum PhotoRowOutcome {
    Skipped,
    Migrated,
    Failed(String),
}

/// Resolved destination for one Photo row's decoded bytes.
struct PhotoBlobTarget<'a> {
    key: &'a KeyValue,
    pub_key: String,
    sha: String,
    /// True when the record already carries `file_sha256`.
    sha_in_field: bool,
    media_type: Option<String>,
    filename: Option<String>,
}

fn key_label(key: &KeyValue) -> &str {
    key.hash.as_deref().unwrap_or("?")
}

fn field_str<'a>(maps: &'a PhotoFieldMaps, field: &str, key: &KeyValue) -> Option<&'a str> {
    maps.get(field)
        .and_then(|m| m.get(key))
        .and_then(|fv| fv.value.as_str())
}

impl FoldDB {
    /// Move `photos/Photo.file_bytes` (inline base64 JPEG) into the local
    /// `cas_blobs` tree and clear the field on each record.
    ///
    /// CAS key = `sha256:{file_sha256}` (must match decoded bytes). After this,
    /// run [`Self::gc_orphan_atoms`] to delete the large orphaned field atoms.
    pub async fn migrate_photo_file_bytes_to_blobs(
        &self,
        dry_run: bool,
    ) -> Result<PhotoBlobMigrateReport, FoldDbError> {
        let work = async {
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
            let mut offset = 0usize;
            loop {
                let field_maps = self.query_photo_page(offset, &ctx).await?;

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
                    match self
                        .migrate_photo_row(key, &field_maps, &ctx, dry_run, &mut report)
                        .await
                    {
                        PhotoRowOutcome::Skipped => report.photos_skipped += 1,
                        PhotoRowOutcome::Migrated => report.photos_migrated += 1,
                        PhotoRowOutcome::Failed(message) => {
                            report.photos_failed += 1;
                            report.errors.push(message);
                        }
                    }
                }

                if keys.len() < PHOTO_PAGE_SIZE {
                    break;
                }
                offset += keys.len();
                if offset > PHOTO_MAX_OFFSET {
                    break;
                }
            }

            Ok(report)
        };
        let admission = self.acquire_local_mutation_admission().await;
        admission.run(Box::pin(work)).await
    }

    async fn query_photo_page(
        &self,
        offset: usize,
        ctx: &AccessContext,
    ) -> Result<PhotoFieldMaps, FoldDbError> {
        let query = Query::new_with_filter(
            PHOTO_SCHEMA.into(),
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
                limit: PHOTO_PAGE_SIZE,
            }),
        );
        self.query_executor()
            .query_with_access(query, ctx)
            .await
            .map_err(|e| FoldDbError::Database(format!("query photos: {e}")))
    }

    /// Migrate one Photo row's inline bytes into the blob store.
    ///
    /// Adds the decoded byte count to `report.bytes_to_cas`; the caller
    /// tallies the outcome counters.
    async fn migrate_photo_row(
        &self,
        key: &KeyValue,
        field_maps: &PhotoFieldMaps,
        ctx: &AccessContext,
        dry_run: bool,
        report: &mut PhotoBlobMigrateReport,
    ) -> PhotoRowOutcome {
        let file_sha = field_str(field_maps, "file_sha256", key)
            .unwrap_or("")
            .to_string();
        let media_type = field_str(field_maps, "media_type", key).map(str::to_string);
        let filename = field_str(field_maps, "filename", key)
            .or_else(|| field_str(field_maps, "original_filename", key))
            .map(str::to_string);

        let file_bytes_val = field_maps
            .get("file_bytes")
            .and_then(|m| m.get(key))
            .map(|fv| &fv.value);
        let Some(Value::String(b64)) = file_bytes_val else {
            return PhotoRowOutcome::Skipped;
        };
        // Already migrated (empty or blob-ref style), or a tiny placeholder.
        if b64.is_empty() || b64.starts_with("sha256:") || b64.len() < 64 {
            return PhotoRowOutcome::Skipped;
        }

        let raw = match decode_photo_b64(b64) {
            Ok(r) => r,
            Err(e) => {
                return PhotoRowOutcome::Failed(format!("decode {}…: {e}", key_label(key)));
            }
        };
        let digest = hex_lower(Sha256::digest(&raw));
        if !file_sha.is_empty() && file_sha != digest {
            return PhotoRowOutcome::Failed(format!(
                "hash mismatch key={} field={file_sha} decoded={digest}",
                key_label(key)
            ));
        }
        let sha = if file_sha.is_empty() {
            digest
        } else {
            file_sha.clone()
        };

        report.bytes_to_cas += raw.len() as u64;
        if dry_run {
            return PhotoRowOutcome::Migrated;
        }

        let pub_key = field_maps
            .get("file_bytes")
            .and_then(|m| m.get(key))
            .and_then(|fv| fv.writer_pubkey.clone())
            .unwrap_or_default();
        let target = PhotoBlobTarget {
            key,
            pub_key,
            sha,
            sha_in_field: !file_sha.is_empty(),
            media_type,
            filename,
        };
        self.write_photo_blob(target, &raw, ctx).await
    }

    /// Write the decoded bytes for one Photo row: through the sync engine when
    /// one is attached, otherwise into the local CAS tree plus a clearing
    /// mutation on the record.
    async fn write_photo_blob(
        &self,
        target: PhotoBlobTarget<'_>,
        raw: &[u8],
        ctx: &AccessContext,
    ) -> PhotoRowOutcome {
        use crate::sharing::blob_cas;
        use crate::sharing::delivery_wire::content_addressed_blob;

        let PhotoBlobTarget {
            key,
            pub_key,
            sha,
            sha_in_field,
            media_type,
            filename,
        } = target;
        if let Some(engine) = self.sync_engine() {
            let request = crate::fold_db_core::fold_db::PersonalFileBlobWrite {
                schema_name: PHOTO_SCHEMA.into(),
                field_name: "file_bytes".into(),
                key_value: key.clone(),
                writer_pubkey: pub_key,
                mutation_type: MutationType::Update,
                name: filename,
                media_type,
                thumbnail: None,
                cache_local_plaintext: true,
                additional_fields: if sha_in_field {
                    HashMap::new()
                } else {
                    HashMap::from([("file_sha256".into(), Value::String(sha))])
                },
            };
            return match self
                .put_personal_file_blob(engine.as_ref(), request, raw)
                .await
            {
                Ok(_) => PhotoRowOutcome::Migrated,
                Err(e) => {
                    PhotoRowOutcome::Failed(format!("remote blob write {}: {e}", key_label(key)))
                }
            };
        }

        let blob = content_addressed_blob(raw, media_type, filename);
        if let Err(e) = blob_cas::put_blob_in_ops(self.db_ops(), &blob).await {
            return PhotoRowOutcome::Failed(format!("cas put {sha}: {e}"));
        }

        // Clear inline bytes; keep sha. Bytes live at cas_blobs[sha256:{sha}].
        let mut fields: HashMap<String, Value> = HashMap::new();
        fields.insert("file_bytes".into(), Value::String(String::new()));
        if !sha_in_field {
            fields.insert("file_sha256".into(), Value::String(sha));
        }
        let mutation = Mutation::new(
            PHOTO_SCHEMA.into(),
            fields,
            key.clone(),
            pub_key,
            MutationType::Update,
        );
        match self
            .mutation_manager()
            .write_mutations_with_access(vec![mutation], ctx)
            .await
        {
            Ok(_) => PhotoRowOutcome::Migrated,
            Err(e) => PhotoRowOutcome::Failed(format!("mutate {}: {e}", key_label(key))),
        }
    }
}

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

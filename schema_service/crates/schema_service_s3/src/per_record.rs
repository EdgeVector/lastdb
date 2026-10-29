//! Per-record S3 storage helpers for apps, releases, and channels.

use aws_sdk_s3::primitives::ByteStream;
use schema_service_core::app_release::{ChannelRecord, ReleaseRecord};
use schema_service_core::snapshot::AppRecord;
use schema_types::{FoldDbError, FoldDbResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;

use super::{error_code, is_precondition_failed, short_error, S3BlobPersistence};

impl S3BlobPersistence {
    pub(crate) async fn put_single_object<T>(
        &self,
        key: &str,
        record: &T,
        use_conditional: bool,
    ) -> FoldDbResult<()>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(record)
            .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
        let mut put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .body(ByteStream::from(body))
            .content_type("application/json");
        if use_conditional {
            put = put.if_none_match("*");
        }
        put.send()
            .await
            .map_err(|e| FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e))))?;
        Ok(())
    }

    /// Replace one object when its ETag still matches.
    /// A generation pair turns a precondition failure into `GenerationConflict`.
    async fn put_if_match<T>(
        &self,
        key: &str,
        record: &T,
        etag: &str,
        generation_conflict: Option<(u64, u64)>,
    ) -> FoldDbResult<()>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(record)
            .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .body(ByteStream::from(body))
            .content_type("application/json")
            .if_match(etag)
            .send()
            .await
            .map_err(|e| {
                if let Some((expected, supplied)) = generation_conflict {
                    if is_precondition_failed(&e) {
                        return FoldDbError::GenerationConflict { expected, supplied };
                    }
                }
                FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e)))
            })?;
        Ok(())
    }

    pub(crate) async fn get_single_object<T>(&self, key: &str) -> FoldDbResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .send()
            .await;
        match result {
            Ok(resp) => {
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| FoldDbError::S3(format!("read S3 body '{key}': {e}")))?;
                let bytes = body.into_bytes();
                let record: T = serde_json::from_slice(&bytes)
                    .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                Ok(Some(record))
            }
            Err(e) => {
                if error_code(&e).as_deref() == Some("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(FoldDbError::S3(format!(
                        "get S3 object '{key}': {}",
                        short_error(&e)
                    )))
                }
            }
        }
    }

    /// Read one record and its ETag. `None` means the object is absent.
    async fn get_record_with_etag<T>(&self, key: &str) -> FoldDbResult<Option<(T, String)>>
    where
        T: DeserializeOwned,
    {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .send()
            .await;
        match result {
            Ok(resp) => {
                let etag = resp
                    .e_tag()
                    .ok_or_else(|| FoldDbError::S3(format!("no ETag for '{key}'")))?
                    .to_string();
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| FoldDbError::S3(format!("read S3 body '{key}': {e}")))?;
                let bytes = body.into_bytes();
                let record: T = serde_json::from_slice(&bytes)
                    .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                Ok(Some((record, etag)))
            }
            Err(e) => {
                if error_code(&e).as_deref() == Some("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(FoldDbError::S3(format!(
                        "get S3 object '{key}': {}",
                        short_error(&e)
                    )))
                }
            }
        }
    }

    /// Page one prefix and decode each object. `label` is the list error text.
    async fn list_records<T>(&self, prefix: &str, label: &str) -> FoldDbResult<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let mut records = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| FoldDbError::S3(format!("list S3 '{label}': {}", short_error(&e))))?;
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(record) = self.get_single_object::<T>(key).await? else {
                    continue;
                };
                records.push(record);
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }
        Ok(records)
    }

    pub(crate) async fn list_and_load_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, ReleaseRecord>> {
        let prefix = self.definition_key("releases/");
        let records = self
            .list_records::<ReleaseRecord>(prefix.as_ref(), "releases/")
            .await?;
        Ok(records
            .into_iter()
            .map(|record| (record.release_id.clone(), record))
            .collect())
    }

    pub(crate) async fn list_and_load_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, ChannelRecord>> {
        let prefix = self.definition_key("channels/");
        let records = self
            .list_records::<ChannelRecord>(prefix.as_ref(), "channels/")
            .await?;
        Ok(records
            .into_iter()
            .map(|record| {
                (
                    schema_service_core::app_release::channel_key(&record.app_id, &record.channel),
                    record,
                )
            })
            .collect())
    }

    pub(crate) async fn list_and_load_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        // Apps stay outside the definition namespace. See `definition_key`.
        let records = self.list_records::<AppRecord>("apps/", "apps/").await?;
        Ok(records
            .into_iter()
            .map(|record| (record.app_id.clone(), record))
            .collect())
    }

    pub(crate) async fn put_release_preserving_revocation(
        &self,
        key: &str,
        record: &ReleaseRecord,
    ) -> FoldDbResult<()> {
        if let Some((existing, etag)) = self.get_record_with_etag::<ReleaseRecord>(key).await? {
            let mut final_record = record.clone();
            if existing.revoked_at.is_some() {
                final_record.revoked_at = existing.revoked_at;
                final_record.revocation_reason = existing.revocation_reason;
            }
            self.put_if_match(key, &final_record, &etag, None).await?;
        } else {
            self.put_single_object(key, record, true).await?;
        }
        Ok(())
    }

    pub(crate) async fn put_channel_with_generation_check(
        &self,
        key: &str,
        record: &ChannelRecord,
    ) -> FoldDbResult<()> {
        if let Some((existing, etag)) = self.get_record_with_etag::<ChannelRecord>(key).await? {
            let expected = record.generation.checked_sub(1).ok_or_else(|| {
                FoldDbError::Config(format!(
                    "generation underflow: new generation {} must be at least 1",
                    record.generation
                ))
            })?;
            if existing.generation != expected {
                return Err(FoldDbError::GenerationConflict {
                    expected: existing.generation,
                    supplied: record.generation,
                });
            }
            self.put_if_match(
                key,
                record,
                &etag,
                Some((existing.generation, record.generation)),
            )
            .await?;
        } else {
            self.put_single_object(key, record, true).await?;
        }
        Ok(())
    }

    pub(crate) async fn list_and_load_compat_rows(
        &self,
        release_id: &str,
    ) -> FoldDbResult<Vec<schema_service_core::app_release::CompatRow>> {
        let prefix = format!("compat/{release_id}/");
        let listed = self.definition_key(&prefix);
        self.list_records(listed.as_ref(), &format!("compat/{release_id}"))
            .await
    }
}

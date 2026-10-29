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
        let existing: Option<(ReleaseRecord, String)> = {
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
                    let existing: ReleaseRecord = serde_json::from_slice(&bytes)
                        .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                    Some((existing, etag))
                }
                Err(e) => {
                    if error_code(&e).as_deref() == Some("NoSuchKey") {
                        None
                    } else {
                        return Err(FoldDbError::S3(format!(
                            "get S3 object '{key}': {}",
                            short_error(&e)
                        )));
                    }
                }
            }
        };

        let mut final_record = record.clone();
        if let Some((existing, etag)) = existing {
            if existing.revoked_at.is_some() {
                final_record.revoked_at = existing.revoked_at;
                final_record.revocation_reason = existing.revocation_reason;
            }
            let body = serde_json::to_vec(&final_record)
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
                    FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e)))
                })?;
        } else {
            let body = serde_json::to_vec(&final_record)
                .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(self.definition_key(key).as_ref())
                .body(ByteStream::from(body))
                .content_type("application/json")
                .if_none_match("*")
                .send()
                .await
                .map_err(|e| {
                    FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e)))
                })?;
        }
        Ok(())
    }

    pub(crate) async fn put_channel_with_generation_check(
        &self,
        key: &str,
        record: &ChannelRecord,
    ) -> FoldDbResult<()> {
        let existing: Option<(ChannelRecord, String)> = {
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
                    let existing: ChannelRecord = serde_json::from_slice(&bytes)
                        .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                    Some((existing, etag))
                }
                Err(e) => {
                    if error_code(&e).as_deref() == Some("NoSuchKey") {
                        None
                    } else {
                        return Err(FoldDbError::S3(format!(
                            "get S3 object '{key}': {}",
                            short_error(&e)
                        )));
                    }
                }
            }
        };
        if let Some((existing, etag)) = existing {
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
            let stored_generation = existing.generation;
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
                    if is_precondition_failed(&e) {
                        FoldDbError::GenerationConflict {
                            expected: stored_generation,
                            supplied: record.generation,
                        }
                    } else {
                        FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e)))
                    }
                })?;
        } else {
            let body = serde_json::to_vec(record)
                .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(self.definition_key(key).as_ref())
                .body(ByteStream::from(body))
                .content_type("application/json")
                .if_none_match("*")
                .send()
                .await
                .map_err(|e| {
                    FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(&e)))
                })?;
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

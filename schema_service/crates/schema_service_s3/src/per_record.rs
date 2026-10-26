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
            .key(key)
            .body(ByteStream::from(body))
            .content_type("application/json");
        if use_conditional {
            put = put.if_none_match("*");
        }
        put.send().await.map_err(|e| {
            FoldDbError::Config(format!("put S3 object '{key}': {}", short_error(&e)))
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
            .key(key)
            .send()
            .await;
        match result {
            Ok(resp) => {
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| FoldDbError::Config(format!("read S3 body '{key}': {e}")))?;
                let bytes = body.into_bytes();
                let record: T = serde_json::from_slice(&bytes)
                    .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                Ok(Some(record))
            }
            Err(e) => {
                if error_code(&e).as_deref() == Some("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(FoldDbError::Config(format!(
                        "get S3 object '{key}': {}",
                        short_error(&e)
                    )))
                }
            }
        }
    }

    pub(crate) async fn list_and_load_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, ReleaseRecord>> {
        let mut result = HashMap::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("releases/");
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                FoldDbError::Config(format!("list S3 'releases/': {}", short_error(&e)))
            })?;
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(rec) = self.get_single_object::<ReleaseRecord>(key).await? else {
                    continue;
                };
                result.insert(rec.release_id.clone(), rec);
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }
        Ok(result)
    }

    pub(crate) async fn list_and_load_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, ChannelRecord>> {
        let mut result = HashMap::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("channels/");
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                FoldDbError::Config(format!("list S3 'channels/': {}", short_error(&e)))
            })?;
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(rec) = self.get_single_object::<ChannelRecord>(key).await? else {
                    continue;
                };
                let channel_key =
                    schema_service_core::app_release::channel_key(&rec.app_id, &rec.channel);
                result.insert(channel_key, rec);
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }
        Ok(result)
    }

    pub(crate) async fn list_and_load_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        let mut result = HashMap::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("apps/");
            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                FoldDbError::Config(format!("list S3 'apps/': {}", short_error(&e)))
            })?;
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(rec) = self.get_single_object::<AppRecord>(key).await? else {
                    continue;
                };
                result.insert(rec.app_id.clone(), rec);
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }
        Ok(result)
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
                .key(key)
                .send()
                .await;
            match result {
                Ok(resp) => {
                    let etag = resp
                        .e_tag()
                        .ok_or_else(|| FoldDbError::Config(format!("no ETag for '{key}'")))?
                        .to_string();
                    let body =
                        resp.body.collect().await.map_err(|e| {
                            FoldDbError::Config(format!("read S3 body '{key}': {e}"))
                        })?;
                    let bytes = body.into_bytes();
                    let existing: ChannelRecord = serde_json::from_slice(&bytes)
                        .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))?;
                    Some((existing, etag))
                }
                Err(e) => {
                    if error_code(&e).as_deref() == Some("NoSuchKey") {
                        None
                    } else {
                        return Err(FoldDbError::Config(format!(
                            "get S3 object '{key}': {}",
                            short_error(&e)
                        )));
                    }
                }
            }
        };
        match existing {
            Some((existing, etag)) => {
                if existing.generation != record.generation - 1 {
                    return Err(FoldDbError::Config(format!(
                        "generation conflict: stored={}, new={}",
                        existing.generation, record.generation
                    )));
                }
                let body = serde_json::to_vec(record)
                    .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(ByteStream::from(body))
                    .content_type("application/json")
                    .if_match(etag)
                    .send()
                    .await
                    .map_err(|e| {
                        if is_precondition_failed(&e) {
                            FoldDbError::Config("generation conflict (ETag mismatch)".to_string())
                        } else {
                            FoldDbError::Config(format!(
                                "put S3 object '{key}': {}",
                                short_error(&e)
                            ))
                        }
                    })?;
            }
            None => {
                let body = serde_json::to_vec(record)
                    .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))?;
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(ByteStream::from(body))
                    .content_type("application/json")
                    .if_none_match("*")
                    .send()
                    .await
                    .map_err(|e| {
                        FoldDbError::Config(format!("put S3 object '{key}': {}", short_error(&e)))
                    })?;
            }
        }
        Ok(())
    }
}

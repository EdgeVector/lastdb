//! Per-record S3 storage helpers for apps, releases, and channels.

use aws_sdk_s3::primitives::ByteStream;
use schema_service_core::app_release::{ChannelRecord, ReleaseRecord};
use schema_service_core::snapshot::AppRecord;
use schema_types::{FoldDbError, FoldDbResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

use super::{error_code, is_precondition_failed, short_error, S3BlobPersistence};

type StoredObject = aws_sdk_s3::operation::get_object::GetObjectOutput;
type PutRequest = aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;

struct PreparedPut<'a, T: Clone> {
    record: Cow<'a, T>,
    conflict: Option<(u64, u64)>,
}

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
        let mut put = self.start_put(key, Self::encode_json(key, record)?);
        if use_conditional {
            put = put.if_none_match("*");
        }
        put.send()
            .await
            .map_err(|error| Self::put_s3_error(key, &error))?;
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
        self.start_put(key, Self::encode_json(key, record)?)
            .if_match(etag)
            .send()
            .await
            .map_err(|error| {
                if let Some((expected, supplied)) = generation_conflict {
                    if is_precondition_failed(&error) {
                        return FoldDbError::GenerationConflict { expected, supplied };
                    }
                }
                Self::put_s3_error(key, &error)
            })?;
        Ok(())
    }

    /// Encode one record for an S3 put.
    fn encode_json<T: Serialize>(key: &str, record: &T) -> FoldDbResult<Vec<u8>> {
        serde_json::to_vec(record)
            .map_err(|e| FoldDbError::Serialization(format!("serialize '{key}': {e}")))
    }

    /// Start one JSON put. The caller adds the match condition.
    fn start_put(&self, key: &str, body: Vec<u8>) -> PutRequest {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .body(ByteStream::from(body))
            .content_type("application/json")
    }

    /// The shared S3 text for a failed put.
    fn put_s3_error<E, R>(key: &str, error: &aws_sdk_s3::error::SdkError<E, R>) -> FoldDbError
    where
        E: aws_sdk_s3::error::ProvideErrorMetadata,
    {
        FoldDbError::S3(format!("put S3 object '{key}': {}", short_error(error)))
    }

    pub(crate) async fn get_single_object<T>(&self, key: &str) -> FoldDbResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        let Some(resp) = self.get_stored_object(key).await? else {
            return Ok(None);
        };
        Ok(Some(Self::decode_json_body(key, resp.body).await?))
    }

    /// Read one record and its ETag. `None` means the object is absent.
    async fn get_record_with_etag<T>(&self, key: &str) -> FoldDbResult<Option<(T, String)>>
    where
        T: DeserializeOwned,
    {
        let Some(resp) = self.get_stored_object(key).await? else {
            return Ok(None);
        };
        let etag = resp
            .e_tag()
            .ok_or_else(|| FoldDbError::S3(format!("no ETag for '{key}'")))?
            .to_string();
        let record = Self::decode_json_body(key, resp.body).await?;
        Ok(Some((record, etag)))
    }

    /// Read one definition object. `None` means the key is absent.
    async fn get_stored_object(&self, key: &str) -> FoldDbResult<Option<StoredObject>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.definition_key(key).as_ref())
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp)),
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

    /// Read and parse one object body.
    async fn decode_json_body<T>(key: &str, body: ByteStream) -> FoldDbResult<T>
    where
        T: DeserializeOwned,
    {
        let body = body
            .collect()
            .await
            .map_err(|e| FoldDbError::S3(format!("read S3 body '{key}': {e}")))?;
        serde_json::from_slice(&body.into_bytes())
            .map_err(|e| FoldDbError::Serialization(format!("parse '{key}': {e}")))
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

    /// Page one prefix and index each record by `key_of`.
    async fn list_record_map<T, K, F>(
        &self,
        prefix: &str,
        label: &str,
        key_of: F,
    ) -> FoldDbResult<HashMap<K, T>>
    where
        T: DeserializeOwned,
        K: Hash + Eq,
        F: Fn(&T) -> K,
    {
        let records = self.list_records::<T>(prefix, label).await?;
        Ok(records
            .into_iter()
            .map(|record| {
                let key = key_of(&record);
                (key, record)
            })
            .collect())
    }

    pub(crate) async fn list_and_load_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, ReleaseRecord>> {
        let prefix = self.definition_key("releases/");
        self.list_record_map::<ReleaseRecord, _, _>(prefix.as_ref(), "releases/", |record| {
            record.release_id.clone()
        })
        .await
    }

    pub(crate) async fn list_and_load_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, ChannelRecord>> {
        let prefix = self.definition_key("channels/");
        self.list_record_map::<ChannelRecord, _, _>(prefix.as_ref(), "channels/", |record| {
            schema_service_core::app_release::channel_key(&record.app_id, &record.channel)
        })
        .await
    }

    pub(crate) async fn list_and_load_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        // Apps stay outside the definition namespace. See `definition_key`.
        self.list_record_map::<AppRecord, _, _>("apps/", "apps/", |record| record.app_id.clone())
            .await
    }

    pub(crate) async fn put_release_preserving_revocation(
        &self,
        key: &str,
        record: &ReleaseRecord,
    ) -> FoldDbResult<()> {
        self.replace_or_create(key, record, |existing, record| {
            let mut final_record = record.clone();
            if existing.revoked_at.is_some() {
                final_record.revoked_at = existing.revoked_at;
                final_record.revocation_reason = existing.revocation_reason;
            }
            Ok(PreparedPut {
                record: Cow::Owned(final_record),
                conflict: None,
            })
        })
        .await
    }

    pub(crate) async fn put_channel_with_generation_check(
        &self,
        key: &str,
        record: &ChannelRecord,
    ) -> FoldDbResult<()> {
        self.replace_or_create(key, record, |existing, record| {
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
            Ok(PreparedPut {
                record: Cow::Borrowed(record),
                conflict: Some((existing.generation, record.generation)),
            })
        })
        .await
    }

    /// Replace one record when it exists. Create it when the key is absent.
    /// The caller prepares the stored record and any generation clash.
    async fn replace_or_create<'a, T, F>(
        &'a self,
        key: &str,
        record: &'a T,
        prepare: F,
    ) -> FoldDbResult<()>
    where
        T: Serialize + DeserializeOwned + Clone,
        F: FnOnce(T, &'a T) -> FoldDbResult<PreparedPut<'a, T>>,
    {
        if let Some((existing, etag)) = self.get_record_with_etag::<T>(key).await? {
            let prepared = prepare(existing, record)?;
            self.put_if_match(key, prepared.record.as_ref(), &etag, prepared.conflict)
                .await
        } else {
            self.put_single_object(key, record, true).await
        }
    }
}

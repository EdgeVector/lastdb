//! Replay remembers an erasure without leaving a record at the live key.

use super::{RecordAddress, RecordFormat};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::portable_mutation::{AppliedDelete, RecordKey, Stamp, MAX_MUTATION_ENTRY_BYTES};
use laststore::disk_run::MAX_RUN_KEY_BYTES;
use serde::{Deserialize, Serialize};

pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";
pub(crate) const DELETE_AGE_FAMILY: &str = "ade";
const MAX_DELETE_BYTES: usize = 512;
// The physical key can contain JSON escapes. Leave room for the logical
// portable key and framing while admitting the full native key range.
const MAX_DELETE_AGE_BYTES: usize = 6 * MAX_RUN_KEY_BYTES + MAX_MUTATION_ENTRY_BYTES + 1024;

pub(super) fn encode(key: &RecordAddress, stamp: &Stamp) -> StorageResult<KvMutation> {
    let value = serde_json::to_vec(&AppliedDelete::from(stamp.clone())).map_err(invalid)?;
    Ok(KvMutation::put(address(key.as_str()), value))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteAge {
    pub(crate) marker: String,
    pub(crate) stamp: Stamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) primary: Option<RecordKey>,
}

pub(super) fn encode_age(key: &RecordAddress, stamp: &Stamp) -> StorageResult<KvMutation> {
    let age = DeleteAge {
        marker: address(key.as_str()),
        stamp: stamp.clone(),
        primary: Some(RecordKey::try_from(key.api_key().clone()).map_err(invalid)?),
    };
    let bytes = serde_json::to_vec(&age).map_err(invalid)?;
    require_age_size(&bytes)?;
    Ok(KvMutation::put(
        age_key(stamp.authored_at_ms, &age.marker),
        bytes,
    ))
}

pub(in crate::storage) fn validate_age_key(key: &str) -> StorageResult<()> {
    if age_ms(key).is_none() {
        return Err(invalid("invalid delete age coordinate"));
    }
    Ok(())
}

pub(in crate::storage) fn is_age_key(key: &str) -> bool {
    key.split_once(':')
        .is_some_and(|(family, _)| family == DELETE_AGE_FAMILY)
}

pub(in crate::storage) fn validate_age_row(key: &str, bytes: &[u8]) -> StorageResult<()> {
    decode_age_row(key, bytes).map(|_| ())
}

fn decode_age_row(key: &str, bytes: &[u8]) -> StorageResult<DeleteAge> {
    require_age_size(bytes)?;
    let age: DeleteAge = serde_json::from_slice(bytes).map_err(invalid)?;
    age.stamp.position.validate().map_err(invalid)?;
    if let Some(primary) = &age.primary {
        primary.validate().map_err(invalid)?;
    }
    let Some(time) = age_ms(key) else {
        return Err(invalid("invalid delete age coordinate"));
    };
    if record_key(&age.marker).is_none()
        || age.stamp.authored_at_ms != time
        || age_key(time, &age.marker) != key
    {
        return Err(invalid("delete age row points outside its marker"));
    }
    crate::storage::current_keys::validate(
        crate::storage::format::MAIN_NAMESPACE,
        age.marker.as_bytes(),
    )?;
    Ok(age)
}

fn require_age_size(bytes: &[u8]) -> StorageResult<()> {
    if bytes.len() > MAX_DELETE_AGE_BYTES {
        return Err(invalid("delete age row exceeds its bound"));
    }
    Ok(())
}

fn age_ms(key: &str) -> Option<u64> {
    let (family, rest) = key.split_once(':')?;
    let (time, digest) = rest.split_once(':')?;
    if family != DELETE_AGE_FAMILY
        || time.len() != 20
        || digest.len() != 64
        || !time.bytes().all(|byte| byte.is_ascii_digit())
        || !digest
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    time.parse().ok()
}

fn age_key(time: u64, marker: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{DELETE_AGE_FAMILY}:{time:020}:{:x}",
        Sha256::digest(marker.as_bytes())
    )
}

pub(in crate::storage) fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
    if bytes.len() > MAX_DELETE_BYTES {
        return Err(invalid("encoded value exceeds its bound"));
    }
    let entry: AppliedDelete = serde_json::from_slice(bytes).map_err(invalid)?;
    entry.delete_position.validate().map_err(invalid)?;
    Ok(entry.into())
}

pub(crate) fn record_key(key: &str) -> Option<&str> {
    let (family, record) = key.split_once(':')?;
    (family == APPLIED_DELETE_FAMILY).then_some(record)
}

fn address(record: &str) -> String {
    format!("{APPLIED_DELETE_FAMILY}:{record}")
}

impl RecordFormat {
    pub(crate) async fn stage_delete_reclamation(
        &self,
        cutoff_ms: u64,
    ) -> StorageResult<ReclaimedDeletes> {
        self.require_scope().map_err(invalid)?;
        let start = format!("{DELETE_AGE_FAMILY}:");
        let end = format!("{DELETE_AGE_FAMILY}:{cutoff_ms:020}:");
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), 32)
            .await?;
        let mut markers = Vec::with_capacity(rows.len());
        for (key, value) in &rows {
            markers.push(decode_age_row(
                std::str::from_utf8(key).map_err(invalid)?,
                value,
            )?);
        }
        let values = self
            .main_store
            .inner()
            .get_many(
                markers
                    .iter()
                    .map(|age| age.marker.as_bytes().to_vec())
                    .collect(),
            )
            .await?;
        if values.len() != rows.len() {
            return Err(invalid("incomplete delete reclamation preimages"));
        }
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        let mut reclaimed = ReclaimedDeletes::default();
        for ((age, entry), body) in rows
            .into_iter()
            .map(|(key, _)| key)
            .zip(markers)
            .zip(values)
        {
            if let Some(bytes) = body {
                let stamp = decode(&bytes)?;
                if stamp.authored_at_ms < cutoff_ms && stamp == entry.stamp {
                    reclaimed.markers += 1;
                    reclaimed.through_ms = Some(
                        reclaimed
                            .through_ms
                            .map_or(stamp.authored_at_ms, |time| time.max(stamp.authored_at_ms)),
                    );
                    mutations.push(KvMutation::delete(entry.marker.into_bytes()));
                }
            }
            mutations.push(KvMutation::delete(age));
        }
        if !mutations.is_empty() {
            self.main_store.inner().batch_mutate(mutations).await?;
        }
        Ok(reclaimed)
    }

    pub(crate) async fn applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> StorageResult<Vec<Option<Stamp>>> {
        let rows = self
            .main_store
            .inner()
            .get_many(
                keys.iter()
                    .map(|key| address(key.as_str()).into_bytes())
                    .collect(),
            )
            .await?;
        if rows.len() != keys.len() {
            return Err(invalid("incomplete applied-delete read"));
        }
        rows.iter()
            .map(|row| row.as_deref().map(decode).transpose())
            .collect()
    }
}

#[derive(Default)]
pub(crate) struct ReclaimedDeletes {
    pub(crate) markers: u64,
    pub(crate) through_ms: Option<u64>,
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("applied delete: {error}"))
}

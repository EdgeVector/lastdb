//! One confirmed record and bounded attachment pins behind pending local writes.

use super::MutationManager;
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::format::metadata::{
    file_ref_key, index_key, inline_key, record_key, ReplayCapture, ReplayStamp,
};
use crate::storage::format::{records::RecordAddress, replay_pin};
use lastdb_model::record_value::{StoredRecord, MAX_RECORD_BYTES};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};

pub(super) struct Baseline {
    pub stamp: Option<ReplayStamp>,
    pub record: Option<StoredRecord>,
}

/// The nested record uses ordinary serde. Author keys live in its meta map, not in skipped fields.
#[derive(Serialize)]
struct StoredBaseline<'a> {
    stamp: &'a Option<ReplayStamp>,
    record: Option<StoredRecord>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadedBaseline {
    stamp: Option<ReplayStamp>,
    record: Option<StoredRecord>,
}

impl Serialize for Baseline {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StoredBaseline {
            stamp: &self.stamp,
            record: self.record.as_ref().map(StoredRecord::authors_in_meta),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Baseline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut saved = LoadedBaseline::deserialize(deserializer)?;
        if let Some(record) = saved.record.as_mut() {
            record.authors_from_meta();
        }
        Ok(Self {
            stamp: saved.stamp,
            record: saved.record,
        })
    }
}

pub(super) fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("invalid replay baseline: {error}"))
}

pub(super) fn decode(bytes: &[u8]) -> Result<Baseline, SchemaError> {
    let saved: Baseline = serde_json::from_slice(bytes).map_err(invalid)?;
    if let Some(record) = &saved.record {
        validate_record(record)?;
    }
    Ok(saved)
}

pub(super) fn validate_record(record: &StoredRecord) -> Result<(), SchemaError> {
    if record.encode_uncapped().map_err(invalid)?.len() > MAX_RECORD_BYTES {
        return Err(invalid("record exceeds the 64 KiB cap"));
    }
    Ok(())
}

pub(super) fn file_path(
    address: &str,
    record: Option<&StoredRecord>,
) -> Result<Option<String>, SchemaError> {
    record
        .and_then(|r| r.file_version.as_ref())
        .map(|version| record_file_path(address, version).map_err(invalid))
        .transpose()
}

impl MutationManager {
    pub(super) async fn replay_capture(&self, address: &str) -> Result<ReplayCapture, SchemaError> {
        self.db_ops
            .metadata()
            .read_replay_captures(&[address])
            .await?
            .pop()
            .ok_or_else(|| invalid("missing replay capture"))
    }

    pub(super) async fn stage_replay_baselines(
        &self,
        schema: &str,
        addresses: &[RecordAddress],
        preimages: &[Option<Arc<StoredRecord>>],
    ) -> Result<BTreeSet<String>, SchemaError> {
        if addresses.len() != preimages.len() {
            return Err(invalid("incomplete preimages"));
        }
        let keys: Vec<_> = addresses.iter().map(RecordAddress::as_str).collect();
        let captures = self.db_ops.metadata().read_replay_captures(&keys).await?;
        let inline_addresses: Vec<_> = keys
            .iter()
            .zip(preimages)
            .zip(&captures)
            .filter(|((_, record), capture)| {
                !capture.pending && record.as_ref().is_some_and(|r| r.inline_file.is_some())
            })
            .map(|((addr, _), _)| (*addr).to_owned())
            .collect();
        let inline =
            replay_pin::read_inline_rows(&self.db_ops.records().format, &inline_addresses).await?;
        if inline.len() != inline_addresses.len() {
            return Err(invalid("incomplete inline preimages"));
        }
        let mut inline = inline.into_iter();
        let (mut rows, mut keep) = (Vec::new(), BTreeSet::new());
        for ((address, preimage), capture) in keys.into_iter().zip(preimages).zip(captures) {
            let baseline = if capture.pending {
                capture.baseline.as_deref().map(decode).transpose()?
            } else {
                let record = preimage.as_deref().filter(|r| r.is_live()).cloned();
                let inline_row = if record.as_ref().is_some_and(|r| r.inline_file.is_some()) {
                    inline
                        .next()
                        .ok_or_else(|| invalid("incomplete inline preimages"))?
                } else {
                    None
                };
                if let Some(marker) = record.as_ref().and_then(|r| r.inline_file.as_ref()) {
                    super::attachments::inline_row(
                        inline_row
                            .as_deref()
                            .ok_or_else(|| invalid("inline preimage missing"))?,
                        marker,
                    )?;
                }
                let baseline = Baseline {
                    stamp: capture.winner,
                    record,
                };
                rows.extend([
                    (
                        record_key(address, "baseline"),
                        Some(serde_json::to_vec(&baseline).map_err(invalid)?),
                    ),
                    (
                        index_key(schema, address),
                        Some(address.as_bytes().to_vec()),
                    ),
                    (inline_key(address), inline_row),
                    (file_ref_key(address), None),
                ]);
                Some(baseline)
            };
            if let Some(path) =
                file_path(address, baseline.as_ref().and_then(|b| b.record.as_ref()))?
            {
                keep.insert(path);
            }
        }
        self.db_ops.metadata().stage_replay_rows(rows).await?;
        Ok(keep)
    }

    pub(super) async fn release_replay_baseline(
        &self,
        schema: &str,
        address: &str,
    ) -> Result<(), SchemaError> {
        let capture = self.replay_capture(address).await?;
        let baseline = capture.baseline.as_deref().map(decode).transpose()?;
        let live = self.replay_preimage(address.to_owned()).await?;
        self.retire_replay_file(
            address,
            baseline.as_ref().and_then(|b| b.record.as_ref()),
            live.as_deref(),
            None,
        )?;
        self.db_ops
            .metadata()
            .stage_replay_rows(vec![
                (inline_key(address), None),
                (file_ref_key(address), None),
                (index_key(schema, address), None),
                (record_key(address, "baseline"), None),
            ])
            .await
    }

    pub(super) fn retire_replay_file(
        &self,
        address: &str,
        old: Option<&StoredRecord>,
        live: Option<&StoredRecord>,
        next: Option<&StoredRecord>,
    ) -> Result<(), SchemaError> {
        if let Some(path) = file_path(address, old)? {
            if Some(&path) != file_path(address, live)?.as_ref()
                && Some(&path) != file_path(address, next)?.as_ref()
            {
                replay_pin::retire_files(&self.db_ops.native_runtime(), vec![path])?;
            }
        }
        Ok(())
    }
}

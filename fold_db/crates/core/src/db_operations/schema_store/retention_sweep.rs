//! TTL sweep floors, format checks, backfill, and paged age/partition reads.

use super::{SchemaStore, StateChange};
use crate::db_operations::record_store::RangeWalk;
use crate::schema::schema_types::SchemaRetentionPolicy;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::format::schemas::keys::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lastdb_model::kind_partition::exact_prefix_bounds;
use schema_types::{
    field_range_profile::FieldRangeProfile, range_storage_profile::RangeStorageProfile,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

const FULL_PASS_EVERY: u32 = 64;

#[derive(Debug, Default)]
struct Floor {
    after: Option<KeyValue>,
    passes: u32,
    low: Option<String>,
}

impl Floor {
    fn clear(&mut self) {
        self.after = None;
        self.passes = 0;
    }
}

/// Process-memory floors of the TTL sweep per schema and hash partition.
#[derive(Debug, Default)]
pub struct RetentionFloors {
    schemas: Mutex<HashMap<String, HashMap<String, Floor>>>,
}

impl RetentionFloors {
    pub fn start(&self, schema: &str, hash: &str) -> Option<KeyValue> {
        let mut schemas = self.lock();
        let floor = schemas
            .entry(schema.to_owned())
            .or_default()
            .entry(hash.to_owned())
            .or_default();
        floor.low = None;
        if floor.after.is_some() {
            floor.passes += 1;
            if floor.passes >= FULL_PASS_EVERY {
                floor.clear();
            }
        }
        floor.after.clone()
    }

    pub fn keep(&self, schema: &str, hash: &str, after: &KeyValue) {
        let mut schemas = self.lock();
        let floor = schemas
            .entry(schema.to_owned())
            .or_default()
            .entry(hash.to_owned())
            .or_default();
        if floor
            .low
            .as_deref()
            .is_some_and(|low| low <= range_of(after))
        {
            floor.clear();
            return;
        }
        floor.after = Some(after.clone());
    }

    pub fn end(&self, schema: &str, hash: &str) {
        let mut schemas = self.lock();
        let Some(hashes) = schemas.get_mut(schema) else {
            return;
        };
        if hashes.get(hash).is_some_and(|floor| floor.after.is_none()) {
            hashes.remove(hash);
        }
        if hashes.is_empty() {
            schemas.remove(schema);
        }
    }

    fn note_written(&self, schema: &str, keys: &[(String, String)]) {
        let mut schemas = self.lock();
        let Some(hashes) = schemas.get_mut(schema) else {
            return;
        };
        for (hash, range) in keys {
            let Some(floor) = hashes.get_mut(hash) else {
                continue;
            };
            if floor
                .after
                .as_ref()
                .is_some_and(|after| range.as_str() <= range_of(after))
            {
                floor.clear();
            }
            if floor.low.as_ref().is_none_or(|low| range < low) {
                floor.low = Some(range.clone());
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, HashMap<String, Floor>>> {
        self.schemas.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn range_of(key: &KeyValue) -> &str {
    key.range.as_deref().unwrap_or("")
}

impl SchemaStore {
    #[must_use]
    pub fn retention_floors(&self) -> &RetentionFloors {
        &self.retention_floors
    }

    pub(crate) fn note_live_writes<'k>(
        &self,
        schema: &Schema,
        live: impl Iterator<Item = &'k KeyValue>,
    ) -> Result<(), SchemaError> {
        let partitioned = match schema.schema_type {
            DeclarativeSchemaType::Range => false,
            DeclarativeSchemaType::HashRange => true,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::Single => return Ok(()),
        };
        let keys: Vec<(String, String)> = live
            .map(|key| {
                let hash = if partitioned {
                    key.hash.clone().unwrap_or_default()
                } else {
                    String::new()
                };
                (hash, range_of(key).to_owned())
            })
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let floors = Arc::clone(&self.retention_floors);
        let name = schema.name.clone();
        crate::storage::disk_pack::transaction::after_commit(&self.runtime, async move {
            floors.note_written(&name, &keys);
            Ok(())
        })?;
        Ok(())
    }

    pub(in crate::db_operations) async fn require_retention_format(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_owned()))?;
        if !matches!(
            schema.schema_type,
            DeclarativeSchemaType::Range | DeclarativeSchemaType::HashRange
        ) {
            return Ok(());
        }
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        if !matches!(
            profile,
            FieldRangeProfile::TypedV1 {
                scalar: RangeStorageProfile::StringUtf8V1
            }
        ) {
            return Err(SchemaError::InvalidData(
                "range TTL requires a String range with the 20-digit timestamp prefix".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn backfill_retention_rows(
        &self,
        name: &str,
        clocks: Vec<(KeyValue, u64)>,
        range_keyed: bool,
    ) -> Result<usize, SchemaError> {
        if self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(name))
            .await?
            .is_none()
        {
            return Err(SchemaError::InvalidData(
                "retention backfill requires an active policy".into(),
            ));
        }
        let mut unique: HashMap<KeyValue, u64> = HashMap::new();
        for (key, clock) in clocks {
            unique
                .entry(key)
                .and_modify(|old| *old = (*old).max(clock))
                .or_insert(clock);
        }
        if !range_keyed {
            let ages: Vec<_> = unique
                .iter()
                .map(|(key, age)| (key.clone(), *age))
                .collect();
            self.record_schema_retention_write_ages(name, &ages).await?;
            return Ok(unique.len());
        }
        let mut puts = HashMap::new();
        for key in unique.keys() {
            let hash = key
                .hash
                .as_ref()
                .ok_or_else(|| SchemaError::InvalidData("backfill key has no hash".into()))?;
            puts.insert(
                retention_hash_partition_key(name, hash),
                SchemaRetentionHashPartition { hash: hash.clone() },
            );
        }
        self.format
            .batch_states(
                puts.into_values()
                    .map(|entry| StateChange::Partition {
                        schema: name,
                        entry,
                    })
                    .collect(),
            )
            .await?;
        Ok(unique.len())
    }

    pub async fn expired_schema_retention_page(
        &self,
        schema_name: &str,
        cutoff: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<String>), SchemaError> {
        let prefix = retention_age_prefix(schema_name);
        let first = format!("{prefix}{:020}", 0);
        let end = format!("{prefix}{cutoff:020}");
        let start = match after {
            Some(cursor) if cursor >= first.as_str() && cursor < end.as_str() => {
                format!("{cursor}\0")
            }
            Some(_) => return Err(SchemaError::InvalidData("invalid TTL age cursor".into())),
            None => first,
        };
        let rows = self
            .schema_states_store
            .scan_items_in_range_paged::<SchemaRetentionAgeEntry>(&start, &end, limit.clamp(1, 256))
            .await?;
        let next = rows.last().map(|(key, _)| key.clone());
        let latest_keys: Vec<_> = rows
            .iter()
            .map(|(_, entry)| retention_age_latest_key(schema_name, &entry.key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let keys = rows
            .into_iter()
            .zip(latest)
            .filter_map(|((_, entry), current)| {
                (current.as_ref() == Some(&entry)).then_some(entry.key)
            })
            .collect();
        Ok((keys, next))
    }

    pub async fn schema_retention_hash_partitions_page(
        &self,
        schema: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>), SchemaError> {
        let (first, end) = exact_prefix_bounds(&retention_hash_partition_prefix(schema));
        let start = match cursor {
            Some(cursor) => URL_SAFE_NO_PAD.decode(cursor).map_err(invalid_partition)?,
            None => first.as_bytes().to_vec(),
        };
        if start.as_slice() < first.as_bytes() || start.as_slice() >= end.as_bytes() {
            return Err(invalid_partition(
                "partition cursor differs from its retention anchor",
            ));
        }
        let mut walk = RangeWalk::new(&start, end.as_bytes(), limit.clamp(1, 256));
        while let Some((start, end, asked)) = walk.request() {
            let result = self
                .schema_states_store
                .scan_range_yielding(&start, &end, asked, 0)
                .await;
            match result.map_err(SchemaError::from) {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    let hashes = scan
                        .rows
                        .into_iter()
                        .map(|(_, bytes)| {
                            serde_json::from_slice::<SchemaRetentionHashPartition>(&bytes)
                                .map(|entry| entry.hash)
                                .map_err(invalid_partition)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let cursor = walk
                        .request()
                        .map(|(start, _, _)| URL_SAFE_NO_PAD.encode(start));
                    return Ok((hashes, cursor));
                }
                Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                    if !walk.read_too_big() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok((Vec::new(), None))
    }

    pub async fn schema_retention_hash_partitions(
        &self,
        schema: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let mut hashes = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = self
                .schema_retention_hash_partitions_page(schema, cursor.as_deref(), 256)
                .await?;
            hashes.extend(page);
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        hashes.sort();
        hashes.dedup();
        Ok(hashes)
    }

    pub async fn retention_keys_still_expired(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
        cutoff: u64,
    ) -> Result<Vec<KeyValue>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let latest_keys: Vec<_> = keys
            .iter()
            .map(|key| retention_age_latest_key(schema_name, key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        Ok(keys
            .iter()
            .zip(latest)
            .filter(|(_, entry)| entry.as_ref().is_some_and(|e| e.written_at < cutoff))
            .map(|(key, _)| key.clone())
            .collect())
    }
}

fn invalid_partition(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("retention partition page: {error}"))
}

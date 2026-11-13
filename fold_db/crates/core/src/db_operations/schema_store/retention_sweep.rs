//! Retention sweep floors, format checks, and paged TTL sweep queries.

use super::retention::{invalid, latest_keys};
use super::SchemaStore;
use crate::db_operations::record_store::RangeWalk;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::schemas::keys::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lastdb_model::kind_partition::exact_prefix_bounds;
use schema_types::field_range_profile::{FieldRangeProfile, RangeStorageProfile};
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
        } else {
            floor.after = Some(after.clone());
        }
    }

    pub fn end(&self, schema: &str, hash: &str) {
        let mut schemas = self.lock();
        if let Some(hashes) = schemas.get_mut(schema) {
            if hashes.get(hash).is_some_and(|f| f.after.is_none()) {
                hashes.remove(hash);
            }
            if hashes.is_empty() {
                schemas.remove(schema);
            }
        }
    }

    fn note_written(&self, schema: &str, keys: &[(String, String)]) {
        let mut schemas = self.lock();
        let Some(hashes) = schemas.get_mut(schema) else {
            return;
        };
        for (hash, range) in keys {
            if let Some(floor) = hashes.get_mut(hash) {
                if floor
                    .after
                    .as_ref()
                    .is_some_and(|a| range.as_str() <= range_of(a))
                {
                    floor.clear();
                }
                if floor.low.as_ref().is_none_or(|low| range < low) {
                    floor.low = Some(range.clone());
                }
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
            .map(|k| {
                (
                    if partitioned {
                        k.hash.clone().unwrap_or_default()
                    } else {
                        String::new()
                    },
                    range_of(k).to_owned(),
                )
            })
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let (floors, name) = (Arc::clone(&self.retention_floors), schema.name.clone());
        transaction::after_commit(&self.runtime, async move {
            floors.note_written(&name, &keys);
            Ok(())
        })
        .map_err(SchemaError::from)
    }

    pub(super) async fn require_retention_format(
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
        matches!(
            profile,
            FieldRangeProfile::TypedV1 {
                scalar: RangeStorageProfile::StringUtf8V1
            }
        )
        .then_some(())
        .ok_or_else(|| {
            invalid("range TTL requires a String range with the 20-digit timestamp prefix")
        })
    }

    pub async fn expired_schema_retention_page(
        &self,
        schema_name: &str,
        cutoff: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<String>), SchemaError> {
        let prefix = retention_age_prefix(schema_name);
        let (first, end) = (
            format!("{prefix}{:020}", 0),
            format!("{prefix}{cutoff:020}"),
        );
        let start = match after {
            Some(c) if c >= first.as_str() && c < end.as_str() => format!("{c}\0"),
            Some(_) => return Err(invalid("invalid TTL age cursor")),
            None => first,
        };
        let rows = self
            .schema_states_store
            .scan_items_in_range_paged::<SchemaRetentionAgeEntry>(&start, &end, limit.clamp(1, 256))
            .await?;
        let next = rows.last().map(|(k, _)| k.clone());
        let lks: Vec<_> = rows
            .iter()
            .map(|(_, e)| retention_age_latest_key(schema_name, &e.key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&lks)
            .await?;
        let keys = rows
            .into_iter()
            .zip(latest)
            .filter_map(|((_, e), cur)| (cur.as_ref() == Some(&e)).then_some(e.key))
            .collect();
        Ok((keys, next))
    }

    pub async fn schema_retention_hash_partitions_page(
        &self,
        schema: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>), SchemaError> {
        let bad = |e: &dyn std::fmt::Display| invalid(format!("retention partition page: {e}"));
        let (first, end) = exact_prefix_bounds(&retention_hash_partition_prefix(schema));
        let start = match cursor {
            Some(c) => URL_SAFE_NO_PAD.decode(c).map_err(|e| bad(&e))?,
            None => first.as_bytes().to_vec(),
        };
        if start.as_slice() < first.as_bytes() || start.as_slice() >= end.as_bytes() {
            return Err(bad(&"partition cursor differs from its retention anchor"));
        }
        let mut walk = RangeWalk::new(&start, end.as_bytes(), limit.clamp(1, 256));
        while let Some((start, end, asked)) = walk.request() {
            match self
                .schema_states_store
                .scan_range_yielding(&start, &end, asked, 0)
                .await
                .map_err(SchemaError::from)
            {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    let hashes = scan
                        .rows
                        .into_iter()
                        .map(|(_, b)| {
                            serde_json::from_slice::<SchemaRetentionHashPartition>(&b)
                                .map(|e| e.hash)
                                .map_err(|e| bad(&e))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    return Ok((
                        hashes,
                        walk.request().map(|(s, _, _)| URL_SAFE_NO_PAD.encode(s)),
                    ));
                }
                Err(SchemaError::ReadResourceLimit { .. }) if walk.read_too_big() => {}
                Err(error) => return Err(error),
            }
        }
        Ok((Vec::new(), None))
    }

    pub async fn schema_retention_hash_partitions(
        &self,
        schema: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let (mut hashes, mut cursor) = (Vec::new(), None);
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
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys(schema_name, keys))
            .await?;
        Ok(keys
            .iter()
            .zip(latest)
            .filter_map(|(k, e)| e.is_some_and(|e| e.written_at < cutoff).then(|| k.clone()))
            .collect())
    }
}

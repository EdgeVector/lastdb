//! Schema retention policy, age, and hash-partition state key codecs.

use super::{
    put_json, require_name, RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX,
    RETENTION_HASH_PARTITION_KEY_PREFIX, RETENTION_POLICY_KEY_PREFIX,
};
use crate::schema::types::catalog_types::{SchemaRetentionPolicy, SchemaState};
use crate::schema::types::KeyValue;
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::hex::sha256_hex as fp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionAgeEntry {
    pub(crate) key: KeyValue,
    pub(crate) written_at: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionHashPartition {
    pub(crate) hash: String,
}

#[derive(Clone, Copy)]
pub(crate) enum StateKey {
    State,
    Policy,
}

#[derive(Clone, Copy)]
pub(crate) enum Tracking {
    Age,
    Latest,
    Partition,
}

pub(crate) enum StateDelete<'a> {
    Key {
        schema: &'a str,
        kind: StateKey,
    },
    Selected {
        schema: &'a str,
        kind: Tracking,
        keys: Vec<String>,
    },
}

pub(crate) enum StateChange<'a> {
    State {
        schema: &'a str,
        state: &'a SchemaState,
    },
    Policy {
        schema: &'a str,
        policy: &'a SchemaRetentionPolicy,
    },
    Age {
        schema: &'a str,
        entry: SchemaRetentionAgeEntry,
        latest: bool,
    },
    Partition {
        schema: &'a str,
        entry: SchemaRetentionHashPartition,
    },
}

pub(crate) fn retention_policy_key(schema: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{schema}")
}

pub(crate) fn retention_age_prefix(schema: &str) -> String {
    format!("{RETENTION_AGE_KEY_PREFIX}{}\0", fp(schema))
}

pub(crate) fn retention_age_latest_prefix(schema: &str) -> String {
    format!("{RETENTION_AGE_LATEST_PREFIX}{}\0", fp(schema))
}

pub(crate) fn retention_age_key(schema: &str, written_at: u64, key: &KeyValue) -> String {
    let prefix = retention_age_prefix(schema);
    format!("{prefix}{written_at:020}\0{}", fp(key.to_storage_key()))
}

pub(crate) fn retention_age_latest_key(schema: &str, key: &KeyValue) -> String {
    let prefix = retention_age_latest_prefix(schema);
    format!("{prefix}{}", fp(key.to_storage_key()))
}

pub(crate) fn retention_hash_partition_prefix(schema: &str) -> String {
    format!("{RETENTION_HASH_PARTITION_KEY_PREFIX}{}\0", fp(schema))
}

pub(super) fn encode_state(change: StateChange<'_>) -> StorageResult<(&str, KvMutation, bool)> {
    match change {
        StateChange::State { schema, state } => {
            Ok((schema, put_json(schema.into(), state)?, false))
        }
        StateChange::Policy { schema, policy } => {
            if policy.ttl_seconds == 0 || policy.hash_partitions.iter().any(String::is_empty) {
                return Err(StorageError::invalid("invalid schema retention policy"));
            }
            let op = put_json(retention_policy_key(schema), policy)?;
            Ok((schema, op, false))
        }
        StateChange::Age {
            schema,
            entry,
            latest,
        } => {
            let key = if latest {
                retention_age_latest_key(schema, &entry.key)
            } else {
                retention_age_key(schema, entry.written_at, &entry.key)
            };
            Ok((schema, put_json(key, &entry)?, true))
        }
        StateChange::Partition { schema, entry } => {
            let prefix = retention_hash_partition_prefix(schema);
            let key = format!("{prefix}{}", fp(&entry.hash));
            Ok((schema, put_json(key, &entry)?, true))
        }
    }
}

pub(super) fn encode_delete(
    change: StateDelete<'_>,
    out: &mut Vec<KvMutation>,
) -> StorageResult<()> {
    let bad = || StorageError::invalid("retention delete key differs from its owner/family");
    match change {
        StateDelete::Key { schema, kind } => {
            require_name(schema)?;
            let key = match kind {
                StateKey::State => schema.to_owned(),
                StateKey::Policy => retention_policy_key(schema),
            };
            out.push(KvMutation::delete(key));
        }
        StateDelete::Selected { schema, kind, keys } => {
            require_name(schema)?;
            let prefix = match kind {
                Tracking::Age => retention_age_prefix(schema),
                Tracking::Latest => retention_age_latest_prefix(schema),
                Tracking::Partition => retention_hash_partition_prefix(schema),
            };
            for key in keys {
                let suffix = key.strip_prefix(&prefix).ok_or_else(bad)?;
                let digest = if matches!(kind, Tracking::Age) {
                    let (clock, dig) = suffix.split_once('\0').ok_or_else(bad)?;
                    if clock.len() != 20 || clock.parse::<u64>().is_err() {
                        return Err(bad());
                    }
                    dig
                } else {
                    suffix
                };
                if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(bad());
                }
                out.push(KvMutation::delete(key));
            }
        }
    }
    Ok(())
}

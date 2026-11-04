//! Typed state data; keys and JSON shapes are selected by the format.

use super::keys::*;
use super::{require_name, row, Row};
use crate::schema::{SchemaNameClaim, SchemaRetentionPolicy, SchemaState};
use crate::storage::error::StorageResult;
use crate::storage::StorageError;

pub(crate) enum StateChange<'a> {
    State {
        schema: &'a str,
        state: &'a SchemaState,
    },
    Policy {
        schema: &'a str,
        policy: &'a SchemaRetentionPolicy,
    },
    Anchor {
        schema: &'a str,
        anchor: &'a SchemaRetentionAnchor,
    },
    Claim {
        schema: &'a str,
        claim: SchemaNameClaim,
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

pub(super) fn tracking_owner(change: &StateChange<'_>) -> Option<String> {
    match change {
        StateChange::Age { schema, .. } | StateChange::Partition { schema, .. } => {
            Some((*schema).to_owned())
        }
        _ => None,
    }
}

pub(super) fn encode(change: StateChange<'_>) -> StorageResult<Row> {
    let schema = match &change {
        StateChange::State { schema, .. }
        | StateChange::Policy { schema, .. }
        | StateChange::Anchor { schema, .. }
        | StateChange::Claim { schema, .. }
        | StateChange::Age { schema, .. }
        | StateChange::Partition { schema, .. } => *schema,
    };
    require_name(schema)?;
    match change {
        StateChange::State { state, .. } => row(schema.to_owned(), state),
        StateChange::Policy { policy, .. } => {
            if policy.ttl_seconds == 0 || policy.hash_partitions.iter().any(String::is_empty) {
                return Err(StorageError::InvalidOperation(
                    "invalid schema retention policy".into(),
                ));
            }
            row(retention_policy_key(schema), policy)
        }
        StateChange::Anchor { anchor, .. } => {
            require_name(&anchor.anchor)?;
            row(retention_anchor_key(schema), anchor)
        }
        StateChange::Claim { claim, .. } => row(name_claim_key(schema), &claim),
        StateChange::Age { entry, latest, .. } => {
            let key = if latest {
                retention_age_latest_key(schema, &entry.key)
            } else {
                retention_age_key(schema, entry.written_at, &entry.key)
            };
            row(key, &entry)
        }
        StateChange::Partition { entry, .. } => {
            row(retention_hash_partition_key(schema, &entry.hash), &entry)
        }
    }
}

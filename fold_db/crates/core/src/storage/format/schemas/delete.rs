//! Write-time validation of selected retention deletions; no startup audit.

use super::keys::*;
use super::require_name;
use crate::storage::error::StorageResult;
use crate::storage::StorageError;

#[derive(Clone, Copy)]
pub(crate) enum StateKey {
    State,
    Policy,
    Anchor,
    Claim,
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

pub(super) fn encode(changes: Vec<StateDelete<'_>>) -> StorageResult<Vec<Vec<u8>>> {
    let mut output = Vec::new();
    for change in changes {
        match change {
            StateDelete::Key { schema, kind } => {
                output.push(key(schema, kind)?);
            }
            StateDelete::Selected { schema, kind, keys } => {
                require_name(schema)?;
                let prefix = match kind {
                    Tracking::Age => retention_age_prefix(schema),
                    Tracking::Latest => retention_age_latest_prefix(schema),
                    Tracking::Partition => retention_hash_partition_prefix(schema),
                };
                for key in keys {
                    let suffix = key.strip_prefix(&prefix).ok_or_else(invalid)?;
                    let digest = if matches!(kind, Tracking::Age) {
                        let (clock, digest) = suffix.split_once('\0').ok_or_else(invalid)?;
                        if clock.len() != 20
                            || !clock.bytes().all(|byte| byte.is_ascii_digit())
                            || clock.parse::<u64>().is_err()
                        {
                            return Err(invalid());
                        }
                        digest
                    } else {
                        suffix
                    };
                    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(invalid());
                    }
                    output.push(key.into_bytes());
                }
            }
        }
    }
    Ok(output)
}

fn invalid() -> StorageError {
    StorageError::InvalidOperation("retention delete key differs from its owner/family".into())
}

pub(super) fn key(schema: &str, kind: StateKey) -> StorageResult<Vec<u8>> {
    require_name(schema)?;
    Ok(match kind {
        StateKey::State => schema.to_owned(),
        StateKey::Policy => retention_policy_key(schema),
        StateKey::Anchor => retention_anchor_key(schema),
        StateKey::Claim => name_claim_key(schema),
    }
    .into_bytes())
}

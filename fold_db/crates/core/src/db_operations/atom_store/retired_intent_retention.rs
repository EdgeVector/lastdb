//! Blob dwell reads retired-intent blob roots so a retained blob is not deleted.
//! No expiry or release exists here.

use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

mod storage;

/// This namespace uses the normal encryption wrapper and default backup role.
pub(crate) const RETIRED_INTENT_RETENTION_NAMESPACE: &str = "retired_intent_retention";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetOwners {
    version: u8,
    kind: String,
    storage_prefix: Option<String>,
    target: String,
    owners: BTreeSet<String>,
}

fn refused(message: &'static str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

fn source_id_valid(source_id: &str) -> bool {
    source_id.len() == 64
        && source_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

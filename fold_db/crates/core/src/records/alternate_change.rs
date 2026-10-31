//! The durable row of one owner-directed alternate key declaration change.

use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The namespace that holds one change row per schema.
pub(crate) const NAMESPACE: &str = "alternate_changes";

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Change {
    pub(crate) id: String,
    pub(crate) source: Schema,
    pub(crate) target: Schema,
    pub(crate) phase: Phase,
    pub(crate) expected: BTreeMap<String, u64>,
    pub(crate) observed: BTreeMap<String, u64>,
    pub(crate) processed: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Backfill,
    Retiring,
    Aborting,
    Complete,
    Aborted,
}

pub(crate) fn job_key(schema: &str) -> String {
    format!("job:{schema}")
}

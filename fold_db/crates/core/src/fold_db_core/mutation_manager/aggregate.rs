//! Aggregate member materialization, guard invalidation, and repair finalize.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::schema::types::aggregate::{
    AGGREGATE_GUARD_TOKEN_FIELD, AGGREGATE_MEMBER_RESERVED_FIELDS, AGGREGATE_VALID_FIELD,
};
use crate::schema::types::aggregate_state::{
    aggregate_guard_key, aggregate_target_guard_key, apply_member_replacement, metric_fingerprint,
    AggregateMemberRecord,
};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{
    AggregateApply, AggregateFinalize, AggregateGuard, AggregateRepair, AggregateSet,
    AggregateWinner, FieldValueType, KeyValue, Mutation, MutationType,
};
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;
#[cfg(not(feature = "cloud-sync"))]
use super::receipt::CloudMutationReceipt;
use super::receipt::{CloudCapturePolicy, ResidentCommitReceipt, ResidentDurability};
use super::write::WriteOrigin;
use super::MutationManager;

mod finalize;
mod guard;
mod repair;

/// Result of a bounded member-partition repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRepairReceipt {
    pub receipt: ResidentCommitReceipt,
    /// Core-minted capability that the separate finalize call must present.
    pub repair_token: String,
}

/// Local proof that a complete bounded repair produced the metric generation
/// that finalize is about to certify. The token text alone is not authority:
/// a source writer controls its mutation UUID and could otherwise forge the
/// historical `repair:` spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateRepairGrant {
    version: u8,
    repair_token: String,
    association_fingerprint: String,
    enrollment_winner: AggregateWinner,
    totals_fingerprint: String,
    totals: BTreeMap<String, i64>,
}

mod apply_source;
mod keys;
mod validate;
mod write_set;

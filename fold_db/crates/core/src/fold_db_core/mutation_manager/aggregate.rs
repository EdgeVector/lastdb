//! Aggregate member materialization, guard invalidation, and repair finalize.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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

use super::plan::aggregate::{self as aggregate_plan, AggregateRepairGrant};
use super::plan::cas::CurrentRowFields;
use super::receipt::{ResidentCommitReceipt, ResidentDurability};
use super::write::WriteOrigin;
use super::MutationManager;

mod enrolled;
mod finalize;
mod guard;
mod invalidation_rules;
mod repair;
mod rules;
mod source_guards;

/// Result of a bounded member-partition repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRepairReceipt {
    pub receipt: ResidentCommitReceipt,
    /// Core-minted capability that the separate finalize call must present.
    pub repair_token: String,
}

mod apply_source;
mod keys;
mod validate;
mod write_set;

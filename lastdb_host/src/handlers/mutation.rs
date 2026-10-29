//! Mutation, aggregate and batch-mutation handlers.
//!
//! `request` shows the single-request steps, and `plan` holds pure request
//! rules. The write helpers here use the existing host interface directly.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::access::AccessContext;
use fold_db::constants::MUTATION_BACKGROUND_TASK_TIMEOUT;
use fold_db::fold_db_core::mutation_manager::{
    MutationManager, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::operations::{MutationConvergence, MutationDurability, Operation};
use fold_db::schema::types::{AggregateSet, KeyValue, Mutation, MutationType, SchemaError};
use serde_json::Value;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

use self::fields::require_durable_aggregate_receipt;
use self::plan::MutationPlan;

mod plan;
mod request;
pub use request::execute_mutation;

mod fields;
pub use fields::{mutation_unknown_fields, mutation_writable_field_names, unknown_fields_body};
mod aggregate;
pub use aggregate::{execute_aggregate_finalize, execute_aggregate_repair};
mod batch;
pub use batch::execute_mutations_batch;

/// Keep a typed status. A true 500 keeps the historical prefix.
fn prefix_core_failure(error: impl Into<HostError>, failed: &str) -> HostError {
    let mapped = error.into();
    if mapped.status == 500 {
        HostError::internal(format!("{failed}: {}", mapped.message))
    } else {
        mapped
    }
}

// ---------------------------------------------------------------------------
// Mutation (POST /api/mutation)
// ---------------------------------------------------------------------------

/// Bundled, already-parsed components of an `Operation::Mutation`. The caller
/// (each socket executor) parses the wire body into this shape; the shared
/// handler owns schema resolution + the write.
pub struct MutationComponents {
    /// Existing per-key metadata supplied by an owner history restore.
    pub metadata: Option<HashMap<String, String>>,
    /// Schema the mutation writes.
    pub schema: String,
    /// Field values for this key, keyed by field name.
    pub fields_and_values: HashMap<String, Value>,
    /// Record key the mutation addresses.
    pub key_value: KeyValue,
    /// Create, update, or delete, as chosen by the caller.
    pub mutation_type: MutationType,
    /// Optional source file name recorded with the write.
    pub source_file_name: Option<String>,
    /// Optional compare-and-swap expectation for the existing record.
    pub expected: Option<fold_db::schema::types::cas::CasExpectation>,
    /// How concurrent writers of this key are resolved.
    pub convergence: MutationConvergence,
    /// Whether the caller waits for the durable commit.
    pub durability: MutationDurability,
    /// When `Some(true)` on an update, a missing record is an error instead of an insert.
    pub must_exist: Option<bool>,
    /// Delete all live rows under `key_value.hash` with this range prefix.
    pub key_range_prefix: Option<String>,
    /// Concrete replacement for this source row's aggregate member. The
    /// single-mutation path uses the atomic aggregate commit primitive.
    pub aggregate_set: Option<AggregateSet>,
}

impl MutationComponents {
    /// Allocate one mutation identity and move the owned payload and options.
    /// Callers resolve the schema and validate request/field rules first.
    fn into_mutation(self, canonical: String, public_key: String) -> Mutation {
        let mut mutation = configure_mutation(
            Mutation::new(
                canonical,
                self.fields_and_values,
                self.key_value,
                public_key,
                self.mutation_type,
            ),
            self.source_file_name,
            self.durability,
            self.expected,
            self.must_exist,
            self.aggregate_set,
        );
        mutation.metadata = self.metadata;
        mutation
    }

    /// Move parsed fields into a request without host or storage access.
    /// An operation's convergence overrides the caller's default. The caller
    /// supplies the separately decoded aggregate sidecar, when supported.
    #[must_use]
    pub fn from_operation(
        operation: Operation,
        default_convergence: Option<MutationConvergence>,
        aggregate_set: Option<AggregateSet>,
    ) -> Self {
        let Operation::Mutation {
            schema,
            fields_and_values,
            key_value,
            mutation_type,
            source_file_name,
            expected,
            convergence,
            durability,
            must_exist,
            key_range_prefix,
        } = operation;

        Self {
            metadata: None,
            schema,
            fields_and_values,
            key_value,
            mutation_type,
            source_file_name,
            expected,
            convergence: convergence.or(default_convergence).unwrap_or_default(),
            durability: durability.unwrap_or_default(),
            must_exist,
            key_range_prefix,
            aggregate_set,
        }
    }
}

/// Per-class operation counts as the route renders them.
pub(super) fn operations_json(operations: &ResidentCommitOperations) -> Value {
    serde_json::json!({
        "created": operations.created,
        "updated": operations.updated,
        "deleted": operations.deleted,
        "no_op": operations.no_op,
        "total": operations.total(),
    })
}

/// Resident-commit stage clocks, in microseconds.
///
/// These ride the response rather than `status.request_ops` phases: the phase
/// set is a partition of the request's wall clock and these spans overlap
/// buckets that already exist, so publishing them there would corrupt every
/// request's `unattributed` remainder. See
/// `fold_db::fold_db_core::mutation_manager::receipt`.
pub(super) fn stages_json(stages: &ResidentCommitStages) -> Value {
    serde_json::json!({
        "prepare": stages.prepare_us,
        "gate_wait": stages.gate_wait_us,
        "publish": stages.publish_us,
        "ack": stages.ack_us,
    })
}

/// The commit fields shared by a mutation, a batch, a finalize, and a repair.
pub(super) fn commit_core_json(receipt: &ResidentCommitReceipt) -> Value {
    serde_json::json!({
        "revision": receipt.revision,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
        "touched_group_ids": receipt.touched_group_ids,
    })
}

/// Insert every key of `extra` into `base`. A repeated key keeps the `extra` value.
pub(super) fn extend_json(base: Value, extra: Value) -> Value {
    let Value::Object(mut base_map) = base else {
        return extra;
    };
    let Value::Object(extra_map) = extra else {
        return Value::Object(base_map);
    };
    for (key, value) in extra_map {
        base_map.insert(key, value);
    }
    Value::Object(base_map)
}

pub(super) async fn wait_for_requested_convergence<H: HostNode>(
    host: &H,
    convergence: MutationConvergence,
) -> bool {
    if !convergence.waits() {
        return false;
    }

    let index_wait_started = std::time::Instant::now();
    let background_tasks_drained = host
        .wait_for_background_tasks(MUTATION_BACKGROUND_TASK_TIMEOUT)
        .await;
    request_phases::add_phase(RequestPhase::IndexWait, index_wait_started.elapsed());
    background_tasks_drained
}

/// Build the `{ mutation_id, success, … }` payload of a committed mutation.
fn mutation_response(
    mutation_id: &str,
    background_tasks_drained: bool,
    receipt: &ResidentCommitReceipt,
    payload_bytes: usize,
) -> Value {
    // The fixed-size logical accounting of the value this write just
    // attributed: the same payload-bytes measure QoS lane admission
    // already computed, returned inline so a caller (or the attribution
    // proof) has the write's size before it does a second read.
    extend_json(
        commit_core_json(receipt),
        serde_json::json!({
            "mutation_id": mutation_id,
            "success": true,
            "background_tasks_drained": background_tasks_drained,
            "convergence_pending": !background_tasks_drained,
            "size": payload_bytes
        }),
    )
}

/// Response for a prefix delete that matched no rows.
fn empty_delete_response() -> Value {
    extend_json(
        commit_core_json(&ResidentCommitReceipt::empty()),
        serde_json::json!({
            "mutation_id": "",
            "success": true,
            "background_tasks_drained": true,
            "convergence_pending": false,
            "size": 0
        }),
    )
}

/// Move the payload for one selected key. Multi-key prefix deletes retain
/// their independent payloads and the existing per-key identity order.
fn build_mutations<H: HostNode>(
    host: &H,
    mut key_values: Vec<KeyValue>,
    mut plan: MutationPlan,
) -> Vec<Mutation> {
    if key_values.len() == 1 {
        plan.components.key_value = key_values.pop().expect("one selected key");
        return vec![plan
            .components
            .into_mutation(plan.canonical, host.public_key())];
    }
    let components = &plan.components;
    key_values
        .into_iter()
        .map(|key_value| {
            configure_mutation(
                Mutation::new(
                    plan.canonical.clone(),
                    components.fields_and_values.clone(),
                    key_value,
                    host.public_key(),
                    components.mutation_type,
                ),
                components.source_file_name.clone(),
                components.durability,
                components.expected.clone(),
                components.must_exist,
                components.aggregate_set.clone(),
            )
        })
        .collect()
}

/// Set the caller's options without copying an owned mutation payload.
fn configure_mutation(
    mut mutation: Mutation,
    source_file_name: Option<String>,
    durability: MutationDurability,
    expected: Option<fold_db::schema::types::cas::CasExpectation>,
    must_exist: Option<bool>,
    aggregate_set: Option<AggregateSet>,
) -> Mutation {
    mutation.source_file_name = source_file_name;
    if durability.waits_for_persist() {
        mutation.synchronous = Some(true);
    }
    mutation.expected = expected;
    mutation.must_exist = must_exist;
    mutation.aggregate_set = aggregate_set;
    mutation
}

/// Build, admit, and commit the selected mutations under the caller's access
/// context. Return the receipt and whether it came from the aggregate path.
async fn commit_mutation<H: HostNode>(
    host: &H,
    plan: MutationPlan,
    key_values: Vec<KeyValue>,
    ctx: &AccessContext,
    payload_bytes: usize,
) -> Result<(ResidentCommitReceipt, bool), HostError> {
    let mutations = build_mutations(host, key_values, plan);
    let admission_started = std::time::Instant::now();
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;
    let manager = host.fold_db().mutation_manager();
    let aggregate_write = mutations.len() == 1 && mutations[0].aggregate_set.is_some();
    let receipt = start_core_commit(manager, mutations, ctx, aggregate_write)
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation execution failed"))?;

    let receipt = if aggregate_write {
        require_durable_aggregate_receipt(receipt, "Aggregate set")?
    } else {
        receipt
    };
    Ok((receipt, aggregate_write))
}

type CoreCommitFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResidentCommitReceipt, SchemaError>> + Send + 'a>>;

/// Construct the large write future in a separate frame, as the socket route
/// dispatcher does. That frame returns before the request polls the write,
/// so debug builds do not keep its construction stack through the whole commit.
#[inline(never)]
fn start_core_commit<'a>(
    manager: &'a MutationManager,
    mutations: Vec<Mutation>,
    ctx: &'a AccessContext,
    aggregate_write: bool,
) -> CoreCommitFuture<'a> {
    if aggregate_write {
        Box::pin(
            manager.write_aggregate_set_with_access_receipt(
                mutations
                    .into_iter()
                    .next()
                    .expect("one aggregate mutation"),
                ctx,
            ),
        )
    } else {
        Box::pin(manager.write_mutations_with_access_receipt(mutations, ctx))
    }
}

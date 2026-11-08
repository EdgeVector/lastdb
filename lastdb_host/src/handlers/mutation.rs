//! Mutation and batch-mutation handlers.
//!
//! `request` shows the single-request steps. The write helpers here use the
//! existing host interface directly.

use fold_db::schema::types::MutationIds;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::constants::MUTATION_BACKGROUND_TASK_TIMEOUT;
use fold_db::fold_db_core::mutation_manager::{
    MutationManager, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::operations::{MutationConvergence, MutationDurability, Operation};
use fold_db::schema::types::{FileWrite, KeyValue, Mutation, MutationType, SchemaError};
use serde_json::Value;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

mod request;
pub use request::execute_mutation;

mod fields;
pub use fields::{mutation_unknown_fields, mutation_writable_field_names, unknown_fields_body};
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
    /// Schema that owns the record.
    pub schema: String,
    /// Complete replacement payload; omitted fields disappear.
    pub record_fields: HashMap<String, Value>,
    /// Explicit primary address, independent of payload fields.
    pub key_value: KeyValue,
    /// Every declared alternate name and its explicit address or permitted null.
    pub alternates: std::collections::BTreeMap<String, Option<KeyValue>>,
    /// Create, replace, delete, or purge.
    pub mutation_type: MutationType,
    /// Complete replacement of the optional source file name.
    pub source_file_name: Option<String>,
    /// Complete replacement of record metadata; absence clears it.
    pub metadata: Option<HashMap<String, String>>,
    /// Complete selected file marker; changes require an opaque file operation.
    pub file: Option<fold_db::record_value::FileMarker>,
    /// Required whole-record revision or expected-absent creation condition.
    pub expected: fold_db::schema::types::cas::CasExpectation,
    /// Whether to wait for post-commit background work.
    pub convergence: MutationConvergence,
    /// Requested local persistence policy; native commits remain durable.
    pub durability: MutationDurability,
    /// Only a file-blob executor can issue this opaque operation.
    pub file_write: Option<FileWrite>,
    /// Only a file executor sets the inline file; `None` keeps the current one.
    pub inline_write: Option<fold_db::record_value::InlineWrite>,
    /// The definition the writer declared, when the request named one.
    pub definition_id: Option<String>,
}

impl MutationComponents {
    fn into_mutation(self, canonical: String, public_key: String) -> Mutation {
        let target = fold_db::record_replacement::RecordTarget {
            schema: canonical,
            key: self.key_value,
            alternates: self.alternates,
        };
        let mut mutation = Mutation::new(
            uuid::Uuid::new_v4().to_string(),
            target,
            self.record_fields,
            public_key,
            self.mutation_type,
            self.expected,
        );
        mutation.source_file_name = self.source_file_name;
        mutation.metadata = self.metadata;
        mutation.file = self.file;
        mutation.file_write = self.file_write;
        mutation.inline_write = self.inline_write;
        mutation.definition_id = self.definition_id;
        mutation.synchronous = Some(self.durability.waits_for_persist());
        mutation
    }

    /// Move a decoded native request into its host components.
    #[must_use]
    pub fn from_operation(
        operation: Operation,
        default_convergence: Option<MutationConvergence>,
    ) -> Self {
        let Operation::Mutation {
            schema,
            record_fields,
            key_value,
            alternates,
            mutation_type,
            source_file_name,
            metadata,
            file,
            expected,
            convergence,
            durability,
            definition_id,
        } = operation;
        Self {
            schema,
            record_fields,
            key_value,
            alternates,
            mutation_type,
            source_file_name,
            metadata,
            file,
            expected,
            convergence: convergence.or(default_convergence).unwrap_or_default(),
            durability: durability.unwrap_or_default(),
            file_write: None,
            inline_write: None,
            definition_id,
        }
    }
}

/// Per-class operation counts as the route renders them.
pub(super) fn operations_json(operations: &ResidentCommitOperations) -> Value {
    serde_json::json!({
        "created": operations.created,
        "updated": operations.updated,
        "deleted": operations.deleted,
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
        "ack": stages.ack_us,
    })
}

/// The commit fields shared by a mutation and a batch.
pub(super) fn commit_core_json(receipt: &ResidentCommitReceipt) -> Value {
    serde_json::json!({
        "revision": receipt.revision,
        "record_revisions": receipt.record_revisions,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
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
            "record_revision": receipt.record_revisions.get(mutation_id),
            "success": true,
            "background_tasks_drained": background_tasks_drained,
            "convergence_pending": !background_tasks_drained,
            "size": payload_bytes
        }),
    )
}

/// Build, admit, and commit the selected mutations.
async fn commit_mutation<H: HostNode>(
    host: &H,
    components: MutationComponents,
    canonical: String,
    payload_bytes: usize,
) -> Result<ResidentCommitReceipt, HostError> {
    let mutations = vec![components.into_mutation(canonical, host.public_key())];
    let admission_started = std::time::Instant::now();
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;
    let manager = host.fold_db().mutation_manager();
    start_core_commit(manager, mutations)
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation execution failed"))
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
) -> CoreCommitFuture<'a> {
    Box::pin(manager.write_mutations_receipt(mutations))
}

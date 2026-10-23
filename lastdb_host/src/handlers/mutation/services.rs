//! Host adapter for a single mutation's effects. Core access checks and write
//! admission stay in the existing commit path; the coordinator sees no host.

use std::future::Future;
use std::pin::Pin;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::mutation_manager::{
    CloudCapturePolicy, MutationManager, ResidentCommitReceipt,
};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::operations::MutationConvergence;
use fold_db::schema::types::{AggregateSet, KeyValue, Mutation};
use fold_db::schema::SchemaError;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

use super::batch::wait_for_requested_convergence;
use super::cloud::resolve_delete_prefix_keys;
use super::fields::{
    require_durable_aggregate_receipt, resolve_aggregate_set_schema_names,
    resolve_required_schema_name, validate_mutation_fields,
};
use super::plan::MutationPlan;

/// Only the effects the single-request coordinator needs. No method exposes
/// `FoldDB`, the host, or a general-purpose service lookup.
pub(super) trait MutationServices: Sync {
    fn resolve_schema_names(
        &self,
        requested: &str,
        aggregate_set: Option<AggregateSet>,
    ) -> Result<(String, Option<AggregateSet>), HostError>;
    fn validate_fields(
        &self,
        plan: &MutationPlan,
    ) -> impl Future<Output = Result<(), HostError>> + Send;
    fn resolve_prefix_keys(
        &self,
        canonical: &str,
        hash: String,
        prefix: String,
    ) -> impl Future<Output = Result<Vec<KeyValue>, HostError>> + Send;
    fn commit(
        &self,
        plan: &MutationPlan,
        key_values: Vec<KeyValue>,
        payload_bytes: usize,
    ) -> impl Future<Output = Result<(ResidentCommitReceipt, bool), HostError>> + Send;
    fn wait_for_convergence(
        &self,
        convergence: MutationConvergence,
    ) -> impl Future<Output = bool> + Send;
}

/// Bound once at the handler entry point. All reads and writes retain the
/// caller's access context, and the host owns identity and background tasks.
pub(super) struct HostMutationServices<'a, H> {
    pub host: &'a H,
    pub ctx: &'a AccessContext,
}

impl<H: HostNode> MutationServices for HostMutationServices<'_, H> {
    fn resolve_schema_names(
        &self,
        requested: &str,
        aggregate_set: Option<AggregateSet>,
    ) -> Result<(String, Option<AggregateSet>), HostError> {
        resolve_mutation_schema(self.host, requested, aggregate_set)
    }

    async fn validate_fields(&self, plan: &MutationPlan) -> Result<(), HostError> {
        let validate_started = std::time::Instant::now();
        validate_mutation_fields(
            self.host,
            &plan.components.schema,
            &plan.canonical,
            &plan.components.fields_and_values,
        )
        .await?;
        request_phases::add_phase(RequestPhase::Validate, validate_started.elapsed());
        Ok(())
    }

    async fn resolve_prefix_keys(
        &self,
        canonical: &str,
        hash: String,
        prefix: String,
    ) -> Result<Vec<KeyValue>, HostError> {
        resolve_delete_prefix_keys(self.host, canonical.to_string(), hash, prefix, self.ctx).await
    }

    async fn commit(
        &self,
        plan: &MutationPlan,
        key_values: Vec<KeyValue>,
        payload_bytes: usize,
    ) -> Result<(ResidentCommitReceipt, bool), HostError> {
        let mutations = build_mutations(self.host, key_values, plan);
        write_mutation_receipt(
            self.host,
            mutations,
            self.ctx,
            plan.cloud_policy,
            payload_bytes,
        )
        .await
    }

    async fn wait_for_convergence(&self, convergence: MutationConvergence) -> bool {
        wait_for_requested_convergence(self.host, convergence).await
    }
}

/// Resolve the schema name and the aggregate-set schema names, timed as the
/// schema-resolve phase.
///
/// Phase timers are always-on: each is one pair of monotonic clock reads
/// (sub-microsecond) around work that is µs-to-ms scale, reported through
/// the task-local accumulator (`fold_db::request_phases`) so no signature
/// here changes. Outside an instrumented socket request the adds no-op.
fn resolve_mutation_schema<H: HostNode>(
    host: &H,
    schema: &str,
    aggregate_set: Option<AggregateSet>,
) -> Result<(String, Option<AggregateSet>), HostError> {
    let resolve_started = std::time::Instant::now();
    let resolved = (|| {
        let canonical = resolve_required_schema_name(host, schema)?;
        let aggregate_set = aggregate_set
            .map(|aggregate| resolve_aggregate_set_schema_names(host, aggregate))
            .transpose()?;
        Ok::<_, HostError>((canonical, aggregate_set))
    })();
    request_phases::add_phase(RequestPhase::SchemaResolve, resolve_started.elapsed());
    resolved
}

/// Build one mutation identity per selected key in the host adapter.
/// UUID allocation stays outside the pure request policy.
fn build_mutations<H: HostNode>(
    host: &H,
    key_values: Vec<KeyValue>,
    plan: &MutationPlan,
) -> Vec<Mutation> {
    let components = &plan.components;
    key_values
        .into_iter()
        .map(|key_value| {
            let mut mutation = Mutation::new(
                plan.canonical.clone(),
                components.fields_and_values.clone(),
                key_value,
                host.public_key(),
                components.mutation_type,
            );
            mutation
                .source_file_name
                .clone_from(&components.source_file_name);
            if components.durability.waits_for_persist() {
                mutation.synchronous = Some(true);
            }
            let mutation = match components.expected.clone() {
                Some(expectation) => mutation.with_expected(expectation),
                None => mutation,
            };
            let mutation = match components.must_exist {
                Some(flag) => mutation.with_must_exist(flag),
                None => mutation,
            };
            match components.aggregate_set.clone() {
                Some(aggregate) => mutation.with_aggregate_set(aggregate),
                None => mutation,
            }
        })
        .collect()
}

/// Admit and write the mutations, mapping a core failure to a [`HostError`].
/// Returns the receipt and whether it came from the aggregate commit path.
async fn write_mutation_receipt<H: HostNode>(
    host: &H,
    mutations: Vec<Mutation>,
    ctx: &AccessContext,
    cloud_policy: CloudCapturePolicy,
    payload_bytes: usize,
) -> Result<(ResidentCommitReceipt, bool), HostError> {
    // The QoS admission wait was previously invisible — charged as handler
    // work in `duration_ms` with nothing to distinguish "queued behind the
    // write governor" from "the write was slow". Time it as its own bucket.
    let admission_started = std::time::Instant::now();
    let permit = host
        .acquire_op_permit(Lane::for_write_bytes(payload_bytes))
        .await;
    request_phases::add_phase(RequestPhase::AdmissionWait, admission_started.elapsed());
    let _permit = permit?;
    let manager = host.fold_db().mutation_manager();
    let aggregate_write = mutations.len() == 1 && mutations[0].aggregate_set.is_some();
    let receipt = start_core_commit(manager, mutations, ctx, cloud_policy, aggregate_write)
        .await
        .map_err(|e| {
            // Prefer a typed mapping when available; keep the historical
            // "Mutation execution failed:" prefix only for true 500s so
            // owner diagnostics stay readable.
            let mapped = HostError::from(e);
            if mapped.status == 500 {
                HostError::internal(format!("Mutation execution failed: {}", mapped.message))
            } else {
                mapped
            }
        })?;

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
/// dispatcher does. That frame returns before the coordinator polls the write,
/// so debug builds do not keep its construction stack through the whole commit.
#[inline(never)]
fn start_core_commit<'a>(
    manager: &'a MutationManager,
    mutations: Vec<Mutation>,
    ctx: &'a AccessContext,
    cloud_policy: CloudCapturePolicy,
    aggregate_write: bool,
) -> CoreCommitFuture<'a> {
    if aggregate_write {
        Box::pin(
            manager.write_aggregate_set_with_access_receipt_cloud(
                mutations
                    .into_iter()
                    .next()
                    .expect("one aggregate mutation"),
                ctx,
                cloud_policy,
            ),
        )
    } else {
        Box::pin(manager.write_mutations_with_access_receipt_cloud(mutations, ctx, cloud_policy))
    }
}

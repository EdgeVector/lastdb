//! Shared lock prelude for aggregate finalize and repair.

use std::future::Future;
use std::pin::Pin;

use super::super::plan::aggregate::association_matches;
use super::*;

type EnrolledFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, SchemaError>> + Send + 'a>>;

pub(super) fn pin_enrolled<'a, T>(
    future: impl Future<Output = Result<T, SchemaError>> + Send + 'a,
) -> EnrolledFuture<'a, T> {
    Box::pin(future)
}

/// Schema names and keys that a locked finalize or repair presents.
pub(super) trait AggregateWriteRequest: Send {
    fn source_schema_name(&self) -> &str;
    fn source_partition_key_value(&self) -> &KeyValue;
    fn target_schema_name(&self) -> &str;
    fn target_key_value(&self) -> &KeyValue;
    fn member_schema_name(&self) -> &str;
}

impl AggregateWriteRequest for AggregateFinalize {
    fn source_schema_name(&self) -> &str {
        &self.source_schema_name
    }

    fn source_partition_key_value(&self) -> &KeyValue {
        &self.source_partition_key_value
    }

    fn target_schema_name(&self) -> &str {
        &self.target_schema_name
    }

    fn target_key_value(&self) -> &KeyValue {
        &self.target_key_value
    }

    fn member_schema_name(&self) -> &str {
        &self.member_schema_name
    }
}

impl AggregateWriteRequest for AggregateRepair {
    fn source_schema_name(&self) -> &str {
        &self.source_schema_name
    }

    fn source_partition_key_value(&self) -> &KeyValue {
        &self.source_partition_key_value
    }

    fn target_schema_name(&self) -> &str {
        &self.target_schema_name
    }

    fn target_key_value(&self) -> &KeyValue {
        &self.target_key_value
    }

    fn member_schema_name(&self) -> &str {
        &self.member_schema_name
    }
}

impl MutationManager {
    /// Resolve the association, hold the source guard lock and the target lock,
    /// then run `body`. `hash_checked` is the panic text for a missing source
    /// hash. `association_mismatch` is the refusal when the enrolled guard
    /// does not match the request.
    pub(super) async fn with_enrolled_aggregate_target<R, T, F>(
        &self,
        request: R,
        access_context: &crate::access::AccessContext,
        hash_checked: &'static str,
        association_mismatch: &'static str,
        body: F,
    ) -> Result<T, SchemaError>
    where
        R: AggregateWriteRequest,
        F: for<'a> FnOnce(
                R,
                &'a Self,
                KeyValue,
                &'a AggregateGuard,
                Option<&'a str>,
            ) -> EnrolledFuture<'a, T>
            + Send,
    {
        let target_key = self.resolve_aggregate_target_key(
            request.target_schema_name(),
            request.target_key_value(),
        )?;
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                request.source_schema_name(),
                request.target_schema_name(),
                request.member_schema_name(),
                access_context,
            )
            .await?;
        let source_hash = request
            .source_partition_key_value()
            .hash
            .as_deref()
            .expect(hash_checked);
        let guard_key = aggregate_guard_key(
            storage_prefix.as_deref(),
            request.source_schema_name(),
            source_hash,
        );
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .write_owned()
            .await;
        self.with_aggregate_guards(_guard_lock, async move {
            let source_hash = request
                .source_partition_key_value()
                .hash
                .as_deref()
                .expect(hash_checked);
            let guard = self
                .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
                .await?
                .ok_or_else(|| {
                    SchemaError::InvalidData("aggregate source partition is not enrolled".into())
                })?;
            guard.validate().map_err(SchemaError::InvalidData)?;
            if !association_matches(
                &guard,
                request.source_schema_name(),
                source_hash,
                request.target_schema_name(),
                &target_key,
                request.member_schema_name(),
            ) {
                return Err(SchemaError::InvalidData(association_mismatch.into()));
            }
            self.validate_enrolled_aggregate_schemas(&guard)?;

            let target_lock_key = Self::aggregate_target_lock_key(
                storage_prefix.as_deref(),
                request.target_schema_name(),
                &target_key,
            );
            let _target_lock = self
                .aggregate_named_lock(target_lock_key)
                .write_owned()
                .await;
            self.with_aggregate_guards(_target_lock, async move {
                body(request, self, target_key, &guard, storage_prefix.as_deref()).await
            })
            .await
        })
        .await
    }
}

//! Numeric operation accounting; no stored key or value enters these events.

use super::*;
use std::collections::BTreeSet;

struct Counts {
    current: usize,
    incoming: usize,
    projected: usize,
    unique: usize,
}

fn counts(data: &PendingData, operations: &[HomeMutation]) -> Counts {
    let mut unique: BTreeSet<_> = data
        .latest
        .keys()
        .map(|(collection, key)| (collection.as_str(), key.as_str()))
        .collect();
    unique.extend(
        operations
            .iter()
            .map(|operation| (operation.collection.as_str(), operation.key.as_str())),
    );
    Counts {
        current: data.operations.len(),
        incoming: operations.len(),
        projected: data.operations.len().saturating_add(operations.len()),
        unique: unique.len(),
    }
}

pub(super) fn refusal(
    data: &PendingData,
    operations: &[HomeMutation],
    limit: usize,
) -> StorageError {
    let counts = counts(data, operations);
    StorageError::MutationLimit {
        detail: format!(
            "complete native mutation exceeds {limit} storage operations: current={}, incoming={}, projected={}, projected_unique={}",
            counts.current, counts.incoming, counts.projected, counts.unique,
        ),
    }
}

impl PendingBatch {
    /// Retain the typed preparation failure across helpers that add context by
    /// converting their inner storage errors to schema error strings.
    pub(super) fn remember_preparation_limit(&self, error: StorageError) -> StorageError {
        if let StorageError::MutationLimit { detail } = &error {
            self.preparation_limit
                .lock()
                .expect("native complete mutation limit")
                .get_or_insert_with(|| detail.clone());
        }
        error
    }

    pub(super) fn preparation_limit(&self) -> Option<StorageError> {
        self.preparation_limit
            .lock()
            .expect("native complete mutation limit")
            .as_ref()
            .map(|detail| StorageError::MutationLimit {
                detail: detail.clone(),
            })
    }
}

pub(super) fn record_stage(data: &PendingData, operations: &[HomeMutation], limit: usize) {
    if !tracing::enabled!(target: "lastdb::native_write_budget", tracing::Level::DEBUG) {
        return;
    }
    let counts = counts(data, operations);
    tracing::debug!(
        target: "lastdb::native_write_budget",
        current_operations = counts.current,
        incoming_operations = counts.incoming,
        projected_operations = counts.projected,
        projected_unique_operations = counts.unique,
        operation_limit = limit,
        "native preparation cumulative stored operations"
    );
}

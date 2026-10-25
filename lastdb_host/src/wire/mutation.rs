//! Mutation body rules. No host, clocks, task access, or logging.

use fold_db::schema::types::operations::{MutationConvergence, Operation};
use fold_db::schema::types::AggregateSet;
use lastdb_uds::uds_http::UdsResponse;
use serde::Deserialize;
use serde_json::Value;

use crate::envelope::content_free;
use crate::handlers::MutationComponents;
use crate::reject::{Reject, RejectKind};

use super::{first_missing_key, MUTATION_REQUIRED_KEYS};

/// Decode a single mutation before the route records phases or logs a refusal.
/// Rejection order is body shape, required keys, aggregate sidecar, then the
/// strict operation grammar. No caller value enters a rejection (I4).
///
/// # Errors
/// Returns the existing typed rejection for an invalid mutation body.
pub fn parse_mutation_body(body: &[u8]) -> Result<MutationComponents, Reject> {
    let Ok(Value::Object(mut obj)) = serde_json::from_slice::<Value>(body) else {
        return Err(Reject::malformed_body());
    };
    if let Some(key) = first_missing_key(&obj, MUTATION_REQUIRED_KEYS) {
        return Err(Reject::missing(key));
    }
    // `aggregate_set` is a sidecar, not an Operation field. An explicit null
    // remains invalid; only an absent sidecar means no aggregate request.
    let aggregate_set = match obj.remove("aggregate_set") {
        Some(value) => Some(
            serde_json::from_value::<AggregateSet>(value)
                .map_err(|_| Reject::new(RejectKind::InvalidValue))?,
        ),
        None => None,
    };
    let operation = serde_json::from_value::<Operation>(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None))?;
    Ok(MutationComponents::from_operation(
        operation,
        None,
        aggregate_set,
    ))
}

#[derive(Deserialize)]
struct BatchMutationsRequest {
    mutations: Vec<Operation>,
    #[serde(default)]
    convergence: Option<MutationConvergence>,
}

/// Decode either batch mutation body shape without host or task access.
/// Array items use operation defaults; object items can inherit convergence.
/// Operation convergence still overrides the batch default.
///
/// # Errors
/// Malformed JSON retains the bare `400 Bad Request`. Typed grammar failures
/// retain their content-free rejection kind and key, without caller values.
pub fn parse_batch_mutation_body(body: &[u8]) -> Result<Vec<MutationComponents>, UdsResponse> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Err(content_free(400, "Bad Request"));
    };

    // A strict `Operation` refusing a key it does not declare is the
    // client↔node version-skew signal (`unknown_key`), not a malformed body:
    // brain 0.8.0 sent per-mutation `durability` to a primary that predated
    // the key and got the same bare `Bad Request` as a typo (2026-09-03).
    let (operations, default_convergence) = match value {
        Value::Array(items) => {
            let mut operations = Vec::with_capacity(items.len());
            for item in items {
                match serde_json::from_value::<Operation>(item) {
                    Ok(operation) => operations.push(operation),
                    Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
                }
            }
            (operations, None)
        }
        other => match serde_json::from_value::<BatchMutationsRequest>(other) {
            Ok(request) => (request.mutations, request.convergence),
            Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
        },
    };

    Ok(operations
        .into_iter()
        .map(|operation| MutationComponents::from_operation(operation, default_convergence, None))
        .collect())
}

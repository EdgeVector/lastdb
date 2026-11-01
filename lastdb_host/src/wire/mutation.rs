//! Mutation body rules. No host, clocks, task access, or logging.

use fold_db::schema::types::operations::Operation;
use serde_json::Value;

use crate::handlers::MutationComponents;
use crate::reject::Reject;

use super::{first_missing_key, MUTATION_REQUIRED_KEYS};

/// Decode a single mutation before the route records phases or logs a refusal.
/// Rejection order is body shape, required keys, then the strict operation
/// grammar. No caller value enters a rejection (I4).
///
/// # Errors
/// Returns the existing typed rejection for an invalid mutation body.
pub fn parse_mutation_body(body: &[u8]) -> Result<MutationComponents, Reject> {
    let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(body) else {
        return Err(Reject::malformed_body());
    };
    if let Some(key) = first_missing_key(&obj, MUTATION_REQUIRED_KEYS) {
        return Err(Reject::missing(key));
    }
    let operation = serde_json::from_value::<Operation>(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None))?;
    Ok(MutationComponents::from_operation(operation, None))
}

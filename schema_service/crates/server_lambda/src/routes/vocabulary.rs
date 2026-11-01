//! Public standard vocabulary lookup. No separate vocabulary mutation route.
use crate::http::json_response;
use lambda_http::{Body, Error, Response};
use schema_service_server_shared::state::SchemaServiceState;

pub(crate) async fn get(state: &SchemaServiceState) -> Result<Response<Body>, Error> {
    match state.canonical_vocabulary().await {
        Ok(value) => json_response(200, &value),
        Err(error) => json_response(
            500,
            &schema_service_core::error_only_body(&error.to_string()),
        ),
    }
}

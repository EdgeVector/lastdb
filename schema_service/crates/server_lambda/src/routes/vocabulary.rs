//! Public standard vocabulary lookup. No separate vocabulary mutation route.
use super::*;

pub(crate) async fn get(state: &SchemaServiceState) -> Result<Response<Body>, Error> {
    match state.canonical_vocabulary().await {
        Ok(value) => json_response(200, &value),
        Err(error) => json_response(500, &json!({"error": error.to_string()})),
    }
}

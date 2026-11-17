//! `GET /v1/schemas/<definition_id>`: one stored schema definition.
//!
//! The id is 64 lowercase hex. The response is the stored object, with a
//! one-year immutable cache. No credentials are required. A read is not logged.

use std::time::Duration;

use lambda_http::{Body, Error, Response};
use serde_json::json;

use super::storage::valid_definition_id;
use super::Catalog;

const DEFINITION_TIMEOUT: Duration = Duration::from_secs(2);
const DEFINITION_CACHE: &str = "public, max-age=31536000, immutable";

/// Suffix of `GET /v1/schemas/<id>`. `GET /v1/schemas` has no suffix.
pub(crate) fn definition_id(path: &str) -> Option<&str> {
    path.strip_prefix("/v1/schemas/")
        .filter(|id| !id.is_empty())
}

pub(crate) async fn read_definition(
    catalog: Option<&Catalog>,
    definition_id: &str,
) -> Result<Response<Body>, Error> {
    if !valid_definition_id(definition_id) {
        return crate::json_response(400, &json!({ "error": "invalid definition id" }));
    }
    let Some(catalog) = catalog else {
        tracing::warn!(
            reason = "catalog not configured",
            "schema_catalog_definition_unavailable"
        );
        return crate::json_response(503, &json!({ "error": "schema catalog unavailable" }));
    };
    match catalog.load_definition(definition_id).await {
        Ok(Some(body)) => crate::json_bytes_response(200, body, DEFINITION_CACHE),
        Ok(None) => crate::json_response(404, &json!({ "error": "schema definition not found" })),
        Err(reason) => {
            tracing::warn!(%reason, "schema_catalog_definition_unavailable");
            crate::json_response(503, &json!({ "error": "schema catalog unavailable" }))
        }
    }
}

impl Catalog {
    async fn load_definition(&self, definition_id: &str) -> Result<Option<Vec<u8>>, String> {
        self.store
            .get_definition(&self.client, definition_id, DEFINITION_TIMEOUT)
            .await
    }
}

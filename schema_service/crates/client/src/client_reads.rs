//! Read calls: schema lookups and resolve.

use super::*;

impl SchemaServiceClient {
    /// Send a GET request and deserialize the JSON response.
    ///
    /// Retries up to 3 times on transient failures (connect errors, timeouts,
    /// 5xx responses). Does NOT retry on 4xx responses or deserialization
    /// failures.
    pub(super) async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        context: &str,
    ) -> FoldDbResult<T> {
        with_retries(|| async {
            let response = observability::propagation::inject_w3c(self.client.get(url))
                .send()
                .await
                .map_err(|e| {
                    let retryable = reqwest_error_is_retryable(&e);
                    let wrapped = FoldDbError::Config(format!("Failed to fetch {context}: {e}"));
                    RetryError::classify(retryable, wrapped)
                })?;
            let status = response.status();
            if !status.is_success() {
                let wrapped = FoldDbError::Config(format!(
                    "Schema service returned error for {context}: {status}"
                ));
                return Err(RetryError::classify(status_is_retryable(status), wrapped));
            }
            response.json().await.map_err(|e| {
                RetryError::Permanent(FoldDbError::Config(format!(
                    "Failed to parse {context} response: {e}"
                )))
            })
        })
        .await
    }

    /// Get all available schemas with their full definitions from the schema service.
    ///
    /// Each returned envelope carries a `system: bool` flag —
    /// `true` for infrastructure schemas seeded by the service
    /// (`Fingerprint`, `Edge`, `Identity`, `Persona`, …),
    /// `false` for everything user-proposed. Downstream UIs
    /// (fold_db_node's schema list) use the flag to group or hide
    /// system schemas.
    pub async fn get_available_schemas(&self) -> FoldDbResult<Vec<SchemaEnvelope>> {
        #[derive(Deserialize)]
        struct AvailableSchemasResponse {
            schemas: Vec<SchemaEnvelope>,
        }

        let url = format!("{}/v1/schemas/available", self.base_url);
        let resp: AvailableSchemasResponse = self.get_json(&url, "available schemas").await?;
        Ok(resp.schemas)
    }

    /// Get a specific schema definition from the schema service,
    /// including the `system: bool` classification. See
    /// [`SchemaServiceClient::get_available_schemas`] for what the
    /// flag means.
    pub async fn get_schema(&self, name: &str) -> FoldDbResult<SchemaEnvelope> {
        let url = format!("{}/v1/schema/{}", self.base_url, name);
        self.get_json(&url, &format!("schema '{name}'")).await
    }

    /// Resolve proposed schemas against the service registry without mutating it.
    ///
    /// This is the read-only cache-miss companion to `POST /v1/schemas`: the service
    /// returns reuse/novel/refresh advice but does not create schemas or update
    /// shared discovery state.
    pub async fn resolve_schemas(
        &self,
        client_registry_version: Option<u64>,
        proposals: Vec<SchemaResolveProposal>,
    ) -> FoldDbResult<SchemaResolveResponse> {
        let url = format!("{}/v1/schemas/resolve", self.base_url);
        let request = SchemaResolveRequest {
            client_registry_version,
            proposals,
        };

        with_retries(|| async {
            let response =
                observability::propagation::inject_w3c(self.client.post(&url).json(&request))
                    .send()
                    .await
                    .map_err(|e| {
                        let retryable = reqwest_error_is_retryable(&e);
                        let wrapped =
                            FoldDbError::Config(format!("Failed to resolve schemas at {url}: {e}"));
                        RetryError::classify(retryable, wrapped)
                    })?;

            let status = response.status();
            if !status.is_success() {
                let retryable = status_is_retryable(status);
                let body = response_body_text(response).await;
                let wrapped =
                    FoldDbError::Config(format!("Schema resolve failed (status {status}): {body}"));
                return Err(RetryError::classify(retryable, wrapped));
            }

            response.json::<SchemaResolveResponse>().await.map_err(|e| {
                RetryError::Permanent(FoldDbError::Config(format!(
                    "Failed to parse schema resolve response: {e}"
                )))
            })
        })
        .await
    }
}

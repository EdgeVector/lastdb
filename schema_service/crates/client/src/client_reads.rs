//! Read calls: schema lists, lookups, reuse checks, resolve and app fetches.

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

    /// Extract a schema name from a JSON value that may be a string, `{"name": ...}`, or `{"schema": {"name": ...}}`.
    pub(super) fn extract_schema_name(v: &serde_json::Value) -> Option<String> {
        v.as_str().map(Into::into).or_else(|| {
            let obj = v.as_object()?;
            obj.get("name")
                .and_then(|n| n.as_str())
                .or_else(|| {
                    obj.get("schema")
                        .and_then(|s| s.get("name"))
                        .and_then(|n| n.as_str())
                })
                .map(Into::into)
        })
    }

    /// List all available schemas from the schema service.
    pub async fn list_schemas(&self) -> FoldDbResult<Vec<String>> {
        #[derive(Deserialize)]
        struct SchemasListResponse {
            schemas: Vec<serde_json::Value>,
        }

        let url = format!("{}/v1/schemas", self.base_url);
        let resp: SchemasListResponse = self.get_json(&url, "schemas").await?;
        Ok(resp
            .schemas
            .iter()
            .filter_map(Self::extract_schema_name)
            .collect())
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

    /// Look up a schema by its exact catalog identity.
    ///
    /// This preserves the distinction required by idempotent schema sync:
    /// `Ok(None)` means the catalog explicitly returned 404, while transport,
    /// server, and decoding failures remain errors. Callers must not turn an
    /// ambiguous lookup failure into a registration attempt.
    pub async fn find_schema(&self, name: &str) -> FoldDbResult<Option<SchemaEnvelope>> {
        let url = format!("{}/v1/schema/{}", self.base_url, name);
        with_retries(|| async {
            let response = observability::propagation::inject_w3c(self.client.get(&url))
                .send()
                .await
                .map_err(|e| {
                    let retryable = reqwest_error_is_retryable(&e);
                    let wrapped = FoldDbError::Config(format!(
                        "Failed to look up schema '{name}' from {url}: {e}"
                    ));
                    RetryError::classify(retryable, wrapped)
                })?;
            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                let body = response_body_text(response).await;
                let wrapped = FoldDbError::Config(format!(
                    "Schema lookup from {url} returned {status}: {body}"
                ));
                return Err(RetryError::classify(status_is_retryable(status), wrapped));
            }
            response
                .json::<SchemaEnvelope>()
                .await
                .map(Some)
                .map_err(|e| {
                    RetryError::Permanent(FoldDbError::Config(format!(
                        "Failed to parse schema '{name}' lookup response: {e}"
                    )))
                })
        })
        .await
    }

    /// Fetch the signed compact registry index.
    ///
    /// The client only transports and parses the envelope. Signature, Merkle,
    /// and trust-root verification are owned by fold_db_node, which decides
    /// which signing keys are trusted in its runtime.
    pub async fn get_registry_index(&self) -> FoldDbResult<RegistryIndexEnvelope> {
        let url = format!("{}/v1/registry/index", self.base_url);
        self.get_json(&url, "registry index").await
    }

    /// Batch check whether proposed schemas can reuse existing ones.
    ///
    /// Retries up to 3 times on transient failures.
    pub async fn batch_check_schema_reuse(
        &self,
        entries: &[SchemaLookupEntry],
    ) -> FoldDbResult<BatchSchemaReuseResponse> {
        let url = format!("{}/v1/schemas/batch-check-reuse", self.base_url);
        let request = BatchSchemaReuseRequest {
            schemas: entries.to_vec(),
        };

        with_retries(|| async {
            let response =
                observability::propagation::inject_w3c(self.client.post(&url).json(&request))
                    .send()
                    .await
                    .map_err(|e| {
                        let retryable = reqwest_error_is_retryable(&e);
                        let wrapped = FoldDbError::Config(format!(
                            "Failed to batch check schema reuse at {url}: {e}"
                        ));
                        RetryError::classify(retryable, wrapped)
                    })?;

            let status = response.status();
            if !status.is_success() {
                let retryable = status_is_retryable(status);
                let body = response_body_text(response).await;
                let wrapped = FoldDbError::Config(format!(
                    "Batch schema reuse check failed (status {status}): {body}"
                ));
                return Err(RetryError::classify(retryable, wrapped));
            }

            response
                .json::<BatchSchemaReuseResponse>()
                .await
                .map_err(|e| {
                    RetryError::Permanent(FoldDbError::Config(format!(
                        "Failed to parse batch schema reuse response: {e}"
                    )))
                })
        })
        .await
    }

    /// Resolve proposed schemas against the service registry without mutating it.
    ///
    /// This is the read-only cache-miss companion to `add_schema`: the service
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

    pub async fn fetch_app(&self, app_id: &str) -> FoldDbResult<Option<AppLookup>> {
        let url = format!("{}/v1/apps/{}", self.base_url, app_id);
        with_retries(|| async {
            // trace-egress: propagate (schema_service /v1/apps/{app_id})
            let response = observability::propagation::inject_w3c(self.client.get(&url))
                .send()
                .await
                .map_err(|e| {
                    let retryable = reqwest_error_is_retryable(&e);
                    let wrapped =
                        FoldDbError::Config(format!("Failed to fetch app from {url}: {e}"));
                    RetryError::classify(retryable, wrapped)
                })?;
            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                let body = response_body_text(response).await;
                let wrapped =
                    FoldDbError::Config(format!("App fetch from {url} returned {status}: {body}"));
                return Err(RetryError::classify(status_is_retryable(status), wrapped));
            }
            response.json::<AppLookup>().await.map(Some).map_err(|e| {
                RetryError::Permanent(FoldDbError::Config(format!(
                    "Failed to parse app lookup response: {e}"
                )))
            })
        })
        .await
    }

    pub async fn fetch_apps(&self) -> FoldDbResult<Vec<AppRecord>> {
        let url = format!("{}/v1/apps", self.base_url);
        with_retries(|| async {
            // trace-egress: propagate (schema_service /v1/apps)
            let response = observability::propagation::inject_w3c(self.client.get(&url))
                .send()
                .await
                .map_err(|e| {
                    let retryable = reqwest_error_is_retryable(&e);
                    let wrapped =
                        FoldDbError::Config(format!("Failed to fetch apps from {url}: {e}"));
                    RetryError::classify(retryable, wrapped)
                })?;
            let status = response.status();
            if !status.is_success() {
                let body = response_body_text(response).await;
                let wrapped =
                    FoldDbError::Config(format!("App list from {url} returned {status}: {body}"));
                return Err(RetryError::classify(status_is_retryable(status), wrapped));
            }
            response.json::<Vec<AppRecord>>().await.map_err(|e| {
                RetryError::Permanent(FoldDbError::Config(format!(
                    "Failed to parse app list response: {e}"
                )))
            })
        })
        .await
    }
}

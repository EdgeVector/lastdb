//! Schema add/publish calls of the schema service client.

use super::*;

impl SchemaServiceClient {
    /// Register a schema with an explicit shared-surface envelope.
    ///
    /// Sets `offer_to_shared_discovery = true` and attaches
    /// [`SharedSurfaceMetadata`] so the service can observe/enforce the
    /// shared-only control plane. Used by Mini
    /// `POST /api/apps/shared-surface/publish-attach` after local-first
    /// resolve decides the contract is novel.
    pub async fn add_schema_with_shared_surface(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
        shared_surface: SharedSurfaceMetadata,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
    ) -> FoldDbResult<AddSchemaResponse> {
        match self
            .add_schema_typed_inner(
                schema,
                mutation_mappers,
                AddSchemaOptions {
                    schema_match_source,
                    fallback_reason,
                    offer_to_shared_discovery: true,
                    shared_surface: Some(shared_surface),
                },
            )
            .await?
        {
            AddSchemaOutcome::Accepted(response) => Ok(*response),
            AddSchemaOutcome::Conflict(c) => Err(FoldDbError::Config(format!(
                "schema service refused duplicate descriptive_name '{}': \
                 an Approved schema with identity_hash '{}' is already registered. \
                 Reason: {}. Rename the new schema or reuse the existing one.",
                c.descriptive_name, c.existing_canonical, c.reason
            ))),
        }
    }

    pub(super) async fn add_schema_typed_inner(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
        options: AddSchemaOptions<'_>,
    ) -> FoldDbResult<AddSchemaOutcome> {
        let url = format!("{}/v1/schemas", self.base_url);
        let request = AddSchemaRequest {
            // fold_db Schema → schema_types Schema (wire identity) at the
            // service boundary after diet cut #3.
            schema: schema.clone(),
            mutation_mappers,
            schema_match_source: options.schema_match_source.to_string(),
            fallback_reason: options.fallback_reason.map(str::to_string),
            // Local claims and shared offers both require a developer account
            // for new registrations. This flag selects discovery intent only.
            offer_to_shared_discovery: options.offer_to_shared_discovery,
            shared_surface: options.shared_surface,
        };
        let schema_payload = serde_json::to_value(&request.schema).map_err(|e| {
            FoldDbError::Config(format!("Failed to serialize schema for PoW hash: {e}"))
        })?;

        with_retries(|| async {
            let builder = self.client.post(&url);
            let response = observability::propagation::inject_w3c(builder.json(&request))
                .send()
                .await
                .map_err(|error| {
                    let retryable = reqwest_error_is_retryable(&error);
                    let wrapped = FoldDbError::Config(format!(
                        "Failed to submit schema to schema service at {url}: {error}. Is the schema service running?"
                    ));
                    RetryError::classify(retryable, wrapped)
                })?;

            let status = response.status();

            if status == StatusCode::CREATED || status == StatusCode::OK {
                return parse_accepted(response, "Failed to parse schema response").await;
            }

            if status == StatusCode::CONFLICT {
                // 409 means the descriptive_name is already bound to a
                // different active canonical and the proposal could not be
                // cleanly expanded (e.g. cross-schema_type). The body is a
                // `DescriptiveNameConflict` — we hand it back to the caller
                // so the UI can prompt rename vs reuse. Permanent — retrying
                // the same proposal will hit the same conflict.
                return parse_conflict(response, "").await;
            }

            let body = response_body_text(response).await;
            if status == StatusCode::UNAUTHORIZED
                && schema_pow_can_solve(error_reason(&body).as_deref())
            {
                return self
                    .retry_add_schema_with_pow(
                        &url,
                        &request,
                        &schema_payload,
                        schema.owner_app_id.as_deref(),
                        &body,
                    )
                    .await;
            }

            let retryable = status_is_retryable(status);
            Err(RetryError::classify(
                retryable,
                schema_add_error(&url, status, &body),
            ))
        })
        .await
    }

    /// Solve the node-key proof-of-work the service demanded and resubmit once.
    async fn retry_add_schema_with_pow(
        &self,
        url: &str,
        request: &AddSchemaRequest,
        schema_payload: &serde_json::Value,
        owner_app_id: Option<&str>,
        first_body: &str,
    ) -> Result<AddSchemaOutcome, RetryError> {
        let Some(identity) = self.node_identity.as_ref() else {
            return Err(RetryError::Permanent(FoldDbError::Config(format!(
                "Schema service requires node-key proof-of-work for schema mutation, but this client has no node identity configured: {first_body}"
            ))));
        };
        let headers = self
            .solve_schema_pow(identity, schema_payload, owner_app_id)
            .await
            .map_err(RetryError::Permanent)?;
        let retry_builder = self
            .client
            .post(url)
            .header(HEADER_NODE_PUBLIC_KEY, &identity.public_key_b64)
            .header(HEADER_NODE_SIGNATURE, &headers.signature_b64)
            .header(HEADER_POW_CHALLENGE, &headers.challenge_id)
            .header(HEADER_POW_NONCE, &headers.nonce)
            .header(HEADER_POW_CHALLENGE_MAC, &headers.challenge_mac)
            .header(
                HEADER_POW_DIFFICULTY_BITS,
                headers.difficulty_bits.to_string(),
            )
            .header(
                HEADER_POW_EXPIRES_AT,
                headers.expires_at_unix_secs.to_string(),
            )
            .header(HEADER_POW_COUNTER, headers.counter.to_string());
        let retry = observability::propagation::inject_w3c(retry_builder.json(request))
            .send()
            .await
            .map_err(|error| {
                let retryable = reqwest_error_is_retryable(&error);
                let wrapped = FoldDbError::Config(format!(
                    "Failed to submit schema PoW retry to schema service at {url}: {error}"
                ));
                RetryError::classify(retryable, wrapped)
            })?;
        let retry_status = retry.status();
        if retry_status == StatusCode::CREATED || retry_status == StatusCode::OK {
            return parse_accepted(retry, "Failed to parse schema response after PoW retry").await;
        }
        if retry_status == StatusCode::CONFLICT {
            return parse_conflict(retry, " after PoW retry").await;
        }
        let retryable = status_is_retryable(retry_status);
        let body = response_body_text(retry).await;
        Err(RetryError::classify(
            retryable,
            schema_add_error(url, retry_status, &body),
        ))
    }
}

/// Deserialization of a 2xx body is a permanent failure — retrying won't
/// change the server's response shape.
async fn parse_accepted(
    response: reqwest::Response,
    failure_context: &str,
) -> Result<AddSchemaOutcome, RetryError> {
    response
        .json::<AddSchemaResponse>()
        .await
        .map(|r| AddSchemaOutcome::Accepted(Box::new(r)))
        .map_err(|error| {
            RetryError::Permanent(FoldDbError::Config(format!("{failure_context}: {error}")))
        })
}

async fn parse_conflict(
    response: reqwest::Response,
    context_suffix: &str,
) -> Result<AddSchemaOutcome, RetryError> {
    let body = response_body_text(response).await;
    let conflict: DescriptiveNameConflict = serde_json::from_str(&body).map_err(|e| {
        RetryError::Permanent(FoldDbError::Config(format!(
            "Schema service returned CONFLICT (409) with unparseable body{context_suffix}: {body} ({e})"
        )))
    })?;
    Ok(AddSchemaOutcome::Conflict(conflict))
}

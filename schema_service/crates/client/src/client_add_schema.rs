//! Schema add/publish calls of the schema service client.

use super::*;

impl SchemaServiceClient {
    /// Add a schema definition to the schema service.
    ///
    /// Returns the typed `AddSchemaResponse` for 2xx outcomes. Collapses a
    /// 409 `DescriptiveNameConflict` into a `FoldDbError::Config` whose
    /// message includes the conflicting descriptive_name and the existing
    /// canonical hash, so legacy callers that only want a yes/no answer keep
    /// working. Callers that need to distinguish the conflict (UIs, CLI
    /// error mapping) should use [`Self::add_schema_typed`] instead.
    ///
    /// Retries up to 3 times on transient failures (connect errors, timeouts,
    /// 5xx responses). Does NOT retry on 4xx responses including 409 CONFLICT
    /// or on deserialization failures.
    pub async fn add_schema(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<AddSchemaResponse> {
        self.add_schema_with_match_source(schema, mutation_mappers, "direct", None)
            .await
    }

    pub async fn add_schema_with_match_source(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
    ) -> FoldDbResult<AddSchemaResponse> {
        match self
            .add_schema_typed_with_match_source(
                schema,
                mutation_mappers,
                schema_match_source,
                fallback_reason,
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

    /// Like [`Self::add_schema`] but preserves the typed 409
    /// `DescriptiveNameConflict` so callers can drive UI prompts off the
    /// existing canonical hash rather than parsing an error string. 2xx
    /// responses produce `AddSchemaOutcome::Accepted`; 409 produces
    /// `AddSchemaOutcome::Conflict`. Everything else returns
    /// `FoldDbError`.
    pub async fn add_schema_typed(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
    ) -> FoldDbResult<AddSchemaOutcome> {
        self.add_schema_typed_with_match_source(schema, mutation_mappers, "direct", None)
            .await
    }

    pub async fn add_schema_typed_with_match_source(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
    ) -> FoldDbResult<AddSchemaOutcome> {
        self.add_schema_typed_inner(
            schema,
            mutation_mappers,
            AddSchemaOptions {
                schema_match_source,
                fallback_reason,
                offer_to_shared_discovery: false,
                shared_surface: None,
                schema_claim_auth: None,
            },
        )
        .await
    }

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
                    schema_claim_auth: None,
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

    /// Register a shared-surface schema with the DevCert-backed
    /// `schema_claim` envelope required for app-owned shared discovery.
    pub async fn add_schema_with_shared_surface_dev_cert(
        &self,
        schema: &Schema,
        mutation_mappers: HashMap<String, String>,
        shared_surface: SharedSurfaceMetadata,
        schema_match_source: &str,
        fallback_reason: Option<&str>,
        claim: DevSchemaClaim<'_>,
    ) -> FoldDbResult<AddSchemaResponse> {
        let schema_payload = serde_json::to_value(schema).map_err(|e| {
            FoldDbError::Config(format!("Failed to serialize schema for schema claim: {e}"))
        })?;
        let auth = SchemaClaimAuth {
            cert_b64: claim.cert_b64.to_string(),
            signature_b64: sign_dev_schema_claim(claim.dev_key, claim.env, &schema_payload)?,
        };
        match self
            .add_schema_typed_inner(
                schema,
                mutation_mappers,
                AddSchemaOptions {
                    schema_match_source,
                    fallback_reason,
                    offer_to_shared_discovery: true,
                    shared_surface: Some(shared_surface),
                    schema_claim_auth: Some(auth),
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

    // lint:fn-size-ok moved verbatim from its original module
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
            // Local-claim path keeps `offer_to_shared_discovery = false`
            // (cert-free). Shared-surface publish/attach sets it true and
            // attaches the governance envelope.
            offer_to_shared_discovery: options.offer_to_shared_discovery,
            shared_surface: options.shared_surface,
        };
        let schema_payload = serde_json::to_value(&request.schema).map_err(|e| {
            FoldDbError::Config(format!("Failed to serialize schema for PoW hash: {e}"))
        })?;

        with_retries(|| async {
            let mut builder = self.client.post(&url);
            if let Some(auth) = &options.schema_claim_auth {
                builder = builder
                    .header("X-Exemem-Dev-Cert", &auth.cert_b64)
                    .header("X-Signature", &auth.signature_b64);
            }
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
                return response
                    .json::<AddSchemaResponse>()
                    .await
                    .map(|r| AddSchemaOutcome::Accepted(Box::new(r)))
                    .map_err(|error| {
                        // Deserialization of a 2xx body is a permanent failure —
                        // retrying won't change the server's response shape.
                        RetryError::Permanent(FoldDbError::Config(format!(
                            "Failed to parse schema response: {error}"
                        )))
                    });
            }

            if status == StatusCode::CONFLICT {
                // 409 means the descriptive_name is already bound to a
                // different active canonical and the proposal could not be
                // cleanly expanded (e.g. cross-schema_type). The body is a
                // `DescriptiveNameConflict` — we hand it back to the caller
                // so the UI can prompt rename vs reuse. Permanent — retrying
                // the same proposal will hit the same conflict.
                let body = response_body_text(response).await;
                let conflict: DescriptiveNameConflict = serde_json::from_str(&body).map_err(|e| {
                    RetryError::Permanent(FoldDbError::Config(format!(
                        "Schema service returned CONFLICT (409) with unparseable body: {body} ({e})"
                    )))
                })?;
                return Ok(AddSchemaOutcome::Conflict(conflict));
            }

            let body = response_body_text(response).await;
            if status == StatusCode::UNAUTHORIZED && schema_pow_can_solve(error_reason(&body).as_deref()) {
                let Some(identity) = self.node_identity.as_ref() else {
                    return Err(RetryError::Permanent(FoldDbError::Config(format!(
                        "Schema service requires node-key proof-of-work for schema mutation, but this client has no node identity configured: {body}"
                    ))));
                };
                let headers = self
                    .solve_schema_pow(identity, &schema_payload, schema.owner_app_id.as_deref())
                    .await
                    .map_err(RetryError::Permanent)?;
                let mut retry_builder = self
                    .client
                    .post(&url)
                    .header(HEADER_NODE_PUBLIC_KEY, &identity.public_key_b64)
                    .header(HEADER_NODE_SIGNATURE, &headers.signature_b64)
                    .header(HEADER_POW_CHALLENGE, &headers.challenge_id)
                    .header(HEADER_POW_NONCE, &headers.nonce)
                    .header(HEADER_POW_CHALLENGE_MAC, &headers.challenge_mac)
                    .header(
                        HEADER_POW_DIFFICULTY_BITS,
                        headers.difficulty_bits.to_string(),
                    )
                    .header(HEADER_POW_EXPIRES_AT, headers.expires_at_unix_secs.to_string())
                    .header(HEADER_POW_COUNTER, headers.counter.to_string());
                if let Some(auth) = &options.schema_claim_auth {
                    retry_builder = retry_builder
                        .header("X-Exemem-Dev-Cert", &auth.cert_b64)
                        .header("X-Signature", &auth.signature_b64);
                }
                let retry = observability::propagation::inject_w3c(retry_builder.json(&request))
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
                    return retry
                        .json::<AddSchemaResponse>()
                        .await
                        .map(|r| AddSchemaOutcome::Accepted(Box::new(r)))
                        .map_err(|error| {
                            RetryError::Permanent(FoldDbError::Config(format!(
                                "Failed to parse schema response after PoW retry: {error}"
                            )))
                        });
                }
                if retry_status == StatusCode::CONFLICT {
                    let body = response_body_text(retry).await;
                    let conflict: DescriptiveNameConflict =
                        serde_json::from_str(&body).map_err(|e| {
                            RetryError::Permanent(FoldDbError::Config(format!(
                                "Schema service returned CONFLICT (409) with unparseable body after PoW retry: {body} ({e})"
                            )))
                        })?;
                    return Ok(AddSchemaOutcome::Conflict(conflict));
                }
                let retryable = status_is_retryable(retry_status);
                let body = response_body_text(retry).await;
                return Err(RetryError::classify(
                    retryable,
                    schema_add_error(&url, retry_status, &body),
                ));
            }

            let retryable = status_is_retryable(status);
            Err(RetryError::classify(
                retryable,
                schema_add_error(&url, status, &body),
            ))
        })
        .await
    }
}

//! Proof-of-work solving for gated schema mutations.

use super::*;

impl SchemaServiceClient {
    // lint:fn-size-ok moved verbatim from its original module
    pub(super) async fn solve_schema_pow(
        &self,
        identity: &NodePublishIdentity,
        schema_payload: &serde_json::Value,
        app_id: Option<&str>,
    ) -> FoldDbResult<SchemaPowHeaders> {
        const MAX_CLIENT_DIFFICULTY_BITS: u8 = 30;
        let schema_hash = schema_payload_hash(schema_payload).map_err(|e| {
            FoldDbError::Config(format!("Failed to hash schema mutation payload: {e}"))
        })?;
        let url = format!("{}/v1/schemas/mutation-challenge", self.base_url);
        let request = SchemaMutationChallengeRequest {
            node_public_key: identity.public_key_b64.clone(),
            schema_hash: schema_hash.clone(),
            app_id: app_id.map(str::to_string),
        };
        let response =
            observability::propagation::inject_w3c(self.client.post(&url).json(&request))
                .send()
                .await
                .map_err(|e| {
                    FoldDbError::Config(format!(
                        "Failed to request schema mutation PoW challenge at {url}: {e}"
                    ))
                })?;
        let status = response.status();
        if !status.is_success() {
            let body = response_body_text(response).await;
            return Err(schema_challenge_error(&url, status, &body));
        }
        let challenge = response
            .json::<SchemaMutationChallengeResponse>()
            .await
            .map_err(|e| {
                FoldDbError::Config(format!(
                    "Failed to parse schema mutation PoW challenge: {e}"
                ))
            })?;
        if challenge.schema_hash != schema_hash {
            return Err(FoldDbError::Config(format!(
                "Schema mutation PoW challenge hash mismatch: expected {schema_hash}, got {}",
                challenge.schema_hash
            )));
        }
        if challenge.node_public_key_hash != identity.public_key_hash {
            return Err(FoldDbError::Config(format!(
                "Schema mutation PoW challenge node key mismatch: expected {}, got {}",
                identity.public_key_hash, challenge.node_public_key_hash
            )));
        }
        if challenge.difficulty_bits > MAX_CLIENT_DIFFICULTY_BITS {
            return Err(FoldDbError::Config(format!(
                "Schema mutation PoW difficulty {} exceeds client cap {}",
                challenge.difficulty_bits, MAX_CLIENT_DIFFICULTY_BITS
            )));
        }

        let mut counter = challenge.counter_start;
        loop {
            // Check expiry frequently enough that a slow grind cannot burn most
            // of a short TTL between samples (was 65_536; 4_096 ~ few ms at
            // release-rate SHA-256).
            if counter % 4_096 == 0
                && schema_types::clock::unix_secs() >= challenge.expires_at_unix_secs
            {
                return Err(FoldDbError::Config(
                    "Schema mutation PoW challenge expired before a solution was found".to_string(),
                ));
            }
            if pow_satisfies(
                &challenge.nonce,
                &identity.public_key_hash,
                &schema_hash,
                counter,
                challenge.difficulty_bits,
            ) {
                let signature_b64 = sign_schema_claim(
                    identity,
                    &schema_hash,
                    &challenge.challenge_id,
                    &challenge.nonce,
                    counter,
                )?;
                tracing::info!(
                    target: "schema_service_client::pow",
                    difficulty_bits = challenge.difficulty_bits,
                    counter,
                    "solved schema mutation proof-of-work"
                );
                return Ok(SchemaPowHeaders {
                    challenge_id: challenge.challenge_id,
                    nonce: challenge.nonce,
                    challenge_mac: challenge.challenge_mac,
                    difficulty_bits: challenge.difficulty_bits,
                    expires_at_unix_secs: challenge.expires_at_unix_secs,
                    counter,
                    signature_b64,
                });
            }
            counter = counter.checked_add(1).ok_or_else(|| {
                FoldDbError::Config("Schema mutation PoW counter exhausted".to_string())
            })?;
        }
    }
}

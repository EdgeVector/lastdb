//! Gate configuration accessors and challenge issuance on the service state.

use super::*;

impl SchemaServiceState {
    pub fn configure_schema_mutation_gate(&self, config: SchemaMutationGateConfig) {
        if let Ok(mut slot) = self.schema_mutation_gate_config.write() {
            *slot = config;
        }
    }

    pub fn configure_schema_mutation_gate_from_env(&self) {
        self.configure_schema_mutation_gate(SchemaMutationGateConfig::from_env());
    }

    pub fn schema_mutation_gate_config(&self) -> SchemaMutationGateConfig {
        self.schema_mutation_gate_config
            .read()
            .map_or_else(|_| SchemaMutationGateConfig::default(), |c| c.clone())
    }

    pub fn issue_schema_mutation_challenge(
        &self,
        request: &SchemaMutationChallengeRequest,
        remote_ip: Option<&str>,
    ) -> Result<SchemaMutationChallengeResponse, SchemaMutationGateError> {
        let cfg = self.schema_mutation_gate_config();
        let node_public_key = request.node_public_key.trim();
        if node_public_key.is_empty() {
            return Err(SchemaMutationGateError::InvalidChallengeRequest(
                "node_public_key is required",
            ));
        }
        let verifying_key = verifying_key_from_base64(node_public_key)
            .map_err(|_| SchemaMutationGateError::NodeKeyInvalid)?;
        let schema_hash = request.schema_hash.trim();
        if !is_hex_sha256(schema_hash) {
            return Err(SchemaMutationGateError::InvalidChallengeRequest(
                "schema_hash must be a lowercase sha256 hex digest",
            ));
        }

        let now = schema_types::clock::unix_secs();
        let node_public_key_hash = key_id(&verifying_key);
        let app_id = request.app_id.as_deref().filter(|s| !s.is_empty());
        let base_bits = cfg.base_difficulty_bits;
        let adaptive_bits = adaptive_difficulty_bits(
            &cfg,
            &self.schema_mutation_gate_store,
            &node_public_key_hash,
            remote_ip,
            app_id,
            None,
            now,
        )?;
        let catalog_bits = catalog_size_difficulty_bits(
            self.user_schema_count()?,
            cfg.max_catalog_difficulty_bits,
        );
        let uncapped_difficulty_bits = base_bits
            .saturating_add(adaptive_bits)
            .saturating_add(catalog_bits);
        let ttl_cap_bits = max_difficulty_bits_for_challenge_ttl(cfg.challenge_ttl);
        let difficulty_bits = uncapped_difficulty_bits.min(ttl_cap_bits);
        let challenge_id = Uuid::new_v4().to_string();
        let nonce = Uuid::new_v4().simple().to_string();
        let expires_at_unix_secs = now + cfg.challenge_ttl.as_secs();
        let challenge_mac = issue_challenge_mac(
            &cfg,
            &nonce,
            &node_public_key_hash,
            schema_hash,
            difficulty_bits,
            expires_at_unix_secs,
        )?;

        tracing::info!(
            target: "schema_service::schema",
            metric = "schema_mutation_gate_challenge_total",
            status = "issued",
            node_public_key_hash = %node_public_key_hash,
            schema_hash = %schema_hash,
            app_id = app_id.unwrap_or("-"),
            difficulty_bits,
            uncapped_difficulty_bits,
            ttl_cap_bits,
            base_bits,
            adaptive_bits,
            catalog_bits,
            "schema mutation gate issued proof-of-work challenge"
        );

        Ok(SchemaMutationChallengeResponse {
            pow_input: pow_input(&nonce, &node_public_key_hash, schema_hash, 0),
            challenge_id,
            nonce,
            challenge_mac,
            node_public_key_hash,
            schema_hash: schema_hash.to_string(),
            difficulty_bits,
            expires_at_unix_secs,
            counter_start: 0,
        })
    }
}

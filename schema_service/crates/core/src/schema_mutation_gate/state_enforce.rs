//! Gate enforcement and proof-of-work verification on the service state.

use super::*;

impl SchemaServiceState {
    // lint:fn-size-ok moved verbatim from its original module
    pub fn enforce_schema_mutation_gate(
        &self,
        observation: &SchemaMutationGateObservation,
        schema: &Schema,
        schema_payload: &Value,
        headers: &SchemaMutationGateHeaders,
        remote_ip: Option<&str>,
    ) -> Result<(), SchemaMutationGateError> {
        let cfg = self.schema_mutation_gate_config();
        if !cfg.enforce_shared_mutations {
            return Ok(());
        }
        if !observation.requires(SchemaMutationGateRequirement::NodeKey)
            && !observation.requires(SchemaMutationGateRequirement::ProofOfWork)
        {
            return Ok(());
        }

        let schema_hash = schema_payload_hash(schema_payload)
            .map_err(|e| SchemaMutationGateError::Internal(e.to_string()))?;
        let node_public_key = headers
            .node_public_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or(SchemaMutationGateError::NodeKeyRequired)?;
        let verifying_key = verifying_key_from_base64(node_public_key)
            .map_err(|_| SchemaMutationGateError::NodeKeyInvalid)?;
        let node_public_key_hash = key_id(&verifying_key);
        let signature = headers
            .node_signature
            .as_deref()
            .ok_or(SchemaMutationGateError::NodeSignatureRequired)?;
        let challenge_id = headers
            .challenge_id
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?;
        let nonce = headers
            .nonce
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?;
        let challenge_mac = headers
            .challenge_mac
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?;
        let difficulty_bits = headers
            .difficulty_bits
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?
            .parse::<u8>()
            .map_err(|_| SchemaMutationGateError::ProofOfWorkInvalid)?;
        let expires_at_unix_secs = headers
            .expires_at_unix_secs
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?
            .parse::<u64>()
            .map_err(|_| SchemaMutationGateError::ProofOfWorkInvalid)?;
        let counter = headers
            .counter
            .as_deref()
            .ok_or(SchemaMutationGateError::ProofOfWorkRequired)?
            .parse::<u64>()
            .map_err(|_| SchemaMutationGateError::ProofOfWorkInvalid)?;

        let envelope: SignatureEnvelope = decode_b64_json(signature)
            .map_err(|_| SchemaMutationGateError::NodeSignatureInvalid)?;
        let signature_payload = node_signature_payload(
            &schema_hash,
            &node_public_key_hash,
            challenge_id,
            nonce,
            counter,
        );
        verify_envelope(&verifying_key, &envelope)
            .map_err(|_| SchemaMutationGateError::NodeSignatureInvalid)?;
        if envelope.purpose != Purpose::SchemaClaim {
            return Err(SchemaMutationGateError::NodeSignatureInvalid);
        }
        if envelope.env != self.app_identity_config().deployment_env {
            return Err(SchemaMutationGateError::NodeSignatureInvalid);
        }
        let expected_payload_hash = compute_payload_hash(&signature_payload)
            .map_err(|_| SchemaMutationGateError::NodeSignatureInvalid)?;
        if envelope.payload_hash != expected_payload_hash {
            return Err(SchemaMutationGateError::NodeSignatureInvalid);
        }

        self.verify_pow_challenge(&PowChallengeProof {
            nonce,
            challenge_mac,
            counter,
            node_public_key_hash: &node_public_key_hash,
            schema_hash: &schema_hash,
            difficulty_bits,
            expires_at_unix_secs,
        })?;

        self.consume_schema_mutation_quota(
            &cfg,
            &node_public_key_hash,
            remote_ip,
            schema.owner_app_id.as_deref(),
            headers.dev_pubkey.as_deref(),
        )?;

        tracing::info!(
            target: "schema_service::schema",
            metric = "schema_mutation_gate_enforce_total",
            status = "ok",
            node_public_key_hash = %node_public_key_hash,
            schema_hash = %schema_hash,
            intent = observation.intent.as_str(),
            difficulty_bits,
            "schema mutation gate accepted node-key proof"
        );
        Ok(())
    }

    pub(super) fn verify_pow_challenge(
        &self,
        proof: &PowChallengeProof<'_>,
    ) -> Result<(), SchemaMutationGateError> {
        let now = schema_types::clock::unix_secs();
        if proof.expires_at_unix_secs < now {
            return Err(SchemaMutationGateError::ProofOfWorkExpired);
        }
        verify_challenge_mac(
            &self.schema_mutation_gate_config(),
            proof.nonce,
            proof.node_public_key_hash,
            proof.schema_hash,
            proof.difficulty_bits,
            proof.expires_at_unix_secs,
            proof.challenge_mac,
        )?;

        if !pow_satisfies(
            proof.nonce,
            proof.node_public_key_hash,
            proof.schema_hash,
            proof.counter,
            proof.difficulty_bits,
        ) {
            return Err(SchemaMutationGateError::ProofOfWorkInvalid);
        }
        Ok(())
    }
}

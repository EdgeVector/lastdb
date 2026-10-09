use super::*;

impl SchemaServiceState {
    pub fn classify_schema_mutation_gate_observe(
        &self,
        schema: &Schema,
        mutation_mappers: &HashMap<String, String>,
        offer_to_shared_discovery: bool,
    ) -> SchemaMutationGateObservation {
        let owner_app_id = schema
            .owner_app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let intent = if self
            .classify_idempotent_repost(schema, mutation_mappers)
            .is_some()
        {
            SchemaMutationGateIntent::IdempotentRepost
        } else if owner_app_id.is_some() && !offer_to_shared_discovery {
            SchemaMutationGateIntent::LocalClaim
        } else if owner_app_id.is_some() {
            SchemaMutationGateIntent::SharedDiscoveryPublish
        } else {
            SchemaMutationGateIntent::NewSharedMutation
        };

        let required_gates = match intent {
            SchemaMutationGateIntent::IdempotentRepost => vec![],
            SchemaMutationGateIntent::LocalClaim | SchemaMutationGateIntent::NewSharedMutation => {
                vec![
                    SchemaMutationGateRequirement::NodeKey,
                    SchemaMutationGateRequirement::ProofOfWork,
                ]
            }
            SchemaMutationGateIntent::SharedDiscoveryPublish => {
                vec![
                    SchemaMutationGateRequirement::DevCert,
                    SchemaMutationGateRequirement::NodeKey,
                    SchemaMutationGateRequirement::ProofOfWork,
                ]
            }
        };

        SchemaMutationGateObservation {
            intent,
            owner_app_id,
            required_gates,
        }
    }

    pub fn observe_schema_mutation_gate(
        &self,
        schema: &Schema,
        mutation_mappers: &HashMap<String, String>,
        offer_to_shared_discovery: bool,
    ) -> SchemaMutationGateObservation {
        let observation = self.classify_schema_mutation_gate_observe(
            schema,
            mutation_mappers,
            offer_to_shared_discovery,
        );
        tracing::info!(
            target: "schema_service::schema",
            metric = "schema_mutation_gate_observe_total",
            mode = "observe",
            intent = observation.intent.as_str(),
            owner_app_id = observation.owner_app_id.as_deref().unwrap_or("-"),
            required_gates = observation.required_gate_labels(),
            requires_dev_cert =
                observation.requires(SchemaMutationGateRequirement::DevCert),
            requires_api_key =
                observation.requires(SchemaMutationGateRequirement::ApiKey),
            requires_node_key =
                observation.requires(SchemaMutationGateRequirement::NodeKey),
            requires_proof_of_work =
                observation.requires(SchemaMutationGateRequirement::ProofOfWork),
            "schema mutation gate observe-mode decision"
        );
        observation
    }
}

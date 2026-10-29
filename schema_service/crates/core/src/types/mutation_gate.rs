use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Observe-mode classification for `POST /v1/schemas` mutation gate rollout.
///
/// The enum is telemetry-only: callers must not enforce a gate from this
/// classification until the rollout explicitly moves out of observe mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationGateIntent {
    IdempotentRepost,
    LocalClaim,
    SharedDiscoveryPublish,
    NewSharedMutation,
}

impl SchemaMutationGateIntent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdempotentRepost => "idempotent_repost",
            Self::LocalClaim => "local_claim",
            Self::SharedDiscoveryPublish => "shared_discovery_publish",
            Self::NewSharedMutation => "new_shared_mutation",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMutationGateRequirement {
    DevCert,
    ApiKey,
    NodeKey,
    ProofOfWork,
}

impl SchemaMutationGateRequirement {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DevCert => "dev_cert",
            Self::ApiKey => "api_key",
            Self::NodeKey => "node_key",
            Self::ProofOfWork => "proof_of_work",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMutationGateObservation {
    pub intent: SchemaMutationGateIntent,
    pub owner_app_id: Option<String>,
    pub required_gates: Vec<SchemaMutationGateRequirement>,
}

impl SchemaMutationGateObservation {
    pub fn requires(&self, gate: SchemaMutationGateRequirement) -> bool {
        self.required_gates.contains(&gate)
    }

    pub fn required_gate_labels(&self) -> String {
        if self.required_gates.is_empty() {
            return "none".to_string();
        }
        self.required_gates
            .iter()
            .map(|gate| gate.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMatchTelemetrySnapshot {
    pub outcomes: HashMap<String, u64>,
    pub fallback_reasons: HashMap<String, u64>,
    pub sources: HashMap<String, u64>,
}

use serde::{Deserialize, Serialize};

use super::NodeConfigStore;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub private_key: String,
    pub public_key: String,
}

impl NodeConfigStore {
    // --- Node identity ---
    //
    // `identity:private_key` is encrypted at rest with AES-256-GCM when a
    // crypto key is configured on this store handle. The public key is
    // stored in plaintext: there is no confidentiality requirement and
    // callers (e.g. discovery_config) sometimes need to read it without
    // holding the encryption key.
}

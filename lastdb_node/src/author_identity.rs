//! The key and device that sign every entry this node authors.

use std::path::Path;

use app_identity_crypto::SigningKey;
use mutation_log::{DeviceId, NodeKey};

/// The key and device that sign every entry this node authors.
pub(crate) struct AuthorIdentity {
    pub(crate) signing_key: SigningKey,
    pub(crate) node_key: NodeKey,
    pub(crate) device_id: DeviceId,
}

impl AuthorIdentity {
    /// The identity of the home with this node seed. The first call makes the device id.
    pub(crate) fn of_home(home: &Path, node_seed: &[u8; 32]) -> Result<Self, String> {
        let signing_key = SigningKey::from_bytes(node_seed);
        Ok(Self {
            node_key: NodeKey::from_bytes(signing_key.verifying_key().to_bytes()),
            device_id: crate::device_identity::device_id(home)?,
            signing_key,
        })
    }
}

//! Security-related types and data structures

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Public key information stored on the backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicKeyInfo {
    /// Unique identifier for this public key
    pub id: String,
    /// Base64-encoded Ed25519 public key
    pub public_key: String,
    /// User or client ID associated with this key
    pub owner_id: String,
    /// When this key was registered
    pub created_at: i64,
    /// Optional expiration timestamp
    pub expires_at: Option<i64>,
    /// Whether this key is currently active
    pub is_active: bool,
    /// Permissions associated with this key
    pub permissions: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl PublicKeyInfo {
    /// Create a new public key info
    pub fn new(id: String, public_key: String, owner_id: String, permissions: Vec<String>) -> Self {
        Self {
            id,
            public_key,
            owner_id,
            created_at: chrono::Utc::now().timestamp(),
            expires_at: None,
            is_active: true,
            permissions,
            metadata: HashMap::new(),
        }
    }
}

//! Request identity types for local callers.

use serde::{Deserialize, Serialize};

/// Context for a local request — identity plumbing, not an ACL evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessContext {
    /// Who is making the request (public key or user identifier).
    pub user_id: String,
    /// Whether this is the data owner.
    pub is_owner: bool,
}

impl AccessContext {
    /// Create an owner context.
    pub fn owner(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            is_owner: true,
        }
    }

    /// Create a non-owner context.
    pub fn remote(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            is_owner: false,
        }
    }
}

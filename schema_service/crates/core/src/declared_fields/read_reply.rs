//! HTTP classification for one declared-field read.

use serde_json::{json, Value};

use crate::state::SchemaServiceState;

/// The declaration read both HTTP routers answer.
#[derive(Debug)]
pub enum DeclaredFieldReadReply {
    /// The declaration exists. `body` is the stored record as JSON.
    /// Success is HTTP 200.
    Ready { status: u16, body: Value },
    /// The read failed. `status` and `body` come from
    /// [`super::DeclaredFieldError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::get_declared_field`] for the HTTP routers.
    ///
    /// Each router still reads the declaration id from its path.
    #[must_use]
    pub fn declared_field_read_reply(&self, declaration_id: &str) -> DeclaredFieldReadReply {
        match self.get_declared_field(declaration_id) {
            Ok(record) => DeclaredFieldReadReply::Ready {
                status: 200,
                body: json!(record),
            },
            Err(error) => {
                let (status, error_body) = error.to_http();
                DeclaredFieldReadReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
    }
}

//! HTTP classification for an app registration.

use super::*;

/// The registration both HTTP routers answer.
#[derive(Debug)]
pub enum RegisterAppReply {
    /// The registration finished. `status` and `body` come from
    /// [`AppRegisterOutcome::to_http`]. A new app is HTTP 201.
    /// An update or an idempotent re-post is HTTP 200.
    Ready { status: u16, body: Value },
    /// The registration failed. `status` and `body` come from
    /// [`AppRegisterError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::register_app`] for the HTTP routers.
    ///
    /// Each router still checks the signature headers. The metric stays
    /// inside `register_app`. The `env` field still comes from
    /// [`SchemaServiceState::deployment_env_label`].
    #[must_use]
    pub async fn register_app_reply(
        &self,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> RegisterAppReply {
        match self.register_app(cert_header, sig_header, body).await {
            Ok(outcome) => {
                let (status, body) = outcome.to_http(&self.deployment_env_label());
                RegisterAppReply::Ready { status, body }
            }
            Err(error) => {
                let (status, body) = error.to_http();
                RegisterAppReply::Failed { status, body }
            }
        }
    }
}

//! HTTP classification for an app update.

use super::*;

/// The update both HTTP routers answer.
#[derive(Debug)]
pub enum UpdateAppReply {
    /// The update finished. `status` and `body` come from
    /// [`AppUpdateOutcome::to_http`]. A change and a no-op are both HTTP 200.
    Ready { status: u16, body: Value },
    /// The update failed. `status` and `body` come from
    /// [`AppUpdateError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::update_app`] for the HTTP routers.
    ///
    /// Each router still checks the path and the signature headers. The
    /// metric stays inside `update_app`. The `env` field still comes from
    /// [`SchemaServiceState::deployment_env_label`].
    #[must_use]
    pub async fn update_app_reply(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
        body: &Value,
    ) -> UpdateAppReply {
        match self.update_app(app_id, cert_header, sig_header, body).await {
            Ok(outcome) => {
                let (status, body) = outcome.to_http(&self.deployment_env_label());
                UpdateAppReply::Ready { status, body }
            }
            Err(error) => {
                let (status, body) = error.to_http();
                UpdateAppReply::Failed { status, body }
            }
        }
    }
}

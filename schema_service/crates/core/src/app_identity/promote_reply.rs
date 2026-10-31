//! HTTP classification for an app promotion.

use super::*;

/// The promotion both HTTP routers answer.
#[derive(Debug)]
pub enum PromoteAppReply {
    /// The promotion finished. `status` and `body` come from
    /// [`AppPromoteOutcome::to_http`]. Success is HTTP 200.
    Ready { status: u16, body: Value },
    /// The promotion failed. `status` and `body` come from
    /// [`AppPromoteError::to_http`].
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify [`Self::promote_app`] for the HTTP routers.
    ///
    /// Each router still checks the path and the signature headers. The
    /// metric stays inside `promote_app`.
    #[must_use]
    pub async fn promote_app_reply(
        &self,
        app_id: &str,
        cert_header: &str,
        sig_header: &str,
    ) -> PromoteAppReply {
        match self.promote_app(app_id, cert_header, sig_header).await {
            Ok(outcome) => {
                let (status, body) = outcome.to_http();
                PromoteAppReply::Ready { status, body }
            }
            Err(error) => {
                let (status, body) = error.to_http();
                PromoteAppReply::Failed { status, body }
            }
        }
    }
}

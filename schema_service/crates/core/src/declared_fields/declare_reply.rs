//! HTTP classification for a field declaration.

use serde_json::Value;

use super::DeclareFieldRequest;
use crate::state::SchemaServiceState;

/// The declaration both HTTP routers answer.
#[derive(Debug)]
pub enum DeclareFieldReply {
    /// Authorization passed and the declaration finished. `body` is
    /// [`super::DeclaredFieldRecord::declare_json`]. Success is HTTP 200.
    Ready { status: u16, body: Value },
    /// Authorization or the declaration failed. `status` and `body` come
    /// from the error `to_http`.
    Failed { status: u16, body: Value },
}

impl SchemaServiceState {
    /// Classify authorization and [`Self::declare_field`] for the HTTP routers.
    ///
    /// Each router still parses the body and reads the signature headers.
    /// `declare_field` still trusts a caller that already authorized.
    #[must_use]
    pub async fn declare_field_reply(
        &self,
        request: &DeclareFieldRequest,
        cert_header: Option<&str>,
        sig_header: Option<&str>,
        body: &Value,
    ) -> DeclareFieldReply {
        if let Err(error) = self
            .authorize_field_declare(&request.owner_app_id, cert_header, sig_header, body)
            .await
        {
            let (status, error_body) = error.to_http();
            return DeclareFieldReply::Failed {
                status,
                body: error_body,
            };
        }
        match self.declare_field(request).await {
            Ok(record) => DeclareFieldReply::Ready {
                status: 200,
                body: record.declare_json(),
            },
            Err(error) => {
                let (status, error_body) = error.to_http();
                DeclareFieldReply::Failed {
                    status,
                    body: error_body,
                }
            }
        }
    }
}

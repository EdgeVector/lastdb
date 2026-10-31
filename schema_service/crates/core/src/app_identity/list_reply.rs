//! HTTP classification for the public app shelf.

use serde_json::{json, Value};

use crate::state::SchemaServiceState;

/// The public app shelf both HTTP routers answer.
///
/// The read has no failure arm. A poisoned lock returns an empty list, same
/// as [`SchemaServiceState::list_live_apps`]. Success is HTTP 200.
#[derive(Debug)]
pub struct ListLiveAppsReply {
    /// Success is HTTP 200.
    pub status: u16,
    /// The live, non-revoked apps as JSON.
    pub body: Value,
}

impl SchemaServiceState {
    /// Classify [`Self::list_live_apps`] for the HTTP routers.
    ///
    /// The shelf method stays public. Each router still writes its response.
    /// `GET /v2/apps` is a different read and stays in the Lambda router.
    #[must_use]
    pub fn list_live_apps_reply(&self) -> ListLiveAppsReply {
        ListLiveAppsReply {
            status: 200,
            body: json!(self.list_live_apps()),
        }
    }
}

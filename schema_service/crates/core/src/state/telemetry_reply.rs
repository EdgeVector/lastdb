//! HTTP classification for the schema-match telemetry read.

use serde_json::{json, Value};

use super::SchemaServiceState;

/// The telemetry read both HTTP routers answer.
///
/// The read has no failure arm. Success is HTTP 200 and the snapshot JSON.
#[derive(Debug)]
pub struct SchemaMatchTelemetryReply {
    /// Success is HTTP 200.
    pub status: u16,
    /// The snapshot as JSON. Both routers return this body.
    pub body: Value,
}

impl SchemaServiceState {
    /// Classify [`Self::schema_match_telemetry_snapshot`] for the HTTP routers.
    ///
    /// The snapshot method stays public. Each router still writes its response.
    #[must_use]
    pub fn schema_match_telemetry_reply(&self) -> SchemaMatchTelemetryReply {
        SchemaMatchTelemetryReply {
            status: 200,
            body: json!(self.schema_match_telemetry_snapshot()),
        }
    }
}

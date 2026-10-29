//! Explicit owner trust and bounded original device events. Cloud remains inert.

use super::*;
use fold_db::storage::disk_pack::byte_backup::SnapshotAdoptionCandidate;
use fold_db::storage::disk_pack::logical_capture::{CaptureEvent, Position};
use serde_json::Value;

const CONTROL_BYTES: usize = 8192;
const EVENT_BYTES: usize = 1024 * 1024;
const PLAN_BYTES: usize = 32 * 1024 + CONTROL_BYTES;
const HTTP_BYTES: usize = EVENT_BYTES + CONTROL_BYTES;

enum Request {
    Export {
        after: u64,
        limit: usize,
    },
    Enroll {
        anchor: Position,
    },
    Position {
        origin: Option<String>,
    },
    Import {
        events: Vec<CaptureEvent>,
    },
    Snapshot {
        action: crate::byte_cloud_backup::LocalSnapshotAction,
        request_id: cloud_backup_protocol::Id,
    },
    SnapshotPlan {
        request_id: cloud_backup_protocol::Id,
        candidate: Box<SnapshotAdoptionCandidate>,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Export,
    Enroll,
    Position,
    Import,
    SnapshotPrepare,
    SnapshotInspect,
    SnapshotResume,
    SnapshotRelease,
    SnapshotPlan,
}

// A plain struct parses the event field directly. An internally tagged enum
// first buffers a generic value tree before the bounded event visitor runs.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    action: Action,
    after: Option<u64>,
    limit: Option<usize>,
    anchor: Option<Position>,
    origin: Option<String>,
    events: Option<Events>,
    request_id: Option<cloud_backup_protocol::Id>,
    candidate: Option<Box<SnapshotAdoptionCandidate>>,
}

struct Events(Vec<CaptureEvent>);

impl<'de> Deserialize<'de> for Events {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        bounded_events(deserializer).map(Self)
    }
}

impl TryFrom<WireRequest> for Request {
    type Error = ();

    fn try_from(wire: WireRequest) -> Result<Self, Self::Error> {
        match (
            wire.action,
            wire.after,
            wire.limit,
            wire.anchor,
            wire.origin,
            wire.events,
            wire.request_id,
            wire.candidate,
        ) {
            (Action::Export, Some(after), Some(limit), None, None, None, None, None) => {
                Ok(Self::Export { after, limit })
            }
            (Action::Enroll, None, None, Some(anchor), None, None, None, None) => {
                Ok(Self::Enroll { anchor })
            }
            (Action::Position, None, None, None, origin, None, None, None) => {
                Ok(Self::Position { origin })
            }
            (Action::Import, None, None, None, None, Some(Events(events)), None, None) => {
                Ok(Self::Import { events })
            }
            (action, None, None, None, None, None, Some(request_id), None) => {
                use crate::byte_cloud_backup::LocalSnapshotAction;
                let action = match action {
                    Action::SnapshotPrepare => LocalSnapshotAction::Prepare,
                    Action::SnapshotInspect => LocalSnapshotAction::Inspect,
                    Action::SnapshotResume => LocalSnapshotAction::Resume,
                    Action::SnapshotRelease => LocalSnapshotAction::Release,
                    _ => return Err(()),
                };
                Ok(Self::Snapshot { action, request_id })
            }
            (
                Action::SnapshotPlan,
                None,
                None,
                None,
                None,
                None,
                Some(request_id),
                Some(candidate),
            ) => Ok(Self::SnapshotPlan {
                request_id,
                candidate,
            }),
            _ => Err(()),
        }
    }
}

fn bounded_events<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<CaptureEvent>, D::Error> {
    struct Events;
    impl<'de> serde::de::Visitor<'de> for Events {
        type Value = Vec<CaptureEvent>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("one to four logical events")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut events = Vec::with_capacity(4);
            for _ in 0..4 {
                match sequence.next_element()? {
                    Some(event) => events.push(event),
                    None if !events.is_empty() => return Ok(events),
                    None => return Err(serde::de::Error::custom("logical events are empty")),
                }
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("logical events exceed four"));
            }
            Ok(events)
        }
    }
    deserializer.deserialize_seq(Events)
}

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "logical sync is owner-only", ctx);
    }
    if req.body.len() > HTTP_BYTES {
        return error_response(413, "logical sync request exceeds its byte limit", ctx);
    }
    let request = match serde_json::from_slice::<WireRequest>(&req.body) {
        Ok(value) => match Request::try_from(value) {
            Ok(request) => request,
            Err(()) => return error_response(400, "invalid logical sync fields", ctx),
        },
        Err(_) => return error_response(400, "invalid logical sync request", ctx),
    };
    let control_bytes = if matches!(&request, Request::SnapshotPlan { .. }) {
        PLAN_BYTES
    } else {
        CONTROL_BYTES
    };
    if !matches!(&request, Request::Import { .. }) && req.body.len() > control_bytes {
        return error_response(413, "logical sync control exceeds its byte limit", ctx);
    }
    match perform(host, request).await {
        Ok(value) => {
            let response = json_ok(&envelope(&value, ctx.user_id.as_str()));
            if response.body.len() > HTTP_BYTES {
                return error_response(413, "logical sync response exceeds its byte limit", ctx);
            }
            response
        }
        Err(error) => error_response(409, &error, ctx),
    }
}

async fn perform(host: &Host, request: Request) -> Result<Value, String> {
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or("logical sync requires native disk storage")?;
    match request {
        Request::Export { after, limit } => {
            let value = runtime
                .logical_capture_export(after, limit)
                .await
                .map_err(|error| error.to_string())?;
            encoded(value)
        }
        Request::Enroll { anchor } => {
            let position = runtime
                .logical_capture_enroll_peer(anchor)
                .await
                .map_err(|error| error.to_string())?;
            encoded(serde_json::json!({ "position": position }))
        }
        Request::Position {
            origin: Some(origin),
        } => {
            let position = runtime
                .logical_capture_peer_position(&origin)
                .await
                .map_err(|error| error.to_string())?;
            encoded(serde_json::json!({ "position": position }))
        }
        Request::Position { origin: None } => {
            let peers = runtime
                .logical_capture_peer_positions()
                .await
                .map_err(|error| error.to_string())?;
            encoded(serde_json::json!({ "peers": peers }))
        }
        Request::Import { events } => encoded(
            host.db
                .mutation_manager()
                .import_logical_events(events)
                .await
                .map_err(|error| error.to_string())?,
        ),
        Request::Snapshot { action, request_id } => encoded(
            crate::byte_cloud_backup::local_snapshot(host, action, request_id)
                .await
                .map_err(|error| error.to_string())?,
        ),
        Request::SnapshotPlan {
            request_id,
            candidate,
        } => encoded(
            crate::byte_cloud_backup::local_snapshot_plan(host, request_id, *candidate)
                .await
                .map_err(|error| error.to_string())?,
        ),
    }
}

fn encoded(value: impl Serialize) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|_| "cannot encode logical sync response".into())
}

//! JSON for the mutation-log data routes.
//!
//! Append, head, list, ack, and snapshot use G's field names and JSON numbers.
//! Status bodies are not encoded here. A receipt grant revision stays a decimal
//! string on `access`, and this file does not send a second list body.

use cloud_backup_protocol::canonical_bytes;
use mutation_log::{
    AppendBatch, AppendOutcome, Capability, Entry, Incorporation, ListPage, LogError, ScopedHead,
    Snapshot, SnapshotRef, Stamp, StoredEntry, Writer,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::paths;

#[derive(Serialize)]
struct DatabaseBody<'a> {
    database_id: &'a str,
}

#[derive(Serialize)]
struct AppendRequest<'a> {
    database_id: &'a str,
    device_id: &'a str,
    entries: Vec<EntryWire<'a>>,
}

#[derive(Serialize)]
struct EntryWire<'a> {
    writer_seq: u64,
    authored_at_ms: u64,
    namespace: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    incorporated_from: Option<IncorporatedWire<'a>>,
    payload: String,
}

#[derive(Serialize)]
struct IncorporatedWire<'a> {
    writer_key: &'a str,
    writer_device: &'a str,
    writer_seq: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendResponse {
    result: String,
    #[serde(default)]
    delivery_seqs: Vec<u64>,
    #[serde(default)]
    next_seq: Option<u64>,
}

#[derive(Serialize)]
struct ListRequest<'a> {
    database_id: &'a str,
    device_id: &'a str,
    after: u64,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageResponse {
    entries: Vec<StoredWire>,
    next: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredWire {
    delivery_seq: u64,
    accepted_at_ms: u64,
    writer_key: String,
    writer_device: String,
    writer_seq: u64,
    authored_at_ms: u64,
    namespace: String,
    #[serde(default)]
    incorporated_from: Option<IncorporatedBody>,
    payload: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncorporatedBody {
    writer_key: String,
    writer_device: String,
    writer_seq: u64,
}

#[derive(Serialize)]
struct AckRequest<'a> {
    database_id: &'a str,
    device_id: &'a str,
    through: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AckResponse {
    through: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    cursor: u64,
    #[serde(default)]
    authored_at_ms: Option<u64>,
    #[serde(default)]
    writer_key: Option<String>,
    #[serde(default)]
    writer_device: Option<String>,
    #[serde(default)]
    sequence: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct RevokeRequest<'a> {
    pub database_id: &'a str,
    pub principal: &'a str,
    pub capability: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotResponse {
    through_delivery_seq: u64,
    at_ms: u64,
    snapshot_ref: String,
}

#[derive(Serialize)]
pub(crate) struct SnapshotBytesRequest<'a> {
    pub database_id: &'a str,
    pub snapshot_ref: &'a str,
}

pub(crate) fn database_body(database_id: &str) -> Result<Vec<u8>, LogError> {
    canonical_bytes(&DatabaseBody { database_id }).map_err(|_| LogError::InvalidEntry)
}

pub(crate) fn append_request(batch: &AppendBatch) -> Result<Vec<u8>, LogError> {
    let database_id = batch
        .entries
        .first()
        .map(|entry| entry.database_id.as_str())
        .ok_or(LogError::EmptyBatch)?;
    if batch
        .entries
        .iter()
        .any(|entry| entry.database_id.as_str() != database_id || entry.writer != batch.writer)
    {
        return Err(LogError::InvalidEntry);
    }
    let request = AppendRequest {
        database_id,
        device_id: batch.writer.device.as_str(),
        entries: batch
            .entries
            .iter()
            .map(entry_wire)
            .collect::<Result<Vec<_>, _>>()?,
    };
    canonical_bytes(&request).map_err(|_| LogError::InvalidEntry)
}

pub(crate) fn list_body(
    database_id: &str,
    device_id: &str,
    after: u64,
    limit: usize,
) -> Result<Vec<u8>, LogError> {
    canonical_bytes(&ListRequest {
        database_id,
        device_id,
        after,
        limit,
    })
    .map_err(|_| LogError::InvalidEntry)
}

pub(crate) fn ack_body(
    database_id: &str,
    device_id: &str,
    through: u64,
) -> Result<Vec<u8>, LogError> {
    canonical_bytes(&AckRequest {
        database_id,
        device_id,
        through,
    })
    .map_err(|_| LogError::InvalidEntry)
}

fn entry_wire(entry: &Entry) -> Result<EntryWire<'_>, LogError> {
    Ok(EntryWire {
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: entry.namespace.as_str(),
        incorporated_from: entry.incorporated_from.as_ref().map(incorporation_wire),
        payload: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&entry.payload),
    })
}

fn incorporation_wire(value: &Incorporation) -> IncorporatedWire<'_> {
    IncorporatedWire {
        writer_key: value.writer.key.as_str(),
        writer_device: value.writer.device.as_str(),
        writer_seq: value.writer_seq,
    }
}

pub(crate) fn append_outcome(body: &[u8]) -> Result<AppendOutcome, LogError> {
    let response: AppendResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    match response.result.as_str() {
        "accepted" => Ok(AppendOutcome::Accepted {
            delivery_seqs: response.delivery_seqs,
        }),
        "duplicate" => Ok(AppendOutcome::Duplicate {
            next_seq: response.next_seq.ok_or(LogError::UnexpectedOutcome)?,
        }),
        _ => Err(LogError::UnexpectedOutcome),
    }
}

pub(crate) fn list_page(database_id: &str, body: &[u8]) -> Result<ListPage, LogError> {
    let page: PageResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    Ok(ListPage {
        entries: page
            .entries
            .into_iter()
            .map(|wire| stored_entry(database_id, wire))
            .collect::<Result<Vec<_>, _>>()?,
        next: page.next,
    })
}

pub(crate) fn acknowledged(body: &[u8]) -> Result<u64, LogError> {
    let response: AckResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    Ok(response.through)
}

pub(crate) fn scoped_head(body: &[u8]) -> Result<Option<ScopedHead>, LogError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    if empty_head(&value) {
        return Ok(None);
    }
    let wire: HeadBody = serde_json::from_value(value).map_err(|_| LogError::UnexpectedOutcome)?;
    Ok(Some(ScopedHead {
        cursor: wire.cursor,
        stamp: stamp_of(wire)?,
    }))
}

pub(crate) fn snapshot(body: &[u8]) -> Result<Snapshot, LogError> {
    let response: SnapshotResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    Ok(Snapshot {
        through_delivery_seq: response.through_delivery_seq,
        at_ms: response.at_ms,
        snapshot_ref: SnapshotRef::new(&response.snapshot_ref)?,
    })
}

pub(crate) fn service_error(body: &[u8]) -> Option<LogError> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let code = value.get("code").and_then(Value::as_str).unwrap_or("");
    let field = value.get("field").and_then(Value::as_str).unwrap_or("");
    let through = json_u64(value.get("snapshot_through")).unwrap_or(0);
    match code {
        "cursor_expired" => Some(LogError::CursorExpired {
            snapshot_through: through,
        }),
        // G e37d7c851: list cursor expiry is HTTP 409 predecessor_conflict.
        "predecessor_conflict" if field == "mutation_log.cursor" => Some(LogError::CursorExpired {
            snapshot_through: through,
        }),
        "conflict" => Some(LogError::Conflict),
        "unauthorized" => Some(LogError::Unauthorized),
        _ => None,
    }
}

pub(crate) fn capability_name(capability: &Capability) -> &'static str {
    match capability {
        Capability::Owner => "owner",
        Capability::Receive => "receive",
        Capability::Submit => "submit",
    }
}

pub(crate) fn operation(path: &str) -> &'static str {
    match path {
        paths::CREATE => "create-database",
        paths::ACCESS => "access",
        paths::APPEND => "append",
        paths::HEAD => "head",
        paths::LIST => "list",
        paths::ACK => "ack",
        paths::REVOKE => "revoke",
        paths::SNAPSHOT => "snapshot",
        paths::SNAPSHOT_BYTES => "snapshot-bytes",
        _ => "log",
    }
}

fn empty_head(value: &Value) -> bool {
    match value.as_object() {
        Some(object) => object.len() == 1 && matches!(object.get("cursor"), Some(Value::Null)),
        None => false,
    }
}

fn stamp_of(wire: HeadBody) -> Result<Option<Stamp>, LogError> {
    match (
        wire.authored_at_ms,
        wire.writer_key,
        wire.writer_device,
        wire.sequence,
    ) {
        (None, None, None, None) => Ok(None),
        (Some(authored_at_ms), Some(key), Some(device), Some(sequence)) => Ok(Some(Stamp {
            authored_at_ms,
            writer: writer_of(&key, &device)?,
            sequence,
        })),
        _ => Err(LogError::UnexpectedOutcome),
    }
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    value.as_str()?.parse().ok()
}

fn stored_entry(database_id: &str, wire: StoredWire) -> Result<StoredEntry, LogError> {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(wire.payload)
        .map_err(|_| LogError::InvalidEntry)?;
    Ok(StoredEntry {
        delivery_seq: wire.delivery_seq,
        accepted_at_ms: wire.accepted_at_ms,
        entry: Entry {
            database_id: mutation_log::DatabaseId::new(database_id)?,
            writer: writer_of(&wire.writer_key, &wire.writer_device)?,
            writer_seq: wire.writer_seq,
            authored_at_ms: wire.authored_at_ms,
            namespace: mutation_log::Namespace::new(&wire.namespace)?,
            incorporated_from: wire
                .incorporated_from
                .map(|value| incorporated(value))
                .transpose()?,
            payload,
        },
    })
}

fn incorporated(body: IncorporatedBody) -> Result<Incorporation, LogError> {
    Ok(Incorporation {
        writer: writer_of(&body.writer_key, &body.writer_device)?,
        writer_seq: body.writer_seq,
    })
}

fn writer_of(key: &str, device: &str) -> Result<Writer, LogError> {
    Ok(Writer {
        key: mutation_log::PrincipalId::new(key)?,
        device: mutation_log::DeviceId::new(device)?,
    })
}

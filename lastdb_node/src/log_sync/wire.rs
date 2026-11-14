//! JSON for the mutation-log data routes.
//!
//! Append, head, and list use one encoding: decimal strings. A shareable
//! list entry is nested. `payload_b64` is unpadded URL-safe base64 of the
//! envelope. Status bodies are not encoded here.

use base64::Engine as _;
use cloud_backup_protocol::{canonical_bytes, DecimalU64, Digest};
use mutation_log::{
    AppendBatch, AppendOutcome, Entry, Incorporation, ListPage, LogError, ScopedHead, Stamp,
    StoredEntry, Writer,
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
    writer_seq: DecimalU64,
    authored_at_ms: DecimalU64,
    namespace: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    incorporated_from: Option<IncorporatedWire<'a>>,
    payload_b64: String,
}

#[derive(Serialize)]
struct IncorporatedWire<'a> {
    node_public_key: &'a str,
    device_id: &'a str,
    writer_seq: DecimalU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendResponse {
    result: String,
    delivery_seqs: Vec<DecimalU64>,
    next_seq: DecimalU64,
}

#[derive(Serialize)]
struct ListRequest<'a> {
    database_id: &'a str,
    after: DecimalU64,
    limit: DecimalU64,
    #[serde(skip_serializing_if = "is_false")]
    accept_native_reset: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageResponse {
    entries: Vec<StoredWire>,
    next: DecimalU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredWire {
    delivery_seq: DecimalU64,
    accepted_at_ms: DecimalU64,
    entry: EntryBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryBody {
    database_id: String,
    node_public_key: String,
    device_id: String,
    writer_seq: DecimalU64,
    authored_at_ms: DecimalU64,
    namespace: String,
    #[serde(default)]
    incorporated_from: Option<IncorporatedBody>,
    payload_b64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncorporatedBody {
    node_public_key: String,
    device_id: String,
    writer_seq: DecimalU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    cursor: DecimalU64,
    next: DecimalU64,
    entries: Vec<StoredWire>,
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
    after: u64,
    limit: usize,
    accept_native_reset: bool,
) -> Result<Vec<u8>, LogError> {
    let limit = u64::try_from(limit).map_err(|_| LogError::InvalidEntry)?;
    canonical_bytes(&ListRequest {
        database_id,
        after: DecimalU64::new(after),
        limit: DecimalU64::new(limit),
        accept_native_reset,
    })
    .map_err(|_| LogError::InvalidEntry)
}

fn entry_wire(entry: &Entry) -> Result<EntryWire<'_>, LogError> {
    Ok(EntryWire {
        writer_seq: DecimalU64::new(entry.writer_seq),
        authored_at_ms: DecimalU64::new(entry.authored_at_ms),
        namespace: entry.namespace.as_str(),
        incorporated_from: entry.incorporated_from.as_ref().map(incorporation_wire),
        payload_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&entry.payload),
    })
}

fn incorporation_wire(value: &Incorporation) -> IncorporatedWire<'_> {
    IncorporatedWire {
        node_public_key: value.writer.key.as_str(),
        device_id: value.writer.device.as_str(),
        writer_seq: DecimalU64::new(value.writer_seq),
    }
}

pub(crate) fn append_outcome(body: &[u8], batch: &AppendBatch) -> Result<AppendOutcome, LogError> {
    let response: AppendResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    let tail = batch.entries.last().ok_or(LogError::EmptyBatch)?.writer_seq;
    let next = response.next_seq.get();
    let delivery_seqs: Vec<u64> = response
        .delivery_seqs
        .into_iter()
        .map(DecimalU64::get)
        .collect();
    match response.result.as_str() {
        "accepted"
            if delivery_seqs.first().is_some_and(|seq| *seq > 0)
                && delivery_seqs.len() <= batch.entries.len()
                && delivery_seqs
                    .windows(2)
                    .all(|pair| pair[0].checked_add(1) == Some(pair[1]))
                && tail.checked_add(1) == Some(next) =>
        {
            Ok(AppendOutcome::Accepted { delivery_seqs })
        }
        "duplicate" if delivery_seqs.is_empty() => Ok(AppendOutcome::Duplicate { next_seq: next }),
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
        next: page.next.get(),
    })
}

pub(crate) fn scoped_head(body: &[u8]) -> Result<Option<ScopedHead>, LogError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    if empty_head(&value) {
        return Ok(None);
    }
    let wire: HeadBody = serde_json::from_value(value).map_err(|_| LogError::UnexpectedOutcome)?;
    if wire.next.get() != wire.cursor.get() {
        return Err(LogError::UnexpectedOutcome);
    }
    Ok(Some(ScopedHead {
        cursor: wire.cursor.get(),
        stamp: head_stamp(&wire.entries)?,
    }))
}

#[derive(Deserialize)]
pub(super) struct NativeCut {
    pub native_snapshot_id: Digest,
    pub snapshot_through: DecimalU64,
}

pub(super) fn native_cut(body: &[u8], after: u64) -> Option<NativeCut> {
    let value: Value = serde_json::from_slice(body).ok()?;
    if value.get("code")?.as_str()? != "predecessor_conflict"
        || value.get("field")?.as_str()? != "mutation_log.cursor"
    {
        return None;
    }
    let cut: NativeCut = serde_json::from_value(value).ok()?;
    (cut.snapshot_through.get() > after).then_some(cut)
}

pub(super) fn unsupported_reset(body: &[u8]) -> bool {
    serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(body).is_ok_and(|error| {
        error.code == cloud_backup_protocol::ErrorCode::InvalidInput && error.field == "http.body"
    })
}

pub(crate) fn service_error(body: &[u8]) -> Option<LogError> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let code = value.get("code").and_then(Value::as_str).unwrap_or("");
    let field = value.get("field").and_then(Value::as_str).unwrap_or("");
    match code {
        "cursor_expired" => expired(&value),
        // List cursor expiry is HTTP 409 predecessor_conflict.
        "predecessor_conflict" if field == "mutation_log.cursor" => expired(&value),
        "conflict" | "identity_conflict" if field == "mutation_log.conflict" => {
            Some(LogError::Conflict)
        }
        "conflict" => Some(LogError::Conflict),
        "unauthorized" => Some(LogError::Unauthorized),
        _ => None,
    }
}

pub(crate) fn operation(path: &str) -> &'static str {
    match path {
        paths::CREATE => "create-database",
        paths::ACCESS => "access",
        paths::APPEND => "append",
        paths::HEAD => "head",
        paths::LIST => "list",
        paths::DELIVERY_OBJECT => "delivery-object",
        _ => "log",
    }
}

fn expired(value: &Value) -> Option<LogError> {
    let through = json_u64(value.get("snapshot_through"))?;
    Some(LogError::CursorExpired {
        snapshot_through: through,
    })
}

fn empty_head(value: &Value) -> bool {
    match value.as_object() {
        Some(object) => object.len() == 1 && matches!(object.get("cursor"), Some(Value::Null)),
        None => false,
    }
}

fn head_stamp(entries: &[StoredWire]) -> Result<Option<Stamp>, LogError> {
    match entries {
        [] => Ok(None),
        [entry] => Ok(Some(Stamp {
            authored_at_ms: Stamp::ordering_millis(
                entry.entry.authored_at_ms.get(),
                entry.accepted_at_ms.get(),
            ),
            writer: writer_of(&entry.entry.node_public_key, &entry.entry.device_id)?,
            sequence: entry.entry.writer_seq.get(),
        })),
        _ => Err(LogError::UnexpectedOutcome),
    }
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    let text = value?.as_str()?;
    DecimalU64::try_from(text.to_owned())
        .ok()
        .map(DecimalU64::get)
}

fn stored_entry(database_id: &str, wire: StoredWire) -> Result<StoredEntry, LogError> {
    if wire.entry.database_id != database_id {
        return Err(LogError::UnexpectedOutcome);
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(wire.entry.payload_b64)
        .map_err(|_| LogError::InvalidEntry)?;
    Ok(StoredEntry {
        delivery_seq: wire.delivery_seq.get(),
        accepted_at_ms: wire.accepted_at_ms.get(),
        entry: Entry {
            database_id: mutation_log::DatabaseId::new(database_id)?,
            writer: writer_of(&wire.entry.node_public_key, &wire.entry.device_id)?,
            writer_seq: wire.entry.writer_seq.get(),
            authored_at_ms: wire.entry.authored_at_ms.get(),
            namespace: mutation_log::Namespace::new(&wire.entry.namespace)?,
            incorporated_from: wire
                .entry
                .incorporated_from
                .as_ref()
                .map(incorporated)
                .transpose()?,
            payload,
        },
    })
}

fn incorporated(body: &IncorporatedBody) -> Result<Incorporation, LogError> {
    Ok(Incorporation {
        writer: writer_of(&body.node_public_key, &body.device_id)?,
        writer_seq: body.writer_seq.get(),
    })
}

fn writer_of(key: &str, device: &str) -> Result<Writer, LogError> {
    Ok(Writer {
        key: mutation_log::PrincipalId::new(key)?,
        device: mutation_log::DeviceId::new(device)?,
    })
}

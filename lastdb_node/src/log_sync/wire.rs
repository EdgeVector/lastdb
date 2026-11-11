//! JSON this client sends on the log routes. Integers are decimal strings.

use cloud_backup_protocol::{DecimalU64, Id};
use mutation_log::{
    AppendBatch, AppendOutcome, Capability, Entry, Incorporation, ListPage, LogError, Snapshot,
    SnapshotRef, StoredEntry, Writer,
};
use serde::{Deserialize, Serialize};

use super::paths;

#[derive(Serialize)]
pub(crate) struct WriterWire<'a> {
    pub node_public_key: &'a str,
    pub device_id: &'a str,
}

#[derive(Serialize)]
pub(crate) struct EntryWire<'a> {
    pub database_id: &'a str,
    pub writer: WriterWire<'a>,
    pub writer_seq: DecimalU64,
    pub authored_at_ms: DecimalU64,
    pub namespace: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incorporated_from: Option<IncorporationWire<'a>>,
    pub payload_b64: String,
}

#[derive(Serialize)]
pub(crate) struct IncorporationWire<'a> {
    pub node_public_key: &'a str,
    pub device_id: &'a str,
    pub writer_seq: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct AppendRequest<'a> {
    pub database_id: &'a str,
    pub writer: WriterWire<'a>,
    pub accepted_at_ms: DecimalU64,
    pub entries: Vec<EntryWire<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct AppendResponse {
    pub result: String,
    #[serde(default)]
    pub delivery_seqs: Vec<DecimalU64>,
    pub next_seq: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct ListRequest<'a> {
    pub database_id: &'a str,
    pub reader: &'a str,
    pub after: DecimalU64,
    pub limit: DecimalU64,
    pub now_ms: DecimalU64,
}

#[derive(Deserialize)]
pub(crate) struct PageResponse {
    pub entries: Vec<StoredWire>,
    pub next: DecimalU64,
    #[serde(default)]
    pub reset: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct StoredWire {
    pub delivery_seq: DecimalU64,
    pub accepted_at_ms: DecimalU64,
    pub entry: EntryBody,
}

#[derive(Deserialize)]
pub(crate) struct EntryBody {
    pub database_id: String,
    pub node_public_key: String,
    pub device_id: String,
    pub writer_seq: DecimalU64,
    pub authored_at_ms: DecimalU64,
    pub namespace: String,
    #[serde(default)]
    pub incorporated_from: Option<IncorporatedBody>,
    pub payload_b64: String,
}

#[derive(Deserialize)]
pub(crate) struct IncorporatedBody {
    pub node_public_key: String,
    pub device_id: String,
    pub writer_seq: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct AckRequest<'a> {
    pub database_id: &'a str,
    pub node_public_key: &'a str,
    pub device_id: &'a str,
    pub through: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct TipRequest<'a> {
    pub database_id: &'a str,
    pub reader: &'a str,
    pub node_public_key: &'a str,
    pub device_id: &'a str,
}

#[derive(Deserialize)]
pub(crate) struct TipResponse {
    pub tip: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct GrantRequest<'a> {
    pub database_id: &'a str,
    pub caller: &'a str,
    pub principal: &'a str,
    pub capability: &'a str,
}

#[derive(Serialize)]
pub(crate) struct RejectRequest<'a> {
    pub database_id: &'a str,
    pub caller: &'a str,
    pub node_public_key: &'a str,
    pub device_id: &'a str,
    pub writer_seq: DecimalU64,
}

#[derive(Serialize)]
pub(crate) struct SnapshotRequest<'a> {
    pub database_id: &'a str,
    pub caller: &'a str,
    pub at_ms: DecimalU64,
    pub snapshot_ref: &'a str,
}

#[derive(Deserialize)]
pub(crate) struct SnapshotResponse {
    pub through_delivery_seq: DecimalU64,
    pub at_ms: DecimalU64,
    pub snapshot_ref: String,
}

#[derive(Serialize)]
pub(crate) struct SnapshotBytesRequest<'a> {
    pub database_id: &'a str,
    pub snapshot_ref: &'a str,
}

pub(crate) fn append_request(batch: &AppendBatch) -> Result<Vec<u8>, LogError> {
    let database_id = batch
        .entries
        .first()
        .map(|entry| entry.database_id.as_str())
        .ok_or(LogError::EmptyBatch)?;
    let request = AppendRequest {
        database_id,
        writer: writer_wire(&batch.writer),
        accepted_at_ms: DecimalU64::new(batch.accepted_at_ms),
        entries: batch
            .entries
            .iter()
            .map(entry_wire)
            .collect::<Result<Vec<_>, _>>()?,
    };
    cloud_backup_protocol::canonical_bytes(&request).map_err(|_| LogError::InvalidEntry)
}

fn entry_wire(entry: &Entry) -> Result<EntryWire<'_>, LogError> {
    Ok(EntryWire {
        database_id: entry.database_id.as_str(),
        writer: writer_wire(&entry.writer),
        writer_seq: DecimalU64::new(entry.writer_seq),
        authored_at_ms: DecimalU64::new(entry.authored_at_ms),
        namespace: entry.namespace.as_str(),
        incorporated_from: entry.incorporated_from.as_ref().map(incorporation_wire),
        payload_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&entry.payload),
    })
}

fn writer_wire(writer: &Writer) -> WriterWire<'_> {
    WriterWire {
        node_public_key: writer.key.as_str(),
        device_id: writer.device.as_str(),
    }
}

fn incorporation_wire(value: &Incorporation) -> IncorporationWire<'_> {
    IncorporationWire {
        node_public_key: value.writer.key.as_str(),
        device_id: value.writer.device.as_str(),
        writer_seq: DecimalU64::new(value.writer_seq),
    }
}

pub(crate) fn append_outcome(body: &[u8]) -> Result<AppendOutcome, LogError> {
    let response: AppendResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    match response.result.as_str() {
        "accepted" => Ok(AppendOutcome::Accepted {
            delivery_seqs: response
                .delivery_seqs
                .into_iter()
                .map(|value| value.get())
                .collect(),
        }),
        "duplicate" => Ok(AppendOutcome::Duplicate {
            next_seq: response.next_seq.get(),
        }),
        _ => Err(LogError::UnexpectedOutcome),
    }
}

pub(crate) fn list_page(body: &[u8]) -> Result<ListPage, LogError> {
    let page = page_response(body)?;
    if page.reset.is_some() {
        return Err(LogError::UnexpectedOutcome);
    }
    Ok(ListPage {
        entries: stored_entries(page.entries)?,
        next: page.next.get(),
    })
}

pub(crate) fn snapshot(body: &[u8]) -> Result<Snapshot, LogError> {
    let response: SnapshotResponse =
        serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)?;
    Ok(Snapshot {
        through_delivery_seq: response.through_delivery_seq.get(),
        at_ms: response.at_ms.get(),
        snapshot_ref: SnapshotRef::new(&response.snapshot_ref)?,
    })
}

pub(crate) fn service_error(body: &[u8]) -> Option<LogError> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = value
        .get("code")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    let field = value
        .get("field")
        .and_then(|item| item.as_str())
        .unwrap_or("");
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

fn json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    value.as_str()?.parse().ok()
}

pub(crate) fn capability_name(capability: &Capability) -> &'static str {
    match capability {
        Capability::Owner => "owner",
        Capability::Receive => "receive",
        Capability::Submit => "submit",
    }
}

pub(crate) fn database_wire(value: &str) -> Result<Id, LogError> {
    Id::try_from(value.to_owned()).map_err(|_| LogError::InvalidId)
}

fn page_response(body: &[u8]) -> Result<PageResponse, LogError> {
    serde_json::from_slice(body).map_err(|_| LogError::UnexpectedOutcome)
}

fn stored_entries(entries: Vec<StoredWire>) -> Result<Vec<StoredEntry>, LogError> {
    entries.into_iter().map(stored_entry).collect()
}

fn stored_entry(wire: StoredWire) -> Result<StoredEntry, LogError> {
    Ok(StoredEntry {
        delivery_seq: wire.delivery_seq.get(),
        accepted_at_ms: wire.accepted_at_ms.get(),
        entry: entry_body(wire.entry)?,
    })
}

fn entry_body(body: EntryBody) -> Result<Entry, LogError> {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body.payload_b64)
        .map_err(|_| LogError::InvalidEntry)?;
    Ok(Entry {
        database_id: mutation_log::DatabaseId::new(&body.database_id)?,
        writer: writer_of(&body.node_public_key, &body.device_id)?,
        writer_seq: body.writer_seq.get(),
        authored_at_ms: body.authored_at_ms.get(),
        namespace: mutation_log::Namespace::new(&body.namespace)?,
        incorporated_from: body
            .incorporated_from
            .map(|value| incorporated(value))
            .transpose()?,
        payload,
    })
}

fn incorporated(body: IncorporatedBody) -> Result<Incorporation, LogError> {
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

pub(crate) fn operation(path: &str) -> &'static str {
    match path {
        paths::CREATE => "create-database",
        paths::APPEND => "append",
        paths::HEAD => "head",
        paths::LIST => "list",
        paths::ACK => "ack",
        paths::GRANT => "grant",
        paths::REVOKE => "revoke",
        paths::SNAPSHOT => "snapshot",
        paths::WRITER_TIP => "writer-tip",
        paths::REJECT => "reject",
        paths::SNAPSHOT_BYTES => "snapshot-bytes",
        _ => "log",
    }
}

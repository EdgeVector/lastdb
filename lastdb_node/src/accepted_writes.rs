//! Publication eligibility over one durable accepted stream.
//!
//! The storage prefix is sealed and complete. This reader opens each
//! envelope with the database content key and applies share policy.
//! One refusal returns no items, so the cursor cannot pass that entry.

use database_policy::{admit_namespace, record_is_shareable, NamespaceClass, SpecBinding};
use fold_db::crypto::decrypt_envelope;
use fold_db::hex::hex_lower;
use fold_db::storage::disk_backup::accepted::{self, Error as StreamError};
use fold_db::storage::disk_pack::PackRuntime;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Which database this read is publishing.
#[derive(Clone, Copy)]
pub enum Publication<'a> {
    /// Every sealed entry is publishable.
    Personal,
    /// Only entries the binding admits. The rest are withheld.
    Share(&'a SpecBinding),
}

/// One logical entry after eligibility.
pub enum Item {
    /// Canonical plaintext and any sealed file row.
    Publish {
        sequence: u64,
        mutation_id: String,
        kind: String,
        canonical: Vec<u8>,
        file_ciphertext: Option<Vec<u8>>,
        /// Custody path recorded for this entry. Absent when the sealed row is present.
        file_reference: Option<String>,
    },
    /// The sequence is occupied. It carries no plaintext and no file.
    Withheld { sequence: u64, mutation_id: String },
}

/// One exclusive page after eligibility.
pub struct Page {
    /// Last sequence in this page, or `after` when the page is empty.
    pub through: u64,
    /// Writes from before the stream still need a snapshot.
    pub snapshot_required: bool,
    /// Entries with sequence `after + 1` through `through`.
    pub items: Vec<Item>,
}

/// Cursor of one database stream.
pub struct StreamStatus {
    /// Last durable sequence. Zero means no entry.
    pub head: u64,
    /// Highest acknowledged sequence.
    pub acked: u64,
    /// Native frontier when the stream started.
    pub opened_frontier: u64,
    /// Writes before the stream are not in this log.
    pub snapshot_required: bool,
    /// A hole is open. Reads and new commits refuse.
    pub gap: bool,
}

/// The page cannot be published. Do not skip the entry and do not acknowledge it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Blocked {
    /// An entry is missing or unreadable.
    Gap,
    /// Share policy cannot classify the entry.
    Unclassified,
    /// The envelope or canonical body is not usable.
    Envelope,
    /// `after` is past the durable head.
    Ahead,
    /// `after` is behind the acknowledged cursor.
    Behind,
    /// The page limit is empty or above 90.
    Limit,
    /// A snapshot still has to cover writes from before the stream.
    Snapshot,
    /// The disk call failed.
    Storage,
}

/// Open envelopes and classify one page. A refusal returns no items.
pub fn prefix(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    publication: Publication<'_>,
    after: u64,
    limit: u32,
) -> Result<Page, Blocked> {
    let sealed = runtime.accepted_prefix(after, limit).map_err(blocked)?;
    let mut items = Vec::with_capacity(sealed.items.len());
    for entry in sealed.items {
        items.push(classify(content_key, &publication, entry)?);
    }
    Ok(Page {
        through: sealed.through,
        snapshot_required: sealed.snapshot_required,
        items,
    })
}

/// Record that every entry through `through` was published or withheld.
pub fn acknowledge(runtime: &PackRuntime, through: u64) -> Result<(), Blocked> {
    runtime.acknowledge_accepted(through).map_err(blocked)
}

/// Record that a snapshot covers the native frontier from before the stream.
pub fn note_snapshot(runtime: &PackRuntime, covered_frontier: u64) -> Result<(), Blocked> {
    runtime
        .note_accepted_snapshot(covered_frontier)
        .map_err(blocked)
}

/// Cursor and coverage. This does not read entry bodies.
pub fn status(runtime: &PackRuntime) -> StreamStatus {
    let status = runtime.accepted_status();
    StreamStatus {
        head: status.head,
        acked: status.acked,
        opened_frontier: status.opened_frontier,
        snapshot_required: status.snapshot_required,
        gap: status.gap,
    }
}

fn classify(
    content_key: &[u8; 32],
    publication: &Publication<'_>,
    entry: accepted::Entry,
) -> Result<Item, Blocked> {
    let (plain, body) = open_body(content_key, &entry)?;
    let kind = body
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(Blocked::Envelope)?;
    let publish = match publication {
        Publication::Personal => true,
        Publication::Share(binding) if kind == "schema" => share_schema(binding, &body)?,
        Publication::Share(binding) => {
            share_record(binding, &body, entry.file_ciphertext.is_some())?
        }
    };
    if !publish {
        return Ok(Item::Withheld {
            sequence: entry.sequence,
            mutation_id: entry.mutation_id,
        });
    }
    Ok(Item::Publish {
        sequence: entry.sequence,
        mutation_id: entry.mutation_id,
        kind: kind.to_string(),
        canonical: plain,
        file_ciphertext: entry.file_ciphertext,
        file_reference: entry.file_reference,
    })
}

fn open_body(content_key: &[u8; 32], entry: &accepted::Entry) -> Result<(Vec<u8>, Value), Blocked> {
    let plain = decrypt_envelope(content_key, &entry.envelope).map_err(|_| Blocked::Envelope)?;
    let body: Value = serde_json::from_slice(&plain).map_err(|_| Blocked::Envelope)?;
    if body.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(Blocked::Envelope);
    }
    if body.get("mutation_id").and_then(Value::as_str) != Some(entry.mutation_id.as_str()) {
        return Err(Blocked::Envelope);
    }
    let kind = body
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(Blocked::Envelope)?;
    if !matches!(
        kind,
        "record" | "file" | "inline" | "schema" | "contribution"
    ) {
        return Err(Blocked::Envelope);
    }
    if !file_hash_ok(&body, entry.file_ciphertext.as_deref()) {
        return Err(Blocked::Envelope);
    }
    Ok((plain, body))
}

fn share_schema(binding: &SpecBinding, body: &Value) -> Result<bool, Blocked> {
    let namespace = namespace_of(
        body.get("schema")
            .and_then(Value::as_str)
            .ok_or(Blocked::Unclassified)?,
    )?;
    match admit_namespace(Some(binding), namespace) {
        Ok(NamespaceClass::Official) => Ok(true),
        Ok(NamespaceClass::AppPrivate) => Ok(false),
        Err(_) => Err(Blocked::Unclassified),
    }
}

fn share_record(binding: &SpecBinding, body: &Value, file_bytes: bool) -> Result<bool, Blocked> {
    let schema = body
        .get("schema")
        .and_then(Value::as_str)
        .ok_or(Blocked::Unclassified)?;
    let (namespace, name) = split_schema(schema)?;
    let fields = field_names(body);
    let file = file_bytes || body.get("file").is_some() || body.get("inline_file").is_some();
    match record_is_shareable(binding, namespace, name, &fields, file) {
        Ok(allowed) => Ok(allowed),
        Err(_) => Err(Blocked::Unclassified),
    }
}

fn namespace_of(schema: &str) -> Result<&str, Blocked> {
    Ok(split_schema(schema)?.0)
}

fn split_schema(schema: &str) -> Result<(&str, &str), Blocked> {
    let (namespace, name) = schema.split_once('/').ok_or(Blocked::Unclassified)?;
    if namespace.is_empty() || name.is_empty() {
        return Err(Blocked::Unclassified);
    }
    Ok((namespace, name))
}

fn field_names(body: &Value) -> Vec<&str> {
    body.get("fields")
        .and_then(Value::as_object)
        .map(|fields| fields.keys().map(String::as_str).collect())
        .unwrap_or_default()
}

fn file_hash_ok(body: &Value, file: Option<&[u8]>) -> bool {
    let expected = body
        .get("file")
        .and_then(|section| section.get("sealed_sha256"))
        .and_then(Value::as_str);
    match (expected, file) {
        (None, None) => true,
        (Some(expected), Some(bytes)) => expected == hex_lower(Sha256::digest(bytes)),
        _ => false,
    }
}

fn blocked(error: StreamError) -> Blocked {
    match error {
        StreamError::Gap => Blocked::Gap,
        StreamError::Ahead => Blocked::Ahead,
        StreamError::Behind => Blocked::Behind,
        StreamError::Limit => Blocked::Limit,
        StreamError::Snapshot => Blocked::Snapshot,
        StreamError::Unavailable | StreamError::Conflict | StreamError::Spool => Blocked::Storage,
    }
}

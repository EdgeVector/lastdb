//! HTTP client for `/relay/v1/log/*`.

mod http;

use std::time::{Duration, Instant};

use app_identity_crypto::SigningKey;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use serde::Deserialize;
use serde_json::{json, Value};

/// Bound a hung peer so the sync loop can return and re-push the log.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A blob transfer fails when it makes no progress for this long, however slow the link.
pub(super) const STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// CI probe budget: request timeout plus a little slack.
const HUNG_PEER_BUDGET: Duration = Duration::from_secs(45);

/// Starts the error of a read of entries that the relay dropped after a snapshot.
pub(super) const BEHIND_THE_TRIM: &str = "this device is behind history that the relay dropped after a snapshot; restore the database into a new home with `lastdb backup restore`";

/// One device stream of a writer as `log.streams` lists it.
#[derive(Debug, Deserialize)]
pub(super) struct StreamHead {
    pub(super) device_id: String,
    pub(super) head_seq: u64,
    pub(super) head_hash: String,
}

/// One page of a `log.read`.
pub(super) struct Page {
    /// The entries in stream order, those of a segment in its place.
    pub(super) entries: Vec<SignedEntry>,
    pub(super) cursor: Option<String>,
    /// For each entry, whether a segment held it (its bytes are the segment blob's).
    pub(super) in_segment: Vec<bool>,
    /// The digests of the segment blobs that held entries of this page.
    pub(super) segments: Vec<String>,
}

/// A log segment as `log.read` names it.
#[derive(Deserialize)]
struct SegmentRef {
    node_key: String,
    device_id: String,
    first_seq: u64,
    last_seq: u64,
    last_hash: String,
    sha256: String,
    bytes: u64,
    url: String,
    #[serde(default)]
    headers: Value,
}

pub(super) struct RelayClient {
    pub(super) base: String,
    pub(super) signing_key: SigningKey,
    pub(super) node_key_hex: String,
    pub(super) device_id: String,
    pub(super) http: reqwest::Client,
    /// Presigned upload client: no total/read timeout (watched by `blob_transfer`), no idle pool.
    pub(super) transfer: reqwest::Client,
    /// Presigned download client: [`STALL_TIMEOUT`] per read, no idle pool.
    pub(super) download: reqwest::Client,
    /// The account of what this device put, for a client that syncs a home. A client that only
    /// reads has none.
    pub(super) usage: Option<super::usage::Ledger>,
}

impl RelayClient {
    pub(super) fn new(
        base: &str,
        signing_key: SigningKey,
        node_key_hex: String,
        device_id: &mutation_log::DeviceId,
    ) -> Result<Self, String> {
        // trace-egress: public — relay gates by grants; node key only in signed headers.
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let transfer = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            // A fresh connection for each upload: a body cannot be sent again on a pooled
            // connection that the store closed while it was idle ("connection closed before
            // message completed").
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let download = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(STALL_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex,
            device_id: device_id.to_string(),
            http,
            transfer,
            download,
            usage: None,
        })
    }

    /// A client for the relay at `base` with this node's keys and device id.
    pub(super) fn for_host(host: &crate::host::Host, base: &str) -> Result<Self, String> {
        let signing_key = SigningKey::from_bytes(&host.keypair.secret_key_bytes());
        let node_key_hex =
            mutation_log::NodeKey::from_bytes(host.keypair.public_key_bytes()).to_hex();
        let device_id = super::device::device_id(&host.home)?;
        let mut client = Self::new(base, signing_key, node_key_hex, &device_id)?;
        let spool_acked = host.db.db_ops().native_runtime().accepted_status().acked;
        client.usage = Some(super::usage::Ledger::new(&host.home, spool_acked));
        Ok(client)
    }

    pub(super) async fn append(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<(u64, u64), String> {
        let body = json!({
            "database_id": database_id,
            "entries": entries,
        });
        let value = self
            .post("/relay/v1/log/append", database_id, &body)
            .await?;
        let (stored, duplicate) = parse_append_counts(&value, entries.len() as u64);
        if let Some(usage) = &self.usage {
            usage.entries_accepted(
                database_id,
                entries,
                stored.saturating_add(duplicate),
                false,
            )?;
        }
        Ok((stored, duplicate))
    }

    /// Append a log segment of `entries`, consecutive entries of this node's stream, whose bytes
    /// the caller put on the relay as the blob the segment names (`relay-protocol.md`, "Log
    /// segments"). Answers `(stored, duplicate)` in entries, like [`Self::append`].
    pub(super) async fn append_segment(
        &self,
        database_id: &str,
        segment: &Value,
        entries: &[&SignedEntry],
    ) -> Result<(u64, u64), String> {
        let body = json!({ "database_id": database_id, "segment": segment });
        let value = self
            .post("/relay/v1/log/append", database_id, &body)
            .await?;
        let (stored, duplicate) = parse_append_counts(&value, entries.len() as u64);
        if let Some(usage) = &self.usage {
            usage.entries_accepted(database_id, entries, stored.saturating_add(duplicate), true)?;
        }
        Ok((stored, duplicate))
    }

    /// Read one writer's entries. Omit `device_id` to receive that writer's
    /// entries across every device; the response cursor carries the device.
    pub(super) async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: Option<&str>,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        let page = self
            .read_page(database_id, writer_key, device_id, from_seq, cursor, limit)
            .await?;
        Ok((page.entries, page.cursor))
    }

    /// [`Self::read`], with the digests of the log segments that held the entries. A segment
    /// is fetched and opened here, so a caller sees the entries it holds as if they were stored
    /// one by one. On a read of one device from `from_seq`, the entries of the first segment
    /// below `from_seq` are left out.
    pub(super) async fn read_page(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: Option<&str>,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Page, String> {
        // OpenAPI LogReadBody names the stream key `writer_key` (hex node key).
        let mut body = json!({
            "database_id": database_id,
            "limit": limit,
        });
        if let Some(cursor) = cursor {
            body["cursor"] = Value::String(cursor.to_owned());
        } else {
            body["writer_key"] = Value::String(writer_key.to_owned());
            body["from_seq"] = json!(from_seq);
            if let Some(device_id) = device_id {
                body["device_id"] = Value::String(device_id.to_owned());
            }
        }
        let value = self
            .post("/relay/v1/log/read", database_id, &body)
            .await
            .map_err(|error| {
                if error.contains("HTTP 410") {
                    format!("{BEHIND_THE_TRIM}: {error}")
                } else {
                    error
                }
            })?;
        const BAD: &str = "relay log.read returned invalid entries";
        let items = value["entries"].as_array().ok_or(BAD)?;
        let skip_below = (cursor.is_none() && device_id.is_some()).then_some(from_seq);
        let mut page = Page {
            entries: Vec::with_capacity(items.len()),
            cursor: value["cursor"].as_str().map(str::to_owned),
            in_segment: Vec::with_capacity(items.len()),
            segments: Vec::new(),
        };
        for (index, item) in items.iter().enumerate() {
            let Some(segment) = item.get("segment") else {
                page.entries
                    .push(serde_json::from_value(item.clone()).map_err(|_| BAD.to_owned())?);
                page.in_segment.push(false);
                continue;
            };
            let segment: SegmentRef =
                serde_json::from_value(segment.clone()).map_err(|_| BAD.to_owned())?;
            let entries = self.open_segment(&segment).await?;
            let from = if index == 0 {
                skip_below.unwrap_or(0)
            } else {
                0
            };
            for entry in entries {
                if entry.header().writer_seq >= from {
                    page.entries.push(entry);
                    page.in_segment.push(true);
                }
            }
            page.segments.push(segment.sha256);
        }
        Ok(page)
    }

    /// The entries of a log segment, after a check that the blob holds exactly the entries the
    /// relay named: that stream, `first_seq` through `last_seq` in order, the last with hash
    /// `last_hash`. The signatures and the chain are the caller's to check, as for any entry.
    async fn open_segment(&self, segment: &SegmentRef) -> Result<Vec<SignedEntry>, String> {
        let bytes = self
            .fetch_bytes(
                &segment.url,
                &segment.headers,
                &segment.sha256,
                segment.bytes,
            )
            .await?;
        let bad = |what: &str| format!("log segment {}: {what}", segment.sha256);
        if segment.last_seq < segment.first_seq || bytes.last() != Some(&b'\n') {
            return Err(bad("not a list of entries"));
        }
        let mut entries = Vec::new();
        for (offset, line) in bytes[..bytes.len() - 1]
            .split(|byte| *byte == b'\n')
            .enumerate()
        {
            let entry: SignedEntry =
                serde_json::from_slice(line).map_err(|_| bad("holds an invalid entry"))?;
            let header = entry.header();
            if header.node_key.to_hex() != segment.node_key
                || header.device_id.to_hex() != segment.device_id
                || Some(header.writer_seq) != segment.first_seq.checked_add(offset as u64)
            {
                return Err(bad("holds another entry than the relay named"));
            }
            entries.push(entry);
        }
        let last = entries.last().ok_or_else(|| bad("is empty"))?;
        if last.header().writer_seq != segment.last_seq
            || mutation_log::entry_hash(last)
                .map(|hash| hash.to_hex())
                .ok()
                != Some(segment.last_hash.clone())
        {
            return Err(bad("does not end at the entry the relay named"));
        }
        Ok(entries)
    }

    /// Ask the relay to drop what the snapshot entry `writer_seq` of the snapshot stream
    /// `device_id` covers, and the snapshot entries and files older than it. The relay reads
    /// that entry itself. `true` when it has nothing more to drop, `false` when it stopped
    /// at its work limit for one call and a repeat continues.
    pub(super) async fn trim(
        &self,
        database_id: &str,
        device_id: &str,
        writer_seq: u64,
    ) -> Result<bool, String> {
        let body = json!({
            "database_id": database_id,
            "device_id": device_id,
            "writer_seq": writer_seq,
        });
        let value = self.post("/relay/v1/log/trim", database_id, &body).await?;
        if let Some(usage) = &self.usage {
            usage.released(database_id, &value)?;
        }
        Ok(value["done"].as_bool().unwrap_or(false))
    }

    /// The device streams of `writer_key` and how far each reaches: for each,
    /// the contiguous head (the highest `writer_seq` such that every seq from 1
    /// up to it is on the relay) and the hash of that entry. A writer with no
    /// stream gives an empty list. Access is the access of an all-devices
    /// `log.read` of the same writer.
    pub(super) async fn streams(
        &self,
        database_id: &str,
        writer_key: &str,
    ) -> Result<Vec<StreamHead>, String> {
        let body = json!({ "database_id": database_id, "writer_key": writer_key });
        let value = self
            .post("/relay/v1/log/streams", database_id, &body)
            .await?;
        serde_json::from_value(value["streams"].clone())
            .map_err(|_| "relay log.streams returned an invalid list".to_owned())
    }

    pub(super) async fn post(
        &self,
        path: &str,
        database_id: &str,
        body: &Value,
    ) -> Result<Value, String> {
        self.post_tracked(path, database_id, body)
            .await
            .map(|(value, _)| value)
    }
}

fn parse_append_counts(value: &Value, sent: u64) -> (u64, u64) {
    let stored = value["stored"].as_u64().unwrap_or(0);
    if stored >= sent {
        (stored, 0)
    } else if value["duplicate"].as_bool() == Some(true) {
        (stored, sent.saturating_sub(stored))
    } else {
        (stored, value["duplicate"].as_u64().unwrap_or(0))
    }
}

/// The name and value of each string in a JSON object of headers.
pub(super) fn header_pairs(value: &Value) -> Vec<(String, String)> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(name, val)| val.as_str().map(|v| (name.clone(), v.to_owned())))
        .collect()
}

/// CI: append against a hung `base` must error inside [`HUNG_PEER_BUDGET`].
pub async fn hung_relay_must_timeout(base: &str) -> Result<Duration, String> {
    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let node_key_hex =
        mutation_log::Hex32::from_bytes(signing_key.verifying_key().to_bytes()).to_hex();
    let device_id = DeviceId::from_bytes([3u8; 32]);
    let client = RelayClient::new(base, signing_key, node_key_hex, &device_id)?;
    let database_id = DatabaseId::from_bytes([9u8; 32]).to_hex();
    let started = Instant::now();
    match client.append(&database_id, &[]).await {
        Err(_) => {
            let elapsed = started.elapsed();
            if elapsed <= HUNG_PEER_BUDGET {
                Ok(elapsed)
            } else {
                Err(format!(
                    "hung peer error took {:.3}s, over {}s",
                    elapsed.as_secs_f64(),
                    HUNG_PEER_BUDGET.as_secs()
                ))
            }
        }
        Ok(_) => Err("hung peer accepted an empty append".into()),
    }
}

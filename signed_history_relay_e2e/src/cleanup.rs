//! Cleanup: the run deletes the blobs it put on the relay, then reads back that they are gone.
//!
//! For each database in the [`Ledger`] (only databases this run minted), as its publisher:
//!
//! 1. `blob/list` names every committed blob of the database. The database id comes from a fresh
//!    random key, so everything it holds was written by this run.
//! 2. `blob/get` of each blob gives a presigned GET of the stored object. A one-byte ranged GET
//!    must succeed now: that is the control for the read-back.
//! 3. One `snapshot` entry goes on a fresh device stream. `blob/drop` needs one to name. Its
//!    manifest digest is one the run never uploads, so no blob has to stay for it.
//! 4. `blob/drop` drops the listed blobs, at most 25 per call, naming the head of each of the
//!    publisher's streams (`log/streams`) as its fence. A 409 (a head moved) reads the heads again.
//! 5. Read-back: `blob/list` is empty, `blob/get` of each digest answers 404, and the same
//!    presigned GET, still unexpired, is refused by the object store. A store answers 404 for a
//!    missing key, or 403 when the signing credential may not list the bucket; the control in
//!    step 2 shows the URL itself was good, so either answer means the object is gone.
//!
//! Log entries stay. `log/trim` drops only sealed `data`/`schema` and `snapshot` entries of a
//! personal database, and this database holds birth, grant and bundle entries; no relay route
//! deletes those. The report counts the publisher's entries that stay. The `log.streams`
//! database (`streams.rs`) holds entries only, no blob, and is not listed here.

use reqwest::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use mutation_log::{Hex32, SnapshotBody};

use crate::chain::{Chain, Publisher};
use crate::grants::http_status;
use crate::identity::{new_device_id, now_ms};
use crate::ledger::{Ledger, OwnedDatabase};
use crate::relay::{header_pairs, RelayClient};

/// The most digests one `blob/drop` may name.
const DROP_BATCH: usize = 25;
/// Times the drop reads the heads again after a 409.
const FENCE_ATTEMPTS: u32 = 3;

/// A presigned GET of one stored object, taken and tried before the drop.
struct Presigned {
    sha256: String,
    url: String,
    headers: Vec<(String, String)>,
    expires_at_ms: Option<u64>,
}

/// The counts that the `blob/drop` answers add up to.
#[derive(Default)]
struct DropCounts {
    dropped: u64,
    released: u64,
    absent: u64,
    calls: u64,
}

/// Clean every database in `ledger`. The report has `ok`, `error`, totals and one object per
/// database. A failure is reported, never swallowed: `ok` is false and `error` says why, and the
/// counts of what was dropped before it stay in the report.
pub async fn run(http: &reqwest::Client, relay_url: &str, ledger: &Ledger) -> Value {
    let mut databases = Vec::new();
    let mut errors = Vec::new();
    let (mut dropped, mut released) = (0u64, 0u64);
    for owned in ledger.take() {
        let mut report = json!({
            "database_id": owned.database_id.to_hex(),
            "blobs_recorded": owned.blobs.len(),
            "bytes_recorded": owned.blobs.iter().map(|(_, bytes)| bytes).sum::<u64>(),
        });
        let outcome = clean(http, relay_url, &owned, &mut report).await;
        dropped += report["blobs_dropped"].as_u64().unwrap_or(0);
        released += report["released_bytes"].as_u64().unwrap_or(0);
        report["ok"] = Value::Bool(outcome.is_ok());
        if let Err(error) = outcome {
            let database_id = owned.database_id.to_hex();
            eprintln!("cleanup of database {database_id} failed: {error}");
            errors.push(format!("database {database_id}: {error}"));
            report["error"] = Value::String(error);
        }
        databases.push(report);
    }
    let error = (!errors.is_empty()).then(|| errors.join("; "));
    json!({
        "ok": error.is_none(),
        "error": error,
        "blobs_dropped": dropped,
        "released_bytes": released,
        "databases": databases,
    })
}

/// Drop the blobs of one database and read back that they are gone, filling `report` as it goes.
async fn clean(
    http: &reqwest::Client,
    relay_url: &str,
    owned: &OwnedDatabase,
    report: &mut Value,
) -> Result<(), String> {
    let device = new_device_id();
    let device_hex = device.to_hex();
    let client = RelayClient::new(relay_url, http, &owned.publisher, owned.database_id, device);
    let listed = match list_all(&client).await {
        Ok(listed) => listed,
        // A run that stopped before its birth entry was stored committed no blob.
        Err(error) if owned.blobs.is_empty() => {
            report["blobs_listed"] = json!(0);
            report["note"] = json!(format!(
                "blob/list refused and the run committed no blob: {error}"
            ));
            return Ok(());
        }
        Err(error) => return Err(format!("blob/list: {error}")),
    };
    report["blobs_listed"] = json!(listed.len());
    report["bytes_listed"] = json!(listed.iter().map(|(_, bytes)| bytes).sum::<u64>());
    let mut digests: Vec<String> = listed.iter().map(|(sha256, _)| sha256.clone()).collect();
    for (sha256, _) in &owned.blobs {
        if !digests.contains(sha256) {
            digests.push(sha256.clone());
        }
    }
    let mut presigned = Vec::with_capacity(listed.len());
    for (sha256, _) in &listed {
        presigned.push(presign(&client, sha256).await?);
    }

    if !listed.is_empty() {
        let publisher = Publisher {
            actor: owned.publisher.clone(),
            database_id: owned.database_id,
        };
        let manifest: [u8; 32] = Sha256::digest(format!(
            "signed_history_relay_e2e cleanup manifest, never uploaded, {}",
            client.database_hex
        ))
        .into();
        let body = SnapshotBody {
            manifest_sha256: Hex32::from_bytes(manifest),
            manifest_bytes: 0,
            through: Vec::new(),
        };
        let snapshot = publisher.snapshot(&mut Chain::new(device), body)?;
        client
            .append(std::slice::from_ref(&snapshot))
            .await
            .map_err(|e| format!("snapshot append: {e}"))?;
        let mut counts = DropCounts::default();
        // The first `listed.len()` digests are the listed ones.
        let dropping = drop_all(
            &client,
            &owned.publisher.node_key_hex(),
            &device_hex,
            &digests[..listed.len()],
            &mut counts,
        )
        .await;
        report["blobs_dropped"] = json!(counts.dropped);
        report["released_bytes"] = json!(counts.released);
        report["absent"] = json!(counts.absent);
        report["drop_calls"] = json!(counts.calls);
        dropping?;
    }

    report["readback"] = read_back(&client, &digests, &presigned).await?;
    let kept = client
        .streams(&owned.publisher.node_key_hex())
        .await
        .map(|streams| streams.iter().map(|s| s.head_seq).sum::<u64>())
        .map_err(|e| format!("log/streams after the drop: {e}"))?;
    report["publisher_log_entries_kept"] = json!(kept);
    Ok(())
}

async fn drop_all(
    client: &RelayClient,
    publisher_hex: &str,
    snapshot_device: &str,
    digests: &[String],
    counts: &mut DropCounts,
) -> Result<(), String> {
    let mut left = digests;
    let mut attempt = 1;
    'fence: loop {
        let streams = client
            .streams(publisher_hex)
            .await
            .map_err(|e| format!("log/streams: {e}"))?;
        let snapshot_seq = streams
            .iter()
            .find(|s| s.device_id == snapshot_device)
            .map(|s| s.head_seq)
            .ok_or("log/streams does not list the cleanup snapshot stream")?;
        let heads: Vec<Value> = streams
            .iter()
            .filter(|s| s.device_id != snapshot_device)
            .map(|s| json!({ "device_id": s.device_id, "writer_seq": s.head_seq }))
            .collect();
        while !left.is_empty() {
            let batch = &left[..left.len().min(DROP_BATCH)];
            let body = json!({
                "database_id": client.database_hex,
                "snapshot": { "device_id": snapshot_device, "writer_seq": snapshot_seq },
                "heads": heads,
                "sha256": batch,
            });
            counts.calls += 1;
            match client.post("/relay/v1/blob/drop", &body).await {
                Ok(answer) => {
                    let count = |field: &str| {
                        answer[field]
                            .as_u64()
                            .ok_or_else(|| format!("blob/drop answer has no {field}: {answer}"))
                    };
                    counts.dropped += count("dropped")?;
                    counts.released += count("released_bytes")?;
                    counts.absent += count("absent")?;
                    left = &left[batch.len()..];
                }
                Err(error) if http_status(&error) == Some(409) && attempt < FENCE_ATTEMPTS => {
                    eprintln!("cleanup: blob/drop answered 409, reading the heads again: {error}");
                    attempt += 1;
                    continue 'fence;
                }
                Err(error) => return Err(format!("blob/drop: {error}")),
            }
        }
        return Ok(());
    }
}

/// Every committed blob of the client's database: `(sha256, bytes)`, all pages.
async fn list_all(client: &RelayClient) -> Result<Vec<(String, u64)>, String> {
    let mut found = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut body = json!({ "database_id": client.database_hex });
        if let Some(after) = &after {
            body["after"] = Value::String(after.clone());
        }
        let page = client.post("/relay/v1/blob/list", &body).await?;
        for blob in page["blobs"]
            .as_array()
            .ok_or("blob/list has no blobs list")?
        {
            let sha256 = blob["sha256"]
                .as_str()
                .ok_or("blob/list item has no sha256")?;
            let bytes = blob["bytes"]
                .as_u64()
                .ok_or("blob/list item has no bytes")?;
            found.push((sha256.to_owned(), bytes));
        }
        match page["next"].as_str() {
            Some(next) => after = Some(next.to_owned()),
            None => return Ok(found),
        }
    }
}

/// A presigned GET of the object of `sha256`, and the proof that it works now.
async fn presign(client: &RelayClient, sha256: &str) -> Result<Presigned, String> {
    let body = json!({ "database_id": client.database_hex, "sha256": sha256 });
    let meta = client
        .post("/relay/v1/blob/get", &body)
        .await
        .map_err(|e| format!("blob/get before the drop: {e}"))?;
    let url = meta["url"]
        .as_str()
        .ok_or("blob/get has no url")?
        .to_owned();
    let presigned = Presigned {
        sha256: sha256.to_owned(),
        url,
        headers: header_pairs(&meta["headers"]),
        expires_at_ms: meta["expires_at_ms"].as_u64(),
    };
    let (status, _) = first_byte(client, &presigned).await?;
    if !status.is_success() {
        return Err(format!(
            "object GET of {sha256} before the drop answered {status}, expected 200 or 206"
        ));
    }
    Ok(presigned)
}

/// A one-byte ranged GET of a presigned object URL: its status and the store's error code.
async fn first_byte(
    client: &RelayClient,
    object: &Presigned,
) -> Result<(StatusCode, String), String> {
    let mut req = client.http.get(&object.url).header("range", "bytes=0-0");
    for (name, value) in &object.headers {
        req = req.header(name.as_str(), value.as_str());
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("object GET of {}: {e}", object.sha256))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let code = text
        .split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .map_or_else(String::new, |(code, _)| code.to_owned());
    Ok((status, code))
}

/// Prove the blobs are gone: none listed, `blob/get` 404 for each digest, and each presigned GET
/// that worked before the drop now refused by the object store (404, or 403 without list rights).
async fn read_back(
    client: &RelayClient,
    digests: &[String],
    presigned: &[Presigned],
) -> Result<Value, String> {
    let after = list_all(client)
        .await
        .map_err(|e| format!("blob/list after the drop: {e}"))?;
    if !after.is_empty() {
        let names: Vec<&str> = after.iter().map(|(sha256, _)| sha256.as_str()).collect();
        return Err(format!(
            "blob/list still lists {} blobs: {names:?}",
            after.len()
        ));
    }
    for sha256 in digests {
        let body = json!({ "database_id": client.database_hex, "sha256": sha256 });
        match client.post("/relay/v1/blob/get", &body).await {
            Err(error) if http_status(&error) == Some(404) => {}
            Err(error) => return Err(format!("blob/get {sha256} after the drop: {error}")),
            Ok(_) => return Err(format!("blob/get {sha256} still answers after the drop")),
        }
    }
    let mut answers = Vec::with_capacity(presigned.len());
    for object in presigned {
        if object.expires_at_ms.is_some_and(|at| at <= now_ms()) {
            return Err(format!(
                "the presigned GET of {} expired before the read-back",
                object.sha256
            ));
        }
        let (status, code) = first_byte(client, object).await?;
        if !matches!(status, StatusCode::NOT_FOUND | StatusCode::FORBIDDEN) {
            return Err(format!(
                "object GET of {} after the drop answered {status} {code}, expected 404 or 403",
                object.sha256
            ));
        }
        answers.push(format!("{} {code}", status.as_u16()).trim_end().to_owned());
    }
    answers.sort();
    answers.dedup();
    Ok(json!({
        "blob_list_after": 0,
        "blob_get_not_found": digests.len(),
        "object_gone": presigned.len(),
        "object_answers": answers,
    }))
}

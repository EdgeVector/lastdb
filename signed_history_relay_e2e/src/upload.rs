//! Bootstrap: birth, read, write and submit grants, a write lease, a member write, ~300 MiB of encrypted
//! bundles, the member's revoke, then `heads`.
//!
//! The publisher writes through two device streams of one node key. Blobs are
//! generated, encrypted and uploaded in parallel (`blob.put` before `log.append`),
//! then the bundle entries are chained in order and appended.

use std::sync::Arc;
use std::time::Instant;

use futures::stream::{self, StreamExt, TryStreamExt};
use mutation_log::{
    BundleBody, DatabaseId, DeviceHead, DeviceId, Hex32, NodeKey, BUNDLE_BLOB_SIZE_MAX,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::blob::{bundle_changes, encode_changes, BundleCipher, BUNDLE_RECORDS, VALUE_TEXT_BYTES};
use crate::chain::{Chain, Publisher};
use crate::grants::{self, Scene};
use crate::identity::{new_device_id, now_ms, Actor};
use crate::ledger::Ledger;

const DAY_MS: u64 = 86_400_000;
use crate::relay::{BlobPutTiming, RelayClient};

/// What a restoring node learns out of band (an invite): the database, the
/// publisher node and its first device stream, and the key that opens bundle blobs.
pub struct Invite {
    pub database_id: DatabaseId,
    pub owner: NodeKey,
    pub root_device: DeviceId,
    pub secret: [u8; 32],
}

pub struct UploadPlan {
    pub relay_url: String,
    pub total_bytes: u64,
    pub concurrency: usize,
    /// Node granted `read`; it performs the restore.
    pub reader: NodeKey,
    /// Where the database and each committed blob are recorded for the cleanup.
    pub ledger: Ledger,
}

pub struct UploadReport {
    pub invite: Invite,
    pub seconds: f64,
    pub records: u64,
    pub bundles: usize,
    pub blob_bytes: u64,
    pub cpu_seconds: f64,
    /// The revoked `write` member whose one record is still in the history.
    pub member: NodeKey,
    /// HTTP status from a never-granted node's append and read (must be 403).
    pub nongrantee_http_status: u16,
    /// Wall time of the bundle puts alone, all bundles, `concurrency` at once.
    pub blobs_seconds: f64,
    /// One object per bundle: its size and the time of begin, the presigned PUT and commit, with
    /// every try of the PUT. It says which upload made a slow run slow.
    pub blob_puts: Vec<Value>,
}

struct PutBundle {
    index: u64,
    sha256: Hex32,
    size: u64,
    count: u32,
    cpu_seconds: f64,
    timing: BlobPutTiming,
}

impl PutBundle {
    fn report(&self) -> Value {
        json!({
            "index": self.index,
            "bytes": self.size,
            "begin_seconds": self.timing.begin_seconds,
            "put_seconds": self.timing.put_seconds,
            "commit_seconds": self.timing.commit_seconds,
            "attempts": self.timing.attempts.iter().map(|attempt| json!({
                "seconds": attempt.seconds,
                "sent_bytes": attempt.sent_bytes,
                "outcome": attempt.outcome,
            })).collect::<Vec<_>>(),
        })
    }
}

pub async fn bootstrap(http: &reqwest::Client, plan: &UploadPlan) -> Result<UploadReport, String> {
    let started = Instant::now();
    let owner = Actor::generate();
    let [member_actor, submit_actor, read_actor] = [(); 3].map(|()| Actor::generate());
    let birth = plan.ledger.mint(&owner);
    let publisher = Publisher {
        actor: owner,
        database_id: birth.database_id,
    };
    let cipher = Arc::new(BundleCipher::new(&birth.secret, birth.database_id)?);
    let mut chain_a = Chain::new(new_device_id());
    let mut chain_b = Chain::new(new_device_id());
    let client = |chain: &Chain| {
        RelayClient::new(
            &plan.relay_url,
            http,
            &publisher.actor,
            birth.database_id,
            chain.device_id,
        )
    };
    let (client_a, client_b) = (client(&chain_a), client(&chain_b));

    let opening = [
        publisher.birth(&mut chain_a, &birth)?,
        publisher.grant_read(&mut chain_a, plan.reader)?,
        publisher.grant_write(&mut chain_a, member_actor.node_key)?,
        publisher.grant_submit(&mut chain_a, submit_actor.node_key)?,
        publisher.grant_read(&mut chain_a, read_actor.node_key)?,
        publisher.lease(&mut chain_a, now_ms() + DAY_MS, now_ms() + 2 * DAY_MS)?,
    ];
    client_a.append(&opening).await?;
    let scene = Scene {
        publisher: &publisher.actor,
        http,
        relay_url: &plan.relay_url,
        database_id: birth.database_id,
        owner: publisher.actor.node_key,
        root_device: chain_a.device_id,
    };
    let member =
        grants::propose_as_member(&scene, member_actor, &submit_actor, &read_actor).await?;

    let records = plan.total_bytes.div_ceil(VALUE_TEXT_BYTES).max(1);
    let bundles = records.div_ceil(BUNDLE_RECORDS);
    let blobs_started = Instant::now();
    let mut puts: Vec<PutBundle> = stream::iter(0..bundles)
        .map(|index| put_bundle(&client_a, &cipher, &plan.ledger, index, records))
        .buffer_unordered(plan.concurrency)
        .try_collect()
        .await?;
    let blobs_seconds = blobs_started.elapsed().as_secs_f64();
    puts.sort_by_key(|put| put.index);

    let mut entries_a = vec![
        publisher.incorporate(&mut chain_a, member.proposal()?)?,
        publisher.revoke(&mut chain_a, member.node_key())?,
    ];
    let mut entries_b = Vec::new();
    for put in &puts {
        let body = BundleBody {
            blob_sha256: put.sha256,
            blob_size: put.size,
            change_count: put.count,
            grant_at_device: None,
            grant_at_seq: None,
        };
        if put.index.is_multiple_of(2) {
            entries_a.push(publisher.bundle(&mut chain_a, body)?);
        } else {
            entries_b.push(publisher.bundle(&mut chain_b, body)?);
        }
    }
    let tips: Vec<DeviceHead> = [chain_a.tip(), chain_b.tip()]
        .into_iter()
        .flatten()
        .collect();
    entries_a.push(publisher.heads(&mut chain_a, tips)?);
    if !entries_b.is_empty() {
        client_b.append(&entries_b).await?;
    }
    client_a.append(&entries_a).await?;
    let seconds = started.elapsed().as_secs_f64();
    grants::expect_revoked(&scene, &member).await?;
    let nongrantee_http_status = grants::expect_nongrantee(&scene).await?;

    Ok(UploadReport {
        invite: Invite {
            database_id: birth.database_id,
            owner: publisher.actor.node_key,
            root_device: chain_a.device_id,
            secret: birth.secret,
        },
        seconds,
        records,
        bundles: puts.len(),
        blob_bytes: puts.iter().map(|put| put.size).sum(),
        cpu_seconds: puts.iter().map(|put| put.cpu_seconds).sum(),
        member: member.node_key(),
        nongrantee_http_status,
        blobs_seconds,
        blob_puts: puts.iter().map(PutBundle::report).collect(),
    })
}

async fn put_bundle(
    client: &RelayClient,
    cipher: &Arc<BundleCipher>,
    ledger: &Ledger,
    index: u64,
    records: u64,
) -> Result<PutBundle, String> {
    let first = index * BUNDLE_RECORDS;
    let count = BUNDLE_RECORDS.min(records - first);
    let cipher = Arc::clone(cipher);
    let cpu = Instant::now();
    let (blob, digest) = tokio::task::spawn_blocking(move || {
        let plain = encode_changes(&bundle_changes(first, count))?;
        let blob = cipher.seal(&plain)?;
        let digest: [u8; 32] = Sha256::digest(&blob).into();
        Ok::<_, String>((Arc::<[u8]>::from(blob), digest))
    })
    .await
    .map_err(|e| format!("bundle worker: {e}"))??;
    let cpu_seconds = cpu.elapsed().as_secs_f64();
    let size = blob.len() as u64;
    if size > BUNDLE_BLOB_SIZE_MAX {
        return Err(format!("bundle {index} blob is {size} bytes, over the cap"));
    }
    let sha256 = Hex32::from_bytes(digest);
    let timing = client.blob_put_timed(&sha256.to_hex(), blob).await?;
    ledger.blob(&client.database_hex, &sha256.to_hex(), size);
    eprintln!(
        "bundle {index}: {size} bytes, begin {:.3}s, put {:.3}s in {} tries, commit {:.3}s",
        timing.begin_seconds,
        timing.put_seconds,
        timing.attempts.len(),
        timing.commit_seconds
    );
    Ok(PutBundle {
        index,
        sha256,
        size,
        count: u32::try_from(count).map_err(|_| "bundle change count over u32")?,
        cpu_seconds,
        timing,
    })
}

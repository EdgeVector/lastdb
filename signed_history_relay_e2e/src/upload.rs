//! Bootstrap: birth, read and write grants, a member write, ~300 MiB of encrypted
//! bundles, the member's revoke, then `heads`.
//!
//! The publisher writes through two device streams of one node key. Blobs are
//! generated, encrypted and uploaded in parallel (`blob.put` before `log.append`),
//! then the bundle entries are chained in order and appended.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use mutation_log::{
    BundleBody, DatabaseId, DeviceHead, DeviceId, Hex32, NodeKey, BUNDLE_BLOB_SIZE_MAX,
};
use sha2::{Digest, Sha256};

use crate::blob::{bundle_changes, encode_changes, BundleCipher, BUNDLE_RECORDS, VALUE_TEXT_BYTES};
use crate::chain::{Chain, Publisher};
use crate::grants::{self, Scene};
use crate::identity::{new_device_id, Actor, DbBirth};
use crate::relay::RelayClient;

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
}

struct PutBundle {
    index: u64,
    sha256: Hex32,
    size: u64,
    count: u32,
    cpu_seconds: f64,
}

pub async fn bootstrap(http: &reqwest::Client, plan: &UploadPlan) -> Result<UploadReport, String> {
    let started = Instant::now();
    let owner = Actor::generate();
    let member_actor = Actor::generate();
    let birth = DbBirth::generate(owner.node_key);
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
    ];
    client_a.append(&opening).await?;
    let scene = Scene {
        http,
        relay_url: &plan.relay_url,
        database_id: birth.database_id,
        owner: publisher.actor.node_key,
        root_device: chain_a.device_id,
    };
    let mut member = grants::write_as_member(&scene, member_actor).await?;

    let records = plan.total_bytes.div_ceil(VALUE_TEXT_BYTES).max(1);
    let bundles = records.div_ceil(BUNDLE_RECORDS);
    let mut puts: Vec<PutBundle> = stream::iter(0..bundles)
        .map(|index| put_bundle(&client_a, &cipher, index, records))
        .buffer_unordered(plan.concurrency)
        .try_collect()
        .await?;
    puts.sort_by_key(|put| put.index);

    let mut entries_a =
        vec![publisher.revoke(&mut chain_a, member.node_key(), member.cutoff()?)?];
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
    grants::expect_revoked(&scene, &mut member).await?;

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
    })
}

async fn put_bundle(
    client: &RelayClient,
    cipher: &Arc<BundleCipher>,
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
        Ok::<_, String>((Bytes::from(blob), digest))
    })
    .await
    .map_err(|e| format!("bundle worker: {e}"))??;
    let cpu_seconds = cpu.elapsed().as_secs_f64();
    let size = blob.len() as u64;
    if size > BUNDLE_BLOB_SIZE_MAX {
        return Err(format!("bundle {index} blob is {size} bytes, over the cap"));
    }
    let sha256 = Hex32::from_bytes(digest);
    client.blob_put(&sha256.to_hex(), blob).await?;
    Ok(PutBundle {
        index,
        sha256,
        size,
        count: u32::try_from(count).map_err(|_| "bundle change count over u32")?,
        cpu_seconds,
    })
}

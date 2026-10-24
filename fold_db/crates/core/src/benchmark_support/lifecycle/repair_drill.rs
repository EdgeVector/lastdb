//! Tom-approved one-time CI recovery drill. This integration branch never merges.
//! The executable checks exact generated-home ownership before entry.

use super::{Result, Value};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::fold_db_core::fold_db::FoldDB;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const CHECKPOINT: &[u8] = b"lifecycle-ci:repair-checkpoint";
const PREFIXES: &[&str] = &["mk:", "mh:", "history\0", "mref:v1:", "atom:", "cas:"];
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Serialize, Deserialize)]
struct Saved {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    original_digest: String,
    saved: Vec<Saved>,
}

pub(super) async fn measure(db: &FoldDB, phase: &str) -> Result<Value> {
    let atoms = db.db_ops().atoms();
    let rows = inventory(atoms).await?;
    if phase == "repair-inventory" {
        return Ok(json!({"digest":digest(&rows), "rows":rows.len()}));
    }
    if phase == "repair-restore" {
        return restore(atoms).await;
    }
    let case = phase
        .strip_prefix("repair-inject-")
        .ok_or("unknown repair phase")?;
    if atoms.raw().inner().get(CHECKPOINT).await?.is_some() {
        return Err("a prior repair drill checkpoint still exists".into());
    }
    let changes = perturbations(&rows, case)?;
    let checkpoint = Checkpoint {
        original_digest: digest(&rows),
        saved: changes
            .keys()
            .map(|key| Saved {
                key: key.clone(),
                value: rows.get(key).cloned(),
            })
            .collect(),
    };
    let mut mutations = operations(&changes);
    mutations.push(KvMutation::put(
        CHECKPOINT,
        serde_json::to_vec(&checkpoint)?,
    ));
    atoms.raw().inner().batch_mutate(mutations).await?;
    atoms.flush().await?;
    let after = inventory(atoms).await?;
    Ok(
        json!({"case":case, "original_digest":checkpoint.original_digest,
        "damaged_digest":digest(&after), "changed_records":changes.len()}),
    )
}

async fn inventory(atoms: &AtomStore) -> Result<Rows> {
    let mut rows = Rows::new();
    for prefix in PREFIXES {
        let end = crate::schema::types::field::FilterUtils::create_prefix_end(prefix)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), 256);
        for _ in 0..64 {
            if walk.done() {
                break;
            }
            let page = atoms.walk_step(&mut walk).await?;
            if page.unsealed > 0 {
                return Err("the drill inventory discarded storage records".into());
            }
            rows.extend(page.rows);
        }
        if !walk.done() || rows.len() > 16_384 {
            return Err("the generated repair drill exceeds its inventory bound".into());
        }
    }
    Ok(rows)
}

fn digest(rows: &Rows) -> String {
    let mut hash = Sha256::new();
    for (key, value) in rows {
        let canonical = serde_json::from_slice::<Value>(value)
            .ok()
            .and_then(|v| serde_json::to_vec(&v).ok())
            .unwrap_or_else(|| value.clone());
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(key);
        hash.update((canonical.len() as u64).to_be_bytes());
        hash.update(canonical);
    }
    format!("{:x}", hash.finalize())
}

fn operations(changes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Vec<KvMutation> {
    changes
        .iter()
        .map(|(key, value)| match value {
            Some(value) => KvMutation::put(key, value),
            None => KvMutation::delete(key),
        })
        .collect()
}

async fn restore(atoms: &AtomStore) -> Result<Value> {
    let bytes = atoms
        .raw()
        .inner()
        .get(CHECKPOINT)
        .await?
        .ok_or("no repair drill checkpoint")?;
    let checkpoint: Checkpoint = serde_json::from_slice(&bytes)?;
    let changes = checkpoint
        .saved
        .into_iter()
        .map(|s| (s.key, s.value))
        .collect();
    let mut mutations = operations(&changes);
    mutations.push(KvMutation::delete(CHECKPOINT));
    atoms.raw().inner().batch_mutate(mutations).await?;
    atoms.flush().await?;
    let restored = digest(&inventory(atoms).await?);
    if restored != checkpoint.original_digest {
        return Err("the generated source differs after exact drill restoration".into());
    }
    Ok(json!({"restored_digest":restored, "complete":true}))
}

fn shard(molecule: &str) -> u64 {
    let token = format!("{:x}", Sha256::digest(molecule.as_bytes()));
    token.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    }) % 4
}

fn header(rows: &Rows, wanted: u64) -> Result<(Vec<u8>, Value, String)> {
    for (key, bytes) in rows {
        let Some(molecule) = key.strip_prefix(b"mh:") else {
            continue;
        };
        let molecule = std::str::from_utf8(molecule)?;
        let value: Value = serde_json::from_slice(bytes)?;
        if value["slots"].as_u64().unwrap_or(0) > 0 && shard(molecule) == wanted {
            return Ok((key.clone(), value, molecule.to_string()));
        }
    }
    Err("the real wide-write workload lacks a required molecule shard".into())
}

fn corrupt_header(mut value: Value) -> Result<Vec<u8>> {
    value["slots"] = json!(900_001);
    value["records"] = json!(900_002);
    value["suspect"] = json!(true);
    Ok(serde_json::to_vec(&value)?)
}

fn perturbations(rows: &Rows, case: &str) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let (dirty_key, dirty, _) = header(rows, 0)?;
    let (key, value, molecule) = header(rows, 3)?;
    // Every refusal also sees an independently repairable header in shard 0.
    // A later-shard refusal must leave that earlier repair unwritten.
    let mut changes = BTreeMap::from([(dirty_key, Some(corrupt_header(dirty)?))]);
    let token = format!("{:x}", Sha256::digest(molecule.as_bytes()));
    let counter_key = format!("mref:v1:n:{token}").into_bytes();
    let edges: Vec<_> = rows
        .iter()
        .filter(|(key, _)| key.starts_with(format!("mref:v1:e:{token}\0").as_bytes()))
        .collect();
    if edges.is_empty() {
        return Err("the selected live molecule lacks real binding records".into());
    }
    match case {
        "repairable" => {
            changes.insert(key, Some(corrupt_header(value)?));
            changes.insert(
                counter_key,
                Some(serde_json::to_vec(
                    &json!({"v":1,"bindings":800_001,"suspect":true}),
                )?),
            );
            let orphan = format!("mref:v1:n:{:x}", Sha256::digest(b"generated-orphan"));
            if rows.contains_key(orphan.as_bytes()) {
                return Err("the generated orphan identity already exists".into());
            }
            changes.insert(
                orphan.into_bytes(),
                Some(serde_json::to_vec(
                    &json!({"v":1,"bindings":0,"suspect":false}),
                )?),
            );
        }
        "missing-header" => {
            changes.insert(key, None);
        }
        "unreadable-header" => {
            changes.insert(key, Some(b"not-json".to_vec()));
        }
        "unbound-live" => {
            for (key, _) in edges {
                changes.insert(key.clone(), None);
            }
        }
        "unreadable-binding" => {
            changes.insert(edges[0].0.clone(), Some(b"not-json".to_vec()));
        }
        "binding-identity" => {
            let mut value: Value = serde_json::from_slice(edges[0].1)?;
            value["molecule_uuid"] = json!("different-generated-molecule");
            changes.insert(edges[0].0.clone(), Some(serde_json::to_vec(&value)?));
        }
        "unreadable-tip" => {
            let tip = rows
                .keys()
                .find(|key| key.starts_with(format!("mk:{molecule}:").as_bytes()))
                .ok_or("the selected live molecule lacks a tip")?;
            changes.insert(tip.clone(), Some(b"not-json".to_vec()));
        }
        "unreadable-conflict" => {
            let record = rows
                .keys()
                .find(|key| {
                    key.starts_with(b"history\0")
                        && key.windows(10).any(|part| part == b":conflict:")
                })
                .ok_or("the real replay workload lacks a conflict record")?;
            changes.insert(record.clone(), Some(b"not-json".to_vec()));
        }
        _ => return Err("unknown generated repair damage".into()),
    }
    Ok(changes)
}

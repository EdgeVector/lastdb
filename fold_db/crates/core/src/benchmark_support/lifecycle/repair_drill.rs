//! Tom-approved one-time CI recovery drill. This integration branch never merges.
//! The executable checks exact generated-home ownership before entry.

use super::{Result, Value};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::fold_db_core::fold_db::FoldDB;
use crate::schema::types::field::build_storage_key;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const CHECKPOINT: &[u8] = b"lifecycle-ci:repair-checkpoint";
const SCOPE: &str = "6e18c61c7416a50b7f221c4cf2f80b6e9a1f4459db974a30430a1c87e3df44ca";
const PREFIXES: &[&str] = &[
    "mk:",
    "mh:",
    "history\0",
    "mref:v1:",
    "atom:",
    "cas_blob:",
    "bref:",
];
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
    if phase == "repair-seed-scope" {
        return seed_scope(atoms, rows).await;
    }
    if phase == "repair-inventory" {
        return Ok(json!({"digest":digest(&rows), "rows":rows.len(),
            "write_surface_digest":write_surface_digest(atoms, rows.clone()).await?}));
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
        "damaged_digest":digest(&after), "changed_records":changes.len(),
        "damaged_write_surface_digest":write_surface_digest(atoms, after).await?}),
    )
}

async fn inventory(atoms: &AtomStore) -> Result<Rows> {
    let mut rows = Rows::new();
    for prefix in [None, Some(SCOPE)].into_iter().flat_map(|scope| {
        PREFIXES
            .iter()
            .map(move |prefix| build_storage_key(scope, prefix))
    }) {
        let end = crate::schema::types::field::FilterUtils::create_prefix_end(&prefix)?;
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

async fn write_surface_digest(atoms: &AtomStore, mut rows: Rows) -> Result<String> {
    for prefix in ["crq:", "crb:", "crs:"] {
        let end = crate::schema::types::field::FilterUtils::create_prefix_end(prefix)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), 256);
        for _ in 0..64 {
            if walk.done() {
                break;
            }
            let page = atoms.walk_step(&mut walk).await?;
            if page.unsealed > 0 {
                return Err("a queue record was discarded".into());
            }
            rows.extend(page.rows);
        }
        if !walk.done() {
            return Err("the generated queue inventory exceeds its bound".into());
        }
    }
    Ok(digest(&rows))
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
            Some(value) => KvMutation::put(key.as_slice(), value.as_slice()),
            None => KvMutation::delete(key.as_slice()),
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

fn header(rows: &Rows, wanted: u64, scope: Option<&str>) -> Result<(Vec<u8>, Value, String)> {
    let prefix = build_storage_key(scope, "mh:");
    for (key, bytes) in rows {
        let Some(molecule) = key.strip_prefix(prefix.as_bytes()) else {
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
    let (dirty_key, dirty, _) = header(rows, 0, None)?;
    let (key, value, molecule) = header(rows, 3, Some(SCOPE))?;
    // Every refusal also sees an independently repairable header in shard 0.
    // A later-shard refusal must leave that earlier repair unwritten.
    let mut changes = BTreeMap::from([(dirty_key, Some(corrupt_header(dirty)?))]);
    let token = format!("{:x}", Sha256::digest(molecule.as_bytes()));
    let counter_key = build_storage_key(Some(SCOPE), &format!("mref:v1:n:{token}")).into_bytes();
    let edges: Vec<_> = rows
        .iter()
        .filter(|(key, _)| {
            key.starts_with(
                build_storage_key(Some(SCOPE), &format!("mref:v1:e:{token}\0")).as_bytes(),
            )
        })
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
            let orphan = build_storage_key(
                Some(SCOPE),
                &format!("mref:v1:n:{:x}", Sha256::digest(b"generated-orphan")),
            );
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
                .find(|key| {
                    key.starts_with(
                        build_storage_key(Some(SCOPE), &format!("mk:{molecule}:")).as_bytes(),
                    )
                })
                .ok_or("the selected live molecule lacks a tip")?;
            changes.insert(tip.clone(), Some(b"not-json".to_vec()));
        }
        "unreadable-conflict" => {
            let record = rows
                .keys()
                .find(|key| {
                    key.starts_with(build_storage_key(Some(SCOPE), "history\0").as_bytes())
                        && key.windows(10).any(|part| part == b":conflict:")
                })
                .ok_or("the real replay workload lacks a conflict record")?;
            changes.insert(record.clone(), Some(b"not-json".to_vec()));
        }
        _ => return Err("unknown generated repair damage".into()),
    }
    Ok(changes)
}

async fn seed_scope(atoms: &AtomStore, rows: Rows) -> Result<Value> {
    let prefix = format!("{SCOPE}:");
    if rows.keys().any(|key| key.starts_with(prefix.as_bytes())) {
        return Err("the generated second scope already contains records".into());
    }
    // Copy only this generated home's actual protected records. This creates
    // a second populated storage scope for cross-scope repair/refusal checks.
    let changes = rows
        .into_iter()
        .map(|(key, value)| {
            let mut scoped = prefix.as_bytes().to_vec();
            scoped.extend(key);
            (scoped, Some(value))
        })
        .collect();
    let mut mutations = operations(&changes);
    let count = mutations.len();
    while !mutations.is_empty() {
        let batch = mutations.split_off(mutations.len().saturating_sub(256));
        atoms.raw().inner().batch_mutate(batch).await?;
    }
    atoms.flush().await?;
    Ok(json!({"scope":SCOPE, "copied_generated_records":count}))
}

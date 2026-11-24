//! Per-stream verification, planning, and deletion helpers for `log/trim`.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::fs;

use crate::error::{ApiError, ApiResult};
use crate::heads::{self, FloorFile, FLOOR_FILE};
use crate::store::{atomic_write, read_json, Store};

pub(crate) struct TrimScope<'a> {
    pub(crate) store: &'a Store,
    pub(crate) database_id: &'a str,
    pub(crate) writer: &'a str,
}

impl TrimScope<'_> {
    pub(crate) fn stream_dir(&self, device: &str) -> PathBuf {
        let mut p = self.store.root.join("logs");
        p.extend([self.database_id, self.writer, device]);
        p
    }

    pub(crate) fn floor(&self, device: &str) -> u64 {
        self.store
            .heads
            .floor(self.database_id, self.writer, device)
    }
}

pub(crate) struct Plan {
    device: String,
    dir: PathBuf,
    floor: Option<(u64, String)>,
    covered: Vec<u64>,
}

pub(crate) fn permits_entry(is_snapshot: bool, entry: &Value) -> bool {
    let kind = entry.get("kind").and_then(Value::as_str);
    if is_snapshot {
        kind == Some("snapshot")
    } else {
        matches!(kind, Some("data" | "schema"))
            && entry.get("namespace").and_then(Value::as_str) == Some("sealed")
    }
}

pub(crate) async fn ensure_personal_logs(
    scope: &TrimScope<'_>,
    snapshot_device: &str,
    snap_covered: u64,
    through_map: &BTreeMap<String, u64>,
) -> ApiResult<()> {
    let db_dir = scope.store.root.join("logs").join(scope.database_id);
    for (writer_name, writer_path) in subdirs(&db_dir).await {
        if writer_name != scope.writer {
            return Err(ApiError::forbidden("grant"));
        }
        for (dev, dev_path) in subdirs(&writer_path).await {
            let skip = if dev == snapshot_device {
                snap_covered
            } else {
                through_map.get(&dev).copied().unwrap_or(0)
            };
            check_stream_tail(&dev_path, &dev, snapshot_device, skip, through_map).await?;
        }
    }
    Ok(())
}

async fn check_stream_tail(
    dir: &Path,
    dev: &str,
    snapshot_device: &str,
    skip: u64,
    through_map: &BTreeMap<String, u64>,
) -> ApiResult<()> {
    let is_snap = dev == snapshot_device;
    for seq in stored_seqs(dir, skip + 1, u64::MAX).await {
        let entry = read_json(&dir.join(format!("{seq}.json"))).await?;
        let kind = entry.get("kind").and_then(Value::as_str);
        if matches!(kind, Some("grant" | "lease" | "bundle")) {
            return Err(ApiError::forbidden("grant"));
        }
        let is_valid_birth =
            kind == Some("birth") && seq == 1 && !is_snap && !through_map.contains_key(dev);
        if !is_valid_birth && !permits_entry(is_snap, &entry) {
            return Err(ApiError::forbidden("namespace"));
        }
    }
    Ok(())
}

pub(crate) async fn plan(
    scope: &TrimScope<'_>,
    device: &str,
    through: u64,
    expect: Option<&str>,
    is_snapshot: bool,
) -> ApiResult<Plan> {
    let dir = scope.stream_dir(device);
    let current_floor = scope.floor(device);
    if through < current_floor {
        return Err(ApiError::invalid("through"));
    }
    if through == 0 {
        return Ok(Plan {
            device: device.to_owned(),
            dir,
            floor: None,
            covered: Vec::new(),
        });
    }
    let head = scope
        .store
        .heads
        .head_seq(scope.database_id, scope.writer, device);
    if head.is_none_or(|h| h < through) {
        return Err(ApiError::invalid("through"));
    }
    let floor = if through > current_floor {
        let last = read_json(&dir.join(format!("{through}.json")))
            .await
            .map_err(|_| ApiError::invalid("through"))?;
        let hash = heads::hash_of(&last)?;
        if expect.is_some_and(|want| want != hash.as_str()) {
            return Err(ApiError::invalid("through"));
        }
        Some((through, hash))
    } else if let Some(want) = expect {
        let raw = fs::read(dir.join(FLOOR_FILE))
            .await
            .map_err(|_| ApiError::invalid("through"))?;
        let rec: FloorFile =
            serde_json::from_slice(&raw).map_err(|_| ApiError::invalid("through"))?;
        if rec.seq != through || rec.hash != want {
            return Err(ApiError::invalid("through"));
        }
        None
    } else {
        None
    };
    let covered = stored_seqs(&dir, 1, through).await;
    for seq in &covered {
        if !permits_entry(
            is_snapshot,
            &read_json(&dir.join(format!("{seq}.json"))).await?,
        ) {
            return Err(ApiError::forbidden("namespace"));
        }
    }
    Ok(Plan {
        device: device.to_owned(),
        dir,
        floor,
        covered,
    })
}

/// Drop what a plan names. Returns how many entries went and the length of their files.
pub(crate) async fn apply(scope: &TrimScope<'_>, plan: &Plan) -> ApiResult<(u64, u64)> {
    if let Some((seq, hash)) = &plan.floor {
        let rec = FloorFile {
            seq: *seq,
            hash: hash.clone(),
        };
        let bytes = serde_json::to_vec(&rec).map_err(|_| ApiError::invalid("floor"))?;
        atomic_write(&plan.dir.join(FLOOR_FILE), &bytes).await?;
        let heads = &scope.store.heads;
        heads.set_floor(
            scope.database_id,
            scope.writer,
            &plan.device,
            *seq,
            hash.clone(),
        );
    }
    let covered = if let Some((through, _)) = &plan.floor {
        stored_seqs(&plan.dir, 1, *through).await
    } else {
        plan.covered.clone()
    };
    let (mut dropped, mut released) = (0, 0);
    for seq in &covered {
        let file = plan.dir.join(format!("{seq}.json"));
        let length = fs::metadata(&file).await.map_or(0, |meta| meta.len());
        match fs::remove_file(&file).await {
            Ok(()) => {
                dropped += 1;
                released += length;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(ApiError::invalid("data_dir")),
        }
    }
    Ok((dropped, released))
}

async fn subdirs(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(mut rd) = fs::read_dir(dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            if ent.file_type().await.is_ok_and(|t| t.is_dir()) {
                if let Some(name) = ent.file_name().to_str() {
                    out.push((name.to_owned(), ent.path()));
                }
            }
        }
    }
    out
}

async fn stored_seqs(dir: &Path, min: u64, max: u64) -> Vec<u64> {
    let mut found = Vec::new();
    if let Ok(mut files) = fs::read_dir(dir).await {
        while let Ok(Some(file)) = files.next_entry().await {
            let seq = file
                .file_name()
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<u64>().ok());
            if let Some(seq) = seq.filter(|s| *s >= min && *s <= max) {
                found.push(seq);
            }
        }
    }
    found.sort_unstable();
    found
}

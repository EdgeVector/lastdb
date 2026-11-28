//! Per-stream verification, planning, and deletion helpers for `log/trim`.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::fs;

use crate::error::{ApiError, ApiResult};
use crate::heads::{FloorFile, FLOOR_FILE};
use crate::segment;
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
    writer: String,
    device: String,
    dir: PathBuf,
    floor: Option<(u64, String)>,
    covered: Vec<u64>,
}

pub(crate) fn permits_entry(is_snapshot: bool, entry: &Value) -> bool {
    // A segment holds sealed entries of a device stream (only the publisher appends one).
    if entry.get("segment").is_some() {
        return !is_snapshot;
    }
    let kind = entry.get("kind").and_then(Value::as_str);
    if is_snapshot {
        kind == Some("snapshot")
    } else {
        matches!(kind, Some("data" | "schema"))
            && entry.get("namespace").and_then(Value::as_str) == Some("sealed")
    }
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
            writer: scope.writer.to_owned(),
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
        match fs::read(dir.join(format!("{through}.json"))).await {
            Ok(bytes) => {
                let last: Value =
                    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("through"))?;
                let hash = segment::last_hash(&last)?;
                if expect.is_some_and(|want| want != hash.as_str()) {
                    return Err(ApiError::invalid("through"));
                }
                Some((through, hash))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                inside_segment(&dir, current_floor, through).await?
            }
            Err(_) => return Err(ApiError::invalid("data_dir")),
        }
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
    let dropped_through = floor.as_ref().map_or(current_floor, |(seq, _)| *seq);
    let covered = stored_seqs(&dir, 1, dropped_through).await?;
    for seq in &covered {
        if !permits_entry(
            is_snapshot,
            &read_json(&dir.join(format!("{seq}.json"))).await?,
        ) {
            return Err(ApiError::forbidden("namespace"));
        }
    }
    Ok(Plan {
        writer: scope.writer.to_owned(),
        device: device.to_owned(),
        dir,
        floor,
        covered,
    })
}

/// The floor of a trim to `through` when no item ends at `through`: it falls inside a stored
/// segment, which stays whole. The floor is the last item before that segment, `None` when there
/// is none above the current floor. No stored item holding `through` is 400 `through`.
async fn inside_segment(
    dir: &Path,
    current_floor: u64,
    through: u64,
) -> ApiResult<Option<(u64, String)>> {
    let holder = stored_seqs(dir, through + 1, u64::MAX).await?;
    let Some(key) = holder.first() else {
        return Err(ApiError::invalid("through"));
    };
    let item = read_json(&dir.join(format!("{key}.json"))).await?;
    let (first, _) = segment::covers(*key, &item)?;
    if first > through {
        return Err(ApiError::invalid("through"));
    }
    let below = stored_seqs(dir, current_floor + 1, through).await?;
    let Some(last) = below.last() else {
        return Ok(None);
    };
    let item = read_json(&dir.join(format!("{last}.json"))).await?;
    Ok(Some((*last, segment::last_hash(&item)?)))
}

/// Drop what a plan names. `done` counts the entries that went and the length of their files,
/// one at a time, so a failure in the middle still leaves the count of what it dropped. A
/// segment counts the entries it held and no length: its bytes are its blob's.
pub(crate) async fn apply(
    scope: &TrimScope<'_>,
    plan: &Plan,
    done: &mut (u64, u64),
) -> ApiResult<()> {
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
            &plan.writer,
            &plan.device,
            *seq,
            hash.clone(),
        );
    }
    let covered = if let Some((through, _)) = &plan.floor {
        stored_seqs(&plan.dir, 1, *through).await?
    } else {
        plan.covered.clone()
    };
    for seq in &covered {
        let file = plan.dir.join(format!("{seq}.json"));
        let (entries, length) = match fs::read(&file).await {
            Ok(bytes) => match serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|item| segment::span(&item).ok().flatten())
            {
                Some(span) => (span.last_seq - span.first_seq + 1, 0),
                None => (1, bytes.len() as u64),
            },
            Err(_) => (1, 0),
        };
        match fs::remove_file(&file).await {
            Ok(()) => {
                done.0 += entries;
                done.1 += length;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(ApiError::invalid("data_dir")),
        }
    }
    Ok(())
}

async fn stored_seqs(dir: &Path, min: u64, max: u64) -> ApiResult<Vec<u64>> {
    let mut found = Vec::new();
    let mut files = fs::read_dir(dir)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?;
    while let Some(file) = files
        .next_entry()
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?
    {
        let seq = file
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|name| name.parse::<u64>().ok());
        if let Some(seq) = seq.filter(|s| *s >= min && *s <= max) {
            found.push(seq);
        }
    }
    found.sort_unstable();
    Ok(found)
}

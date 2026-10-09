use super::*;

use super::attribution::*;
use super::paths::*;

/// One durable work item. Directory names share one encoded parent, which
/// keeps checkpoints compact even when a real home has 100,000+ siblings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HomeStorageWorkItem {
    VisitPath {
        path_hex: String,
    },
    DirectoryChildren {
        parent_path_hex: String,
        child_name_hex: Vec<String>,
    },
}

/// Durable state for one bounded walk. Paths use hex-encoded Unix bytes, so
/// non-UTF-8 names remain addressable across requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageReconcileCheckpoint {
    pub version: u32,
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub pending_work: Vec<HomeStorageWorkItem>,
    pub seen_inodes: BTreeSet<String>,
    pub buckets: BTreeMap<HomeStorageBucketKind, HomeStorageBucket>,
    #[serde(default)]
    pub unresolved_scopes: Vec<String>,
    pub visited_entries: u64,
    pub finished: bool,
}

impl HomeStorageReconcileCheckpoint {
    #[must_use]
    pub fn start(now: DateTime<Utc>) -> Self {
        Self {
            version: 2,
            run_id: format!("home-{}", now.timestamp_micros()),
            started_at: now,
            pending_work: vec![HomeStorageWorkItem::VisitPath {
                path_hex: encode_path(Path::new(".")),
            }],
            seen_inodes: BTreeSet::new(),
            buckets: BTreeMap::new(),
            unresolved_scopes: Vec::new(),
            visited_entries: 0,
            finished: false,
        }
    }

    #[must_use]
    pub fn resume_cursor(&self) -> Option<String> {
        self.pending_work.last().map(|item| match item {
            HomeStorageWorkItem::VisitPath { path_hex } => format!("path:{path_hex}"),
            HomeStorageWorkItem::DirectoryChildren {
                parent_path_hex,
                child_name_hex,
            } => format!("directory:{parent_path_hex}:{}", child_name_hex.len()),
        })
    }

    #[must_use]
    pub fn report(&self, now: DateTime<Utc>) -> HomeStorageReport {
        let buckets: Vec<HomeStorageBucket> = self.buckets.values().cloned().collect();
        let home = buckets
            .iter()
            .fold(HomeStorageTotals::default(), |mut sum, row| {
                sum.apparent_bytes = sum.apparent_bytes.saturating_add(row.apparent_bytes);
                sum.allocated_bytes = sum.allocated_bytes.saturating_add(row.allocated_bytes);
                sum
            });
        let mut unresolved = self.unresolved_scopes.clone();
        if !self.finished {
            push_scope(&mut unresolved, "reconcile_in_progress");
        }
        HomeStorageReport {
            metric: HOME_STORAGE_METRIC.to_string(),
            measured_at: now,
            snapshot_frontier: Some(self.run_id.clone()),
            complete: self.finished && unresolved.is_empty(),
            heavy: false,
            home,
            unique_physical_buckets: buckets,
            unaccounted_bytes: 0,
            overaccounted_bytes: 0,
            unaccounted_apparent_bytes: 0,
            overaccounted_apparent_bytes: 0,
            unknown_path_bytes: 0,
            unresolved_scopes: unresolved,
            inclusive_app_attribution: HomeStorageInclusiveAppAttribution::default(),
        }
        .validated_for_read()
    }
}

/// Process at most `work_budget` filesystem entries from one durable walk.
/// Symlinks are measured but never followed. A damaged path becomes an
/// unresolved scope and does not make the whole request fail invisibly.
pub fn reconcile_page(
    home: &Path,
    checkpoint: &mut HomeStorageReconcileCheckpoint,
    work_budget: usize,
) -> usize {
    let mut visited = 0;
    while visited < work_budget {
        let Some(work) = checkpoint.pending_work.pop() else {
            checkpoint.finished = true;
            break;
        };
        let used = match work {
            HomeStorageWorkItem::VisitPath { path_hex } => {
                visit_path(home, checkpoint, &path_hex);
                1
            }
            HomeStorageWorkItem::DirectoryChildren {
                parent_path_hex,
                mut child_name_hex,
            } => {
                let child = child_name_hex.pop();
                if !child_name_hex.is_empty() {
                    checkpoint
                        .pending_work
                        .push(HomeStorageWorkItem::DirectoryChildren {
                            parent_path_hex: parent_path_hex.clone(),
                            child_name_hex,
                        });
                }
                if let Some(child) = child {
                    visit_child(home, checkpoint, &parent_path_hex, &child);
                }
                1
            }
        };
        visited = visited.saturating_add(used.max(1));
        checkpoint.visited_entries = checkpoint
            .visited_entries
            .saturating_add(used.max(1) as u64);
    }
    if checkpoint.pending_work.is_empty() {
        checkpoint.finished = true;
    }
    visited
}

pub(super) fn visit_path(
    home: &Path,
    checkpoint: &mut HomeStorageReconcileCheckpoint,
    encoded: &str,
) {
    let relative = match decode_path(encoded) {
        Ok(path) if safe_relative(&path) => path,
        _ => {
            push_reconcile_scope(checkpoint, &format!("invalid_cursor_path:{encoded}"));
            return;
        }
    };
    let absolute = absolute_path(home, &relative);
    let metadata = match fs::symlink_metadata(&absolute) {
        Ok(metadata) => metadata,
        Err(error) => {
            push_reconcile_scope(
                checkpoint,
                &format!("metadata:{}:{error}", relative.to_string_lossy()),
            );
            return;
        }
    };

    let is_repeat_hard_link = !metadata.file_type().is_dir()
        && metadata.nlink() > 1
        && !checkpoint
            .seen_inodes
            .insert(format!("{}:{}", metadata.dev(), metadata.ino()));
    if !is_repeat_hard_link {
        add_entry(checkpoint, &relative, &metadata);
    }

    if metadata.file_type().is_dir() {
        let mut names = Vec::new();
        let mut listing_bytes = 0_usize;
        let entries = match fs::read_dir(&absolute) {
            Ok(entries) => entries,
            Err(error) => {
                push_reconcile_scope(
                    checkpoint,
                    &format!("read_dir:{}:{error}", relative.to_string_lossy()),
                );
                return;
            }
        };
        for entry in entries {
            let name = match entry {
                Ok(entry) => entry.file_name(),
                Err(error) => {
                    push_reconcile_scope(
                        checkpoint,
                        &format!("read_dir_entry:{}:{error}", relative.to_string_lossy()),
                    );
                    continue;
                }
            };
            listing_bytes = listing_bytes.saturating_add(name.as_bytes().len() + 16);
            if listing_bytes > DIRECTORY_LISTING_BYTES_MAX {
                push_reconcile_scope(
                    checkpoint,
                    &format!("directory_listing_limit:{}", relative.to_string_lossy()),
                );
                break;
            }
            names.push(fold_db::hex::hex_lower(name.as_bytes()));
        }
        names.sort_unstable();
        names.reverse();
        if !names.is_empty() {
            checkpoint
                .pending_work
                .push(HomeStorageWorkItem::DirectoryChildren {
                    parent_path_hex: encoded.to_string(),
                    child_name_hex: names,
                });
        }
    }
}

pub(super) fn visit_child(
    home: &Path,
    checkpoint: &mut HomeStorageReconcileCheckpoint,
    parent_path_hex: &str,
    child_name_hex: &str,
) {
    let parent = match decode_path(parent_path_hex) {
        Ok(path) if safe_relative(&path) => path,
        _ => {
            push_reconcile_scope(
                checkpoint,
                &format!("invalid_cursor_path:{parent_path_hex}"),
            );
            return;
        }
    };
    let name = match decode_path(child_name_hex) {
        Ok(path)
            if path.components().count() == 1
                && matches!(path.components().next(), Some(Component::Normal(_))) =>
        {
            path
        }
        _ => {
            push_reconcile_scope(checkpoint, &format!("invalid_child_name:{child_name_hex}"));
            return;
        }
    };
    let child = if parent == Path::new(".") {
        name
    } else {
        parent.join(name)
    };
    visit_path(home, checkpoint, &encode_path(&child));
}

pub(super) fn absolute_path(home: &Path, relative: &Path) -> PathBuf {
    if relative == Path::new(".") {
        home.to_path_buf()
    } else {
        home.join(relative)
    }
}

pub(super) fn add_entry(
    checkpoint: &mut HomeStorageReconcileCheckpoint,
    relative: &Path,
    metadata: &fs::Metadata,
) {
    let kind = classify_path(relative);
    let root = display_root(relative);
    let bucket = checkpoint
        .buckets
        .entry(kind)
        .or_insert_with(|| HomeStorageBucket {
            kind,
            apparent_bytes: 0,
            allocated_bytes: 0,
            entry_count: 0,
            roots: Vec::new(),
        });
    bucket.apparent_bytes = bucket.apparent_bytes.saturating_add(metadata.len());
    bucket.allocated_bytes = bucket
        .allocated_bytes
        .saturating_add(metadata.blocks().saturating_mul(512));
    bucket.entry_count = bucket.entry_count.saturating_add(1);
    if bucket.roots.len() < ROOTS_PER_BUCKET_MAX && !bucket.roots.contains(&root) {
        bucket.roots.push(root);
    }
}

pub(super) fn push_reconcile_scope(checkpoint: &mut HomeStorageReconcileCheckpoint, scope: &str) {
    push_scope(&mut checkpoint.unresolved_scopes, scope);
}

//! Persisted LastDB-home storage ledger.
//!
//! `GET /api/storage/home` reads one metadata key and returns this snapshot.
//! It never inventories the filesystem or product collections. The bounded
//! reconcile job owns snapshot creation; the read path only validates the
//! persisted arithmetic before it can call a report complete.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

/// Point-read key for the latest durable home-storage snapshot.
pub const HOME_STORAGE_SNAPSHOT_KEY: &str = "home_storage:snapshot:v1";

/// Point-read key for the active bounded filesystem inventory.
pub const HOME_STORAGE_RECONCILE_KEY: &str = "home_storage:reconcile:v1";

pub const HOME_STORAGE_RECONCILE_WORK_DEFAULT: usize = 128;
pub const HOME_STORAGE_RECONCILE_WORK_MAX: usize = 65_536;
const ROOTS_PER_BUCKET_MAX: usize = 16;
const UNRESOLVED_SCOPES_MAX: usize = 64;
const DIRECTORY_LISTING_BYTES_MAX: usize = 64 * 1024 * 1024;

/// Public metric name. This changes only with a wire-contract change.
pub const HOME_STORAGE_METRIC: &str = "lastdb_home_storage_v1";

/// Public metric name for logical data counted once per reaching app.
pub const HOME_STORAGE_APP_ATTRIBUTION_METRIC: &str = "lastdb_home_inclusive_app_logical_bytes_v1";

/// `10_000` basis points means `1.0x`.
const ONE_X_BASIS_POINTS: u64 = 10_000;

/// One mutually exclusive top-level class below the configured LastDB home.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HomeStorageBucketKind {
    DatabaseStore,
    DatabaseAuxiliary,
    Backup,
    Recovery,
    RuntimeBinary,
    RuntimeApp,
    Log,
    Candidate,
    /// A path that the current classifier does not recognize. It remains an
    /// accounted byte; a future release can give the path a better name.
    UnknownPath,
}

/// Exact aggregate for one mutually exclusive home class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageBucket {
    pub kind: HomeStorageBucketKind,
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
    /// Number of distinct filesystem entries folded into this bucket.
    pub entry_count: u64,
    /// Largest or otherwise useful roots that explain the bucket. Reconcile
    /// bounds this list; the normal GET only returns the persisted values.
    #[serde(default)]
    pub roots: Vec<String>,
}

/// Filesystem totals measured at one snapshot frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HomeStorageTotals {
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
}

/// One schema binding used to build inclusive app reachability.
///
/// This is an in-memory reconcile input. The persisted report stores only the
/// resulting app rows and equations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppLogicalBinding {
    pub schema_binding: String,
    /// `None` is a system schema, not an unknown app.
    pub app_id: Option<String>,
    pub molecules: Vec<AppLogicalMolecule>,
}

/// One logical molecule counter referenced by a schema binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppLogicalMolecule {
    pub molecule_id: String,
    /// Missing means the write-path counter cannot prove this unit yet.
    pub logical_bytes: Option<u64>,
}

/// One app's inclusive logical reachability row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageAppAttributionRow {
    pub app_id: String,
    /// Logical units reached by this app and no other app.
    pub exclusive_logical_bytes: u64,
    /// Logical units reached by this app and at least one other app.
    pub shared_logical_bytes: u64,
    /// `exclusive_logical_bytes + shared_logical_bytes`.
    pub inclusive_logical_bytes: u64,
    /// The part of this row that overlaps another app. This equals the row's
    /// `shared_logical_bytes`; the report-level overlap removes the first
    /// unique copy only once.
    pub overlap_logical_bytes: u64,
    /// Inclusive divided by exclusive, in basis points. `None` means the app
    /// reaches shared data but has no exclusive denominator.
    pub amplification_basis_points: Option<u64>,
    pub molecule_count: u64,
    pub shared_molecule_count: u64,
    pub schema_bindings: Vec<String>,
}

/// Inclusive app ledger persisted beside the unique physical home ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageInclusiveAppAttribution {
    pub metric: String,
    pub measured_at: DateTime<Utc>,
    pub complete: bool,
    /// Every valid logical molecule reached by at least one app, once.
    pub unique_logical_bytes: u64,
    /// Valid logical molecules reached only by system schemas.
    pub system_unique_logical_bytes: u64,
    /// `unique_logical_bytes + system_unique_logical_bytes`.
    pub total_unique_logical_bytes: u64,
    /// Sum of all app rows. Shared molecules repeat once per reaching app.
    pub inclusive_app_logical_bytes: u64,
    /// `inclusive_app_logical_bytes - unique_logical_bytes`.
    pub overlap_logical_bytes: u64,
    /// Inclusive app bytes divided by unique app bytes, in basis points.
    pub amplification_basis_points: Option<u64>,
    pub pending_protein_folds: u64,
    pub apps: Vec<HomeStorageAppAttributionRow>,
    #[serde(default)]
    pub unresolved_scopes: Vec<String>,
}

impl Default for HomeStorageInclusiveAppAttribution {
    fn default() -> Self {
        Self {
            metric: HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string(),
            measured_at: Utc::now(),
            complete: false,
            unique_logical_bytes: 0,
            system_unique_logical_bytes: 0,
            total_unique_logical_bytes: 0,
            inclusive_app_logical_bytes: 0,
            overlap_logical_bytes: 0,
            amplification_basis_points: None,
            pending_protein_folds: 0,
            apps: Vec::new(),
            unresolved_scopes: vec!["attribution_not_measured".to_string()],
        }
    }
}

#[derive(Default)]
struct LogicalUnit {
    logical_bytes: Option<u64>,
    apps: BTreeSet<String>,
    system_reachable: bool,
}

#[derive(Default)]
struct AppLogicalBucket {
    exclusive_logical_bytes: u64,
    shared_logical_bytes: u64,
    molecule_count: u64,
    shared_molecule_count: u64,
    schema_bindings: BTreeSet<String>,
}

/// Build the inclusive logical ledger without reading any atom or tip plane.
/// A molecule contributes once to the unique ledger and once to every app
/// that reaches it through a declared schema binding.
#[must_use]
pub fn build_inclusive_app_attribution(
    bindings: &[AppLogicalBinding],
    counters_complete: bool,
    pending_protein_folds: u64,
    mut unresolved_scopes: Vec<String>,
    measured_at: DateTime<Utc>,
) -> HomeStorageInclusiveAppAttribution {
    let supplied_scopes = std::mem::take(&mut unresolved_scopes);
    for scope in supplied_scopes {
        push_scope(&mut unresolved_scopes, &scope);
    }
    let mut units: BTreeMap<String, LogicalUnit> = BTreeMap::new();
    let mut app_buckets: BTreeMap<String, AppLogicalBucket> = BTreeMap::new();

    if !counters_complete {
        push_scope(&mut unresolved_scopes, "molecule_counters_incomplete");
    }
    if pending_protein_folds > 0 {
        push_scope(&mut unresolved_scopes, "pending_protein_folds");
    }

    for binding in bindings {
        let app_id = binding
            .app_id
            .as_deref()
            .map(str::trim)
            .filter(|app| !app.is_empty());
        if let Some(app_id) = app_id {
            app_buckets
                .entry(app_id.to_string())
                .or_default()
                .schema_bindings
                .insert(binding.schema_binding.clone());
        }

        let mut seen = BTreeSet::new();
        for molecule in &binding.molecules {
            if !seen.insert(molecule.molecule_id.as_str()) {
                continue;
            }
            let Some(logical_bytes) = molecule.logical_bytes else {
                push_scope(
                    &mut unresolved_scopes,
                    &format!("missing_molecule_counter:{}", molecule.molecule_id),
                );
                continue;
            };
            let unit = units.entry(molecule.molecule_id.clone()).or_default();
            match unit.logical_bytes {
                Some(existing) if existing != logical_bytes => {
                    push_scope(
                        &mut unresolved_scopes,
                        &format!("molecule_counter_conflict:{}", molecule.molecule_id),
                    );
                    unit.logical_bytes = Some(existing.max(logical_bytes));
                }
                None => unit.logical_bytes = Some(logical_bytes),
                Some(_) => {}
            }
            if let Some(app_id) = app_id {
                unit.apps.insert(app_id.to_string());
            } else {
                unit.system_reachable = true;
            }
        }
    }

    let mut total_unique_logical_bytes = 0_u64;
    let mut unique_logical_bytes = 0_u64;
    let mut system_unique_logical_bytes = 0_u64;
    for unit in units.values() {
        let logical_bytes = unit.logical_bytes.unwrap_or(0);
        total_unique_logical_bytes = total_unique_logical_bytes.saturating_add(logical_bytes);
        if unit.apps.is_empty() {
            if unit.system_reachable {
                system_unique_logical_bytes =
                    system_unique_logical_bytes.saturating_add(logical_bytes);
            }
            continue;
        }
        unique_logical_bytes = unique_logical_bytes.saturating_add(logical_bytes);
        let shared = unit.apps.len() > 1;
        for app_id in &unit.apps {
            let bucket = app_buckets.entry(app_id.clone()).or_default();
            bucket.molecule_count = bucket.molecule_count.saturating_add(1);
            if shared {
                bucket.shared_logical_bytes =
                    bucket.shared_logical_bytes.saturating_add(logical_bytes);
                bucket.shared_molecule_count = bucket.shared_molecule_count.saturating_add(1);
            } else {
                bucket.exclusive_logical_bytes =
                    bucket.exclusive_logical_bytes.saturating_add(logical_bytes);
            }
        }
    }

    let mut apps: Vec<HomeStorageAppAttributionRow> = app_buckets
        .into_iter()
        .map(|(app_id, bucket)| {
            let inclusive_logical_bytes = bucket
                .exclusive_logical_bytes
                .saturating_add(bucket.shared_logical_bytes);
            HomeStorageAppAttributionRow {
                app_id,
                exclusive_logical_bytes: bucket.exclusive_logical_bytes,
                shared_logical_bytes: bucket.shared_logical_bytes,
                inclusive_logical_bytes,
                overlap_logical_bytes: bucket.shared_logical_bytes,
                amplification_basis_points: ratio_basis_points(
                    inclusive_logical_bytes,
                    bucket.exclusive_logical_bytes,
                ),
                molecule_count: bucket.molecule_count,
                shared_molecule_count: bucket.shared_molecule_count,
                schema_bindings: bucket.schema_bindings.into_iter().collect(),
            }
        })
        .collect();
    apps.sort_by(|left, right| {
        right
            .inclusive_logical_bytes
            .cmp(&left.inclusive_logical_bytes)
            .then_with(|| left.app_id.cmp(&right.app_id))
    });
    let inclusive_app_logical_bytes = apps.iter().fold(0_u64, |sum, row| {
        sum.saturating_add(row.inclusive_logical_bytes)
    });
    let overlap_logical_bytes = inclusive_app_logical_bytes.saturating_sub(unique_logical_bytes);

    HomeStorageInclusiveAppAttribution {
        metric: HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string(),
        measured_at,
        complete: unresolved_scopes.is_empty(),
        unique_logical_bytes,
        system_unique_logical_bytes,
        total_unique_logical_bytes,
        inclusive_app_logical_bytes,
        overlap_logical_bytes,
        amplification_basis_points: ratio_basis_points(
            inclusive_app_logical_bytes,
            unique_logical_bytes,
        ),
        pending_protein_folds,
        apps,
        unresolved_scopes,
    }
    .validated_for_read()
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        return None;
    }
    Some(
        numerator
            .saturating_mul(ONE_X_BASIS_POINTS)
            .checked_div(denominator)
            .unwrap_or(u64::MAX),
    )
}

impl HomeStorageInclusiveAppAttribution {
    #[must_use]
    pub fn validated_for_read(mut self) -> Self {
        self.metric = HOME_STORAGE_APP_ATTRIBUTION_METRIC.to_string();
        let persisted_scopes = std::mem::take(&mut self.unresolved_scopes);
        for scope in persisted_scopes {
            push_scope(&mut self.unresolved_scopes, &scope);
        }
        let mut app_ids = BTreeSet::new();
        let mut inclusive = 0_u64;
        for row in &mut self.apps {
            if !app_ids.insert(row.app_id.clone()) {
                push_scope(&mut self.unresolved_scopes, "duplicate_app_id");
            }
            let expected_inclusive = row
                .exclusive_logical_bytes
                .saturating_add(row.shared_logical_bytes);
            if row.inclusive_logical_bytes != expected_inclusive
                || row.overlap_logical_bytes != row.shared_logical_bytes
            {
                push_scope(&mut self.unresolved_scopes, "app_row_arithmetic_mismatch");
            }
            row.inclusive_logical_bytes = expected_inclusive;
            row.overlap_logical_bytes = row.shared_logical_bytes;
            row.amplification_basis_points =
                ratio_basis_points(row.inclusive_logical_bytes, row.exclusive_logical_bytes);
            row.schema_bindings.sort_unstable();
            row.schema_bindings.dedup();
            inclusive = inclusive.saturating_add(row.inclusive_logical_bytes);
        }
        let expected_total_unique = self
            .unique_logical_bytes
            .saturating_add(self.system_unique_logical_bytes);
        let expected_overlap = inclusive.saturating_sub(self.unique_logical_bytes);
        if self.total_unique_logical_bytes != expected_total_unique
            || self.inclusive_app_logical_bytes != inclusive
            || self.overlap_logical_bytes != expected_overlap
        {
            push_scope(&mut self.unresolved_scopes, "attribution_total_mismatch");
        }
        self.total_unique_logical_bytes = expected_total_unique;
        self.inclusive_app_logical_bytes = inclusive;
        self.overlap_logical_bytes = expected_overlap;
        self.amplification_basis_points = ratio_basis_points(inclusive, self.unique_logical_bytes);
        self.apps.sort_by(|left, right| {
            right
                .inclusive_logical_bytes
                .cmp(&left.inclusive_logical_bytes)
                .then_with(|| left.app_id.cmp(&right.app_id))
        });
        self.unresolved_scopes.sort_unstable();
        self.unresolved_scopes.dedup();
        self.complete &= self.unresolved_scopes.is_empty();
        self
    }
}

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

fn visit_path(home: &Path, checkpoint: &mut HomeStorageReconcileCheckpoint, encoded: &str) {
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

fn visit_child(
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

fn absolute_path(home: &Path, relative: &Path) -> PathBuf {
    if relative == Path::new(".") {
        home.to_path_buf()
    } else {
        home.join(relative)
    }
}

fn add_entry(
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

fn classify_path(relative: &Path) -> HomeStorageBucketKind {
    let first = relative.components().find_map(|component| match component {
        Component::Normal(value) => Some(value.as_bytes()),
        _ => None,
    });
    match first {
        Some(b"data" | b"lastgit-pack-cas" | b"lastgit-pack-manifests" | b"secondary") => {
            HomeStorageBucketKind::DatabaseStore
        }
        Some(b"backup" | b"backups" | b"backup-cut-freeze" | b"backup_gc_jobs" | b".backup") => {
            HomeStorageBucketKind::Backup
        }
        Some(b"recovery" | b"restore" | b"restores") => HomeStorageBucketKind::Recovery,
        Some(
            b"current"
            | b"bin"
            | b"bin-with-upload-cap"
            | b"lastdb"
            | b"lastdbd"
            | b"launchd"
            | b"watchdog.sh",
        ) => HomeStorageBucketKind::RuntimeBinary,
        Some(name) if name == b"apps" || name.starts_with(b"admin-") => {
            HomeStorageBucketKind::RuntimeApp
        }
        Some(b"ingestion_config.json") => HomeStorageBucketKind::RuntimeApp,
        Some(b"log" | b"logs" | b"crash-reports" | b"current-session.json" | b"sessions.jsonl") => {
            HomeStorageBucketKind::Log
        }
        Some(name) if name.starts_with(b"observability.") => HomeStorageBucketKind::Log,
        Some(b"candidate" | b"candidates" | b".candidates" | b".lastdb-staged") => {
            HomeStorageBucketKind::Candidate
        }
        Some(name) if name.starts_with(b"laststore_atom_rewrite") => {
            HomeStorageBucketKind::Recovery
        }
        Some(name)
            if name.starts_with(b"laststore_backup_")
                || name == b"laststore_chunk_sha_memo.json"
                || name == b"laststore_high_water.json"
                || name == b"laststore_pending_purged_atom_retirements.json" =>
        {
            HomeStorageBucketKind::Backup
        }
        Some(name) if name.starts_with(b"cloud_sync.") => HomeStorageBucketKind::DatabaseAuxiliary,
        None
        | Some(
            b".bootstrap_done"
            | b".fastembed_cache"
            | b".metadata_never_index"
            | b"identity.key"
            | b"at_rest_key"
            | b"autostart-enrolled"
            | b"cloud_sync.json"
            | b"config.json"
            | b"data.app-sock"
            | b"folddb.sock"
            | b"install_id"
            | b"lastdb.sock"
            | b"metering-webhook-secret-dev"
            | b"monitoring"
            | b"schema_resolver.json",
        ) => HomeStorageBucketKind::DatabaseAuxiliary,
        Some(_) => HomeStorageBucketKind::UnknownPath,
    }
}

fn display_root(relative: &Path) -> String {
    relative
        .components()
        .find_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            _ => None,
        })
        .unwrap_or_else(|| ".".to_string())
}

fn safe_relative(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::CurDir | Component::Normal(_)))
}

fn encode_path(path: &Path) -> String {
    fold_db::hex::hex_lower(path.as_os_str().as_bytes())
}

fn decode_path(encoded: &str) -> Result<PathBuf, ()> {
    if !encoded.len().is_multiple_of(2) || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(());
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|_| ())?;
            u8::from_str_radix(pair, 16).map_err(|_| ())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

fn push_reconcile_scope(checkpoint: &mut HomeStorageReconcileCheckpoint, scope: &str) {
    push_scope(&mut checkpoint.unresolved_scopes, scope);
}

/// Durable response body for `GET /api/storage/home`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageReport {
    pub metric: String,
    pub measured_at: DateTime<Utc>,
    /// Identifier for the filesystem/storage frontier used by reconcile.
    pub snapshot_frontier: Option<String>,
    /// True only when the persisted snapshot passes every additive invariant.
    pub complete: bool,
    /// Always false. The heavy work belongs to the explicit reconcile route.
    pub heavy: bool,
    pub home: HomeStorageTotals,
    /// One row per [`HomeStorageBucketKind`].
    pub unique_physical_buckets: Vec<HomeStorageBucket>,
    /// Bytes in the measured total but absent from the bucket sum.
    pub unaccounted_bytes: u64,
    /// Bytes in the bucket sum beyond the measured total.
    pub overaccounted_bytes: u64,
    /// Apparent-byte equivalents of the allocation residuals.
    pub unaccounted_apparent_bytes: u64,
    pub overaccounted_apparent_bytes: u64,
    /// Accounted bytes in [`HomeStorageBucketKind::UnknownPath`].
    pub unknown_path_bytes: u64,
    /// Named reasons that prevent an exact report.
    #[serde(default)]
    pub unresolved_scopes: Vec<String>,
    /// Logical data counted once per reaching app. This ledger is independent
    /// from the unique physical bucket ledger above and can exceed its own
    /// unique logical total when apps share molecules.
    #[serde(default)]
    pub inclusive_app_attribution: HomeStorageInclusiveAppAttribution,
}

impl HomeStorageReport {
    /// Empty, honest response for a home that has no persisted snapshot yet.
    #[must_use]
    pub fn missing() -> Self {
        Self {
            metric: HOME_STORAGE_METRIC.to_string(),
            measured_at: Utc::now(),
            snapshot_frontier: None,
            complete: false,
            heavy: false,
            home: HomeStorageTotals::default(),
            unique_physical_buckets: Vec::new(),
            unaccounted_bytes: 0,
            overaccounted_bytes: 0,
            unaccounted_apparent_bytes: 0,
            overaccounted_apparent_bytes: 0,
            unknown_path_bytes: 0,
            unresolved_scopes: vec!["snapshot_missing".to_string()],
            inclusive_app_attribution: HomeStorageInclusiveAppAttribution::default(),
        }
    }

    /// Build a complete snapshot and reject arithmetic or bucket-identity
    /// errors before they can become durable truth.
    pub fn new_complete(
        snapshot_frontier: impl Into<String>,
        measured_at: DateTime<Utc>,
        home: HomeStorageTotals,
        buckets: Vec<HomeStorageBucket>,
    ) -> Result<Self, String> {
        let report = Self {
            metric: HOME_STORAGE_METRIC.to_string(),
            measured_at,
            snapshot_frontier: Some(snapshot_frontier.into()),
            complete: true,
            heavy: false,
            home,
            unique_physical_buckets: buckets,
            unaccounted_bytes: 0,
            overaccounted_bytes: 0,
            unaccounted_apparent_bytes: 0,
            overaccounted_apparent_bytes: 0,
            unknown_path_bytes: 0,
            unresolved_scopes: Vec::new(),
            inclusive_app_attribution: HomeStorageInclusiveAppAttribution::default(),
        }
        .validated_for_read();
        if report.complete {
            Ok(report)
        } else {
            Err(report.unresolved_scopes.join(", "))
        }
    }

    /// Recompute every derived field and fail closed when persisted state is
    /// corrupt or incomplete. This method performs no IO.
    #[must_use]
    pub fn validated_for_read(mut self) -> Self {
        self.metric = HOME_STORAGE_METRIC.to_string();
        self.heavy = false;
        self.inclusive_app_attribution = self.inclusive_app_attribution.validated_for_read();

        let bucket_apparent = self
            .unique_physical_buckets
            .iter()
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.apparent_bytes)
            });
        let bucket_allocated = self
            .unique_physical_buckets
            .iter()
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.allocated_bytes)
            });
        self.unaccounted_bytes = self.home.allocated_bytes.saturating_sub(bucket_allocated);
        self.overaccounted_bytes = bucket_allocated.saturating_sub(self.home.allocated_bytes);
        self.unaccounted_apparent_bytes = self.home.apparent_bytes.saturating_sub(bucket_apparent);
        self.overaccounted_apparent_bytes =
            bucket_apparent.saturating_sub(self.home.apparent_bytes);
        self.unknown_path_bytes = self
            .unique_physical_buckets
            .iter()
            .filter(|bucket| bucket.kind == HomeStorageBucketKind::UnknownPath)
            .fold(0_u64, |sum, bucket| {
                sum.saturating_add(bucket.allocated_bytes)
            });

        let mut kinds = BTreeSet::new();
        let duplicate_kind = self
            .unique_physical_buckets
            .iter()
            .any(|bucket| !kinds.insert(bucket.kind));
        if duplicate_kind {
            push_scope(&mut self.unresolved_scopes, "duplicate_bucket_kind");
        }
        if self.unaccounted_bytes != 0 || self.overaccounted_bytes != 0 {
            push_scope(&mut self.unresolved_scopes, "allocated_total_mismatch");
        }
        if self.unaccounted_apparent_bytes != 0 || self.overaccounted_apparent_bytes != 0 {
            push_scope(&mut self.unresolved_scopes, "apparent_total_mismatch");
        }
        self.unresolved_scopes.sort_unstable();
        self.unresolved_scopes.dedup();
        self.complete &= self.snapshot_frontier.is_some()
            && self.unresolved_scopes.is_empty()
            && self.unaccounted_bytes == 0
            && self.overaccounted_bytes == 0
            && self.unaccounted_apparent_bytes == 0
            && self.overaccounted_apparent_bytes == 0;
        self
    }

    /// Attach a measured inclusive app ledger to this physical snapshot.
    #[must_use]
    pub fn with_inclusive_app_attribution(
        mut self,
        attribution: HomeStorageInclusiveAppAttribution,
    ) -> Self {
        self.inclusive_app_attribution = attribution.validated_for_read();
        self
    }
}

fn push_scope(scopes: &mut Vec<String>, scope: &str) {
    if scopes.iter().any(|existing| existing == scope) {
        return;
    }
    if scopes.len() < UNRESOLVED_SCOPES_MAX.saturating_sub(1) {
        scopes.push(scope.to_string());
    } else if !scopes
        .iter()
        .any(|existing| existing == "additional_unresolved_scopes")
    {
        scopes.push("additional_unresolved_scopes".to_string());
    }
}

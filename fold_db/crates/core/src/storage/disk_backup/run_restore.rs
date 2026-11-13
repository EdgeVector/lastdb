//! Verify an inactive run home without a writer, cleanup, or conversion.
mod node;
mod record;

use super::byte_backup::RunBackupProfile;
use crate::schema::schema_types::SchemaWithState;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::disk_backup::mutation_log::{Frontiers, LogAnchor, LogEntry, LogPosition};
use crate::storage::disk_pack::{native_routing, run_node, RUN_DIR};
use crate::storage::error::{
    NativeProofCause, NativeProofStage as Stage, StorageError, StorageResult,
};
use crate::storage::format::{SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE};
use cloud_backup_protocol::NativeRoot;
use lastdb_model::record_value::FileMarker;
use lastdb_model::storage_policy::backup_decode::{
    NATIVE_STREAM_LOOKUP_READ_BYTES, RECORD_BODY_MAX_BYTES,
};
use laststore::disk_run::{
    home::{ReadBudget, RunHome, RunRead, RUNS},
    MAX_RUN_KEY_BYTES,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{DirBuilder, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

pub use node::RunNodeGuard;
pub use record::{SnapshotAlternatePage, SnapshotRecordPage, SnapshotSchema};

pub struct StagedRecordQuery<'a> {
    schema: &'a str,
    key: KeyValue,
}

impl<'a> StagedRecordQuery<'a> {
    pub fn new(schema: &'a str, key: KeyValue, _include_inline: bool) -> StorageResult<Self> {
        let valid = |s: &str| (1..=MAX_RUN_KEY_BYTES).contains(&s.len());
        (valid(schema)
            && key.hash.as_deref().is_some_and(valid)
            && key
                .range
                .as_ref()
                .is_none_or(|r| r.len() <= MAX_RUN_KEY_BYTES))
        .then_some(Self { schema, key })
        .ok_or_else(|| invalid("staged record coordinates"))
    }
}

pub struct StagedRecord {
    fields: BTreeMap<String, Value>,
}

impl StagedRecord {
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }
}

pub struct RunSnapshotReader(pub(super) RunNodeGuard);

pub(super) fn sum_file_bytes(files: &[(String, u64)], msg: &'static str) -> StorageResult<u64> {
    files
        .iter()
        .try_fold(0u64, |s, (_, b)| s.checked_add(*b))
        .ok_or_else(|| invalid(msg))
}

pub fn verify(
    root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    key: &[u8; 32],
) -> StorageResult<RunNodeGuard> {
    phase(Stage::Profile, profile.validate_root(native_root))?;
    finish_guard(
        root,
        key,
        |n| &n.profile == profile,
        "run restore captured profile differs",
    )
}

pub fn verify_replayed(
    data: &Path,
    anchor: &LogAnchor,
    through: &LogPosition,
    key: &[u8; 32],
) -> StorageResult<RunNodeGuard> {
    if through.epoch != anchor.epoch {
        return Err(invalid("mutation replay epoch"));
    }
    let ok = |n: &RunNodeGuard| {
        n.profile.main.home_id == anchor.main_home
            && n.profile.main.frontier.get() == through.frontiers.main
    };
    finish_guard(data, key, ok, "mutation replay exact final frontier")
}

fn finish_guard(
    root: &Path,
    key: &[u8; 32],
    check: impl FnOnce(&RunNodeGuard) -> bool,
    msg: &'static str,
) -> StorageResult<RunNodeGuard> {
    let native = phase(Stage::PhysicalRoot, RunNodeGuard::open(root, key, false))?;
    if !check(&native) {
        return Err(invalid(msg));
    }
    native.require_empty_local_cas()?;
    phase(Stage::FinalBinding, native.verify_handles()).map(|()| native)
}

pub fn install_empty_cas_home(data_root: &Path, account_key: &[u8; 32]) -> StorageResult<()> {
    let path = data_root.join(run_node::CAS_DIR);
    if !path.try_exists()? {
        drop(RunHome::create(&path, &run_node::cas_key(account_key)).map_err(map)?);
        File::open(data_root)?.sync_all()?;
    }
    Ok(())
}

pub struct MutationReplay {
    main: RunHome,
    _node: File,
}

impl MutationReplay {
    pub fn open(data: &Path, key: &[u8; 32], anchor: &LogAnchor) -> StorageResult<Self> {
        let (_, _node, _) = node::lock_node(data, false, "mutation replay node is owned")?;
        let main =
            RunHome::open(&data.join(run_node::RUN_DIR), &run_node::home_key(key)).map_err(map)?;
        (main.identity().to_string() == anchor.main_home.as_str())
            .then_some(Self { main, _node })
            .ok_or_else(|| invalid("mutation replay native home identity"))
    }

    pub fn frontiers(&self) -> Frontiers {
        Frontiers {
            main: self.main.current_root().frontier(),
        }
    }

    pub fn settle(&self) -> StorageResult<()> {
        let before = self.frontiers();
        while self.main.merge_step().map_err(map)? {}
        (self.frontiers() == before)
            .then_some(())
            .ok_or_else(|| invalid("mutation replay compaction changed frontier"))
    }

    pub fn part(&self, entry: &LogEntry, idx: usize, source: &Path) -> StorageResult<()> {
        let part = entry
            .parts
            .get(idx)
            .ok_or_else(|| invalid("mutation replay part number"))?;
        let prev = idx
            .checked_sub(1)
            .and_then(|i| entry.parts.get(i))
            .map_or(entry.before.main, |p| p.frontier);
        let cur = self.frontiers().main;
        if cur != prev && cur != part.frontier {
            return Err(invalid("mutation replay exact partial frontier"));
        }
        let app = self
            .main
            .replay_commit_file(source, prev, part.frontier)
            .map_err(map)?;
        (app.run_id.to_string() == part.run_id.as_str()
            && app.run_bytes == part.bytes
            && self.frontiers().main == part.frontier)
            .then_some(())
            .ok_or_else(|| invalid("mutation replay original run identity or length"))
    }
}

impl RunSnapshotReader {
    pub fn open(
        path: &Path,
        account_key: &[u8; 32],
        verified_root: &NativeRoot,
    ) -> StorageResult<Self> {
        let native = RunNodeGuard::open(path, account_key, true)?;
        native.profile.validate_root(verified_root)?;
        native.require_empty_local_cas().map(|()| Self(native))
    }

    pub(super) fn lookup(&self, keys: usize) -> StorageResult<RunRead> {
        fork_bounded(&self.0.main, NATIVE_STREAM_LOOKUP_READ_BYTES, keys as u64)
    }

    pub fn read_record(
        &self,
        query: &StagedRecordQuery<'_>,
        account_key: &[u8; 32],
    ) -> StorageResult<Option<StagedRecord>> {
        let (mut read, key) = (
            self.lookup(3)?,
            native_routing::encode(query.schema.as_bytes()),
        );
        let bytes = read
            .get_bounded(SCHEMAS_NAMESPACE, &key, RECORD_BODY_MAX_BYTES)
            .map_err(map)?
            .ok_or_else(|| invalid("staged schema is absent"))?;
        let schema = record::decode_schema(&bytes, &key)?;
        let codec = record::record_codec(&schema, account_key)?;
        let address = record::record_address(&schema, &codec, &query.key)?;
        let Some(rec) = record::read_verified(&mut read, &schema, &codec, &address)? else {
            return Ok(None);
        };
        rec.is_live()
            .then_some(Some(StagedRecord { fields: rec.fields }))
            .ok_or_else(|| invalid("staged record payload differs from its key"))
    }

    pub fn seed_replay(
        &self,
        destination: &Path,
        anchor: &LogAnchor,
        pos: &LogPosition,
    ) -> StorageResult<()> {
        let native = &self.0;
        if native.profile.main.home_id != anchor.main_home
            || native.profile.main.frontier.get() != pos.frontiers.main
            || pos.epoch != anchor.epoch
        {
            return Err(invalid("cached mutation replay predecessor"));
        }
        native.check_binding()?;
        node::directory(destination)?;
        let bytes = sum_file_bytes(&native.files, "cached mutation replay bytes")?;
        let _space = laststore::space::reserve_space(
            destination,
            &laststore::space::SpacePolicy::default(),
            bytes,
        )
        .map_err(map)?;
        let (main, runs) = (
            destination.join(RUN_DIR),
            destination.join(RUN_DIR).join(RUNS),
        );
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&runs)?;
        for (rel, expected_bytes) in &native.files {
            let mut out = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(destination.join(rel))?;
            if node::stream_regular(
                &native.root.join(rel),
                &mut out,
                "cached mutation replay file length",
            )? != *expected_bytes
            {
                return Err(invalid("cached mutation replay file length"));
            }
            out.sync_all()?;
        }
        for dir in [runs.as_path(), main.as_path(), destination] {
            File::open(dir)?.sync_all()?;
        }
        native.check_binding()
    }

    pub fn schemas(&self) -> StorageResult<Vec<SnapshotSchema>> {
        const LIMIT: usize = 1024;
        let mut read = self.lookup(LIMIT * 2 + 8)?;
        let p = read
            .administrative_range(SCHEMAS_NAMESPACE, b"", &[0xff])
            .map_err(map)?
            .next_page_yielding_bounded(LIMIT + 1, 0, 0, RECORD_BODY_MAX_BYTES)
            .map_err(map)?;
        if !p.exhausted || p.resume.is_some() {
            return Err(record::resource_limit(
                "verified schema catalog exceeds its read allowance",
            ));
        }
        p.records
            .into_iter()
            .map(|(name, bytes)| {
                let schema = record::decode_schema(&bytes, &name)?;
                let state = read
                    .get_bounded(SCHEMA_STATES_NAMESPACE, &name, 256)
                    .map_err(map)?
                    .map(|b| serde_json::from_slice(&b).map_err(|_| invalid("staged schema state")))
                    .transpose()?
                    .unwrap_or_default();
                Ok(SnapshotSchema {
                    entry: SchemaWithState::new(schema, state),
                })
            })
            .collect()
    }

    pub fn inline_file(
        &self,
        schema: &Schema,
        key: &KeyValue,
        expected_revision: Option<&str>,
        account_key: &[u8; 32],
    ) -> Result<Option<(FileMarker, Vec<u8>)>, crate::error::FoldDbError> {
        let (mut read, codec) = (self.lookup(4)?, record::record_codec(schema, account_key)?);
        let address = record::checked_address(schema, &codec, key)?;
        let rec = record::read_verified(&mut read, schema, &codec, &address)?;
        let live = rec.as_ref().filter(|r| r.is_live());
        if let Some(expected) = expected_revision {
            let actual = live.map(|r| r.stamp.revision());
            if actual.as_deref() != Some(expected) {
                return Err(SchemaError::CasConflict {
                    schema: schema.name.clone(),
                    key: key.to_storage_key(),
                    expected: expected.to_owned(),
                    actual,
                }
                .into());
            }
        }
        let Some(live_rec) = live else {
            return Ok(None);
        };
        record::read_inline(&mut read, &address, live_rec)?
            .map(|f| {
                f.bytes()
                    .map(|b| (f.marker, b))
                    .map_err(|_| invalid("staged inline file bytes").into())
            })
            .transpose()
    }
}

pub(super) fn invalid(message: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(message),
    }
}

fn phase<T>(stage: Stage, result: StorageResult<T>) -> StorageResult<T> {
    result.map_err(|error| error.at_native_proof(stage))
}

pub(super) fn map(error: laststore::Error) -> StorageError {
    let cause = match error {
        laststore::Error::Corrupt(_) => NativeProofCause::NativeFormat,
        laststore::Error::AeadAuthFail => NativeProofCause::Authentication,
        laststore::Error::Config(m) if m == "disk run snapshot: another owner holds the home" => {
            NativeProofCause::Static("run restore owner lock busy")
        }
        laststore::Error::Config(_) => NativeProofCause::Configuration,
        other => return crate::storage::disk_pack::map(other),
    };
    StorageError::NativeProof {
        stage: Stage::NativeReader,
        cause,
    }
}

pub(super) fn fork_bounded(read: &RunRead, bytes: u64, keys: u64) -> StorageResult<RunRead> {
    read.fork_bounded(ReadBudget {
        bytes,
        keys,
        ..read_budget()
    })
    .map_err(map)
}

pub(super) fn read_budget() -> ReadBudget {
    use lastdb_model::storage_policy::backup_decode as b;
    ReadBudget {
        pages: b::RECORD_INVENTORY_PAGES,
        blocks: b::NATIVE_STREAM_BLOCKS,
        bytes: b::RECORD_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}

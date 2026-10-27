//! Existing files only. No native writer, startup, cleanup, or conversion.

use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::crypto::content_key_proof;
use crate::db_operations::atom_store::AtomStore;
use crate::storage::disk_pack::native_routing;
use crate::storage::disk_pack::{map, run_node, NODE_MARKER};
use laststore::disk_run::home::ReadBudget;
use laststore::disk_run::home::RunRead;
use serde::Serialize;
use std::fs::File;
use std::path::Path;

/// One read of each run home of a stopped v2 node home.
pub(super) struct Source {
    pub(super) main: RunRead,
    pub(super) cas: RunRead,
}

pub(super) struct FrozenSource {
    pub(super) source: Source,
    pub(super) bindings: Vec<super::binding::Binding>,
    _owner: File,
    key_format: String,
    minimum_replay_time: u64,
}

#[derive(Debug, Serialize)]
pub struct SourceFacts {
    pub logical_key_format: String,
    pub minimum_replay_time: u64,
    pub roots: Vec<SourceRoot>,
}

#[derive(Debug, Serialize)]
pub struct SourceRoot {
    pub home: &'static str,
    pub generation: u64,
    pub frontier: u64,
}

impl FrozenSource {
    pub(super) fn open(root: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        require_directory(root)?;
        let mut marker = super::binding::open_file(&root.join(NODE_MARKER))?;
        marker
            .try_lock()
            .map_err(|_| invalid("a node owner holds the source"))?;
        let bytes = super::binding::read(&mut marker, 128)?;
        let mut bindings = vec![super::binding::Binding::capture(&root.join(NODE_MARKER))?];
        if bytes != run_node::RUN_NODE_BYTES {
            return Err(invalid(
                "source native format is unsupported; this program reads only disk format v2",
            ));
        }
        for name in [run_node::RUN_DIR, run_node::CAS_DIR] {
            require_directory(&root.join(name))?;
            bindings.push(super::binding::Binding::capture(
                &root.join(name).join("laststore-run-home-v2"),
            )?);
            bindings.push(super::binding::Binding::capture(
                &root.join(name).join("LOCK"),
            )?);
            bindings.push(super::binding::Binding::capture(
                &root.join(name).join("CURRENT"),
            )?);
        }
        let source = Source {
            main: RunRead::open_existing(
                &root.join(run_node::RUN_DIR),
                &run_node::home_key(key),
                budget(8 * 1024 * 1024, 8),
            )
            .map_err(map)?,
            cas: RunRead::open_existing(
                &root.join(run_node::CAS_DIR),
                &run_node::cas_key(key),
                budget(8 * 1024 * 1024, 8),
            )
            .map_err(map)?,
        };
        let mut frozen = Self {
            source,
            bindings,
            _owner: marker,
            key_format: String::new(),
            minimum_replay_time: 0,
        };
        (frozen.key_format, frozen.minimum_replay_time) = frozen.verify_account(key)?;
        frozen.check_binding()?;
        Ok(frozen)
    }

    fn verify_account(&mut self, key: &[u8; 32]) -> StorageResult<(String, u64)> {
        let marker = AtomStore::CURRENT_KEY_FORMAT_KEY;
        let cutoff = "layout:v3:delete-replay-cutoff";
        let coordinates = vec![
            (
                native_routing::collection_for_key(marker.as_bytes()).into(),
                marker.into(),
            ),
            (
                native_routing::collection_for_key(cutoff.as_bytes()).into(),
                cutoff.into(),
            ),
            (
                content_key_proof::NAMESPACE.into(),
                native_routing::encode(content_key_proof::KEY),
            ),
        ];
        let values = self
            .source
            .main
            .get_many_bounded(&coordinates, 8192)
            .map_err(map)?
            .into_iter()
            .collect::<laststore::Result<Vec<_>>>()
            .map_err(map)?;
        if values.len() != 3 {
            return Err(invalid("source proof read is incomplete"));
        }
        let minimum_replay_time =
            AtomStore::validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
        let proof = values[2]
            .as_deref()
            .ok_or_else(|| invalid("source account proof is absent"))?;
        content_key_proof::verify(key, proof).map_err(|_| invalid("source account key differs"))?;
        let key_format = values[0]
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .ok_or_else(|| invalid("source logical key format is absent or invalid"))?;
        Ok((key_format.into(), minimum_replay_time))
    }

    pub(super) fn facts(&self) -> SourceFacts {
        let roots = [&self.source.main, &self.source.cas]
            .into_iter()
            .zip(["main", "cas"])
            .map(|(read, home)| SourceRoot {
                home,
                generation: read.generation(),
                frontier: read.frontier(),
            })
            .collect();
        SourceFacts {
            logical_key_format: self.key_format.clone(),
            minimum_replay_time: self.minimum_replay_time,
            roots,
        }
    }

    pub(super) fn collect(
        &mut self,
        limits: InventoryLimits,
    ) -> StorageResult<Vec<MetadataRecord>> {
        super::capture::collect(&mut self.source, limits)
    }

    pub(super) fn check_binding(&self) -> StorageResult<()> {
        self.bindings[0].check_owner(&self._owner)?;
        for binding in &self.bindings {
            binding.check()?;
        }
        Ok(())
    }

    /// The source format name in the report. Only disk format v2 opens.
    pub(super) fn format(&self) -> &'static str {
        "runs_v2"
    }
}

fn require_directory(path: &Path) -> StorageResult<()> {
    if !std::fs::symlink_metadata(path)?.is_dir() {
        return Err(invalid(
            "source native directories must be real directories",
        ));
    }
    Ok(())
}

pub(super) fn budget(bytes: usize, keys: usize) -> ReadBudget {
    ReadBudget {
        pages: 16384,
        blocks: 65536,
        bytes: bytes as u64,
        keys: keys as u64,
    }
}

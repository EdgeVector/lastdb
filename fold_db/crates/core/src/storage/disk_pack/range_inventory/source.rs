//! Existing files only. No native writer, startup, cleanup, or conversion.

use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::crypto::content_key_proof;
use crate::db_operations::atom_store::AtomStore;
use crate::storage::disk_pack::{initialization, map, run_node, NODE_MARKER};
use crate::storage::laststore::native_routing;
use laststore::disk_pack::home::{DiskHome, HomeRead};
use laststore::disk_pack::ReadBudget;
use laststore::disk_run::home::RunRead;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub(super) enum Source {
    Packed {
        home: DiskHome,
        reader: Box<HomeRead>,
    },
    Runs {
        main: RunRead,
        _cas: Box<RunRead>,
    },
}

pub(super) struct FrozenSource {
    pub(super) source: Source,
    pub(super) bindings: Vec<super::binding::Binding>,
    _owner: File,
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
        let source = if bytes == run_node::RUN_NODE_BYTES {
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
            Source::Runs {
                main: RunRead::open_existing(
                    &root.join(run_node::RUN_DIR),
                    &run_node::home_key(key),
                    budget(8 * 1024 * 1024, 8),
                )
                .map_err(map)?,
                _cas: Box::new(
                    RunRead::open_existing(
                        &root.join(run_node::CAS_DIR),
                        &run_node::cas_key(key),
                        budget(8 * 1024 * 1024, 8),
                    )
                    .map_err(map)?,
                ),
            }
        } else if bytes == initialization::NODE_BYTES {
            require_directory(&root.join("disk-pack"))?;
            let fence = std::fs::read(root.join("laststore-layout-v1"))?;
            if fence != laststore::disk_pack::home::LEGACY_FENCE_BYTES {
                return Err(invalid("source native fence differs"));
            }
            // HomeRead must not initialize a missing lifetime lock.
            let lifetime = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(root.join("disk-pack/LIFETIME_LOCK"))?;
            if !lifetime.metadata()?.is_file() {
                return Err(invalid("source lifetime lock is not a file"));
            }
            bindings.push(super::binding::Binding::capture(
                &root.join("disk-pack/CURRENT"),
            )?);
            bindings.push(super::binding::Binding::capture(
                &root.join("disk-pack/laststore-pack-home-v1"),
            )?);
            let home = DiskHome::open(&root.join("disk-pack")).map_err(map)?;
            let reader = home.read(budget(8 * 1024 * 1024, 8)).map_err(map)?;
            Source::Packed {
                home,
                reader: Box::new(reader),
            }
        } else {
            return Err(invalid("source native format is unsupported"));
        };
        let mut frozen = Self {
            source,
            bindings,
            _owner: marker,
        };
        frozen.verify_account(key)?;
        frozen.check_binding()?;
        Ok(frozen)
    }

    fn verify_account(&mut self, key: &[u8; 32]) -> StorageResult<()> {
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
        let values = match &mut self.source {
            Source::Packed { reader, .. } => {
                reader.get_many_bounded(&coordinates, 8192).map_err(map)?
            }
            Source::Runs { main, .. } => main.get_many_bounded(&coordinates, 8192).map_err(map)?,
        }
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
        if values.len() != 3 {
            return Err(invalid("source proof read is incomplete"));
        }
        AtomStore::validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
        let proof = values[2]
            .as_deref()
            .ok_or_else(|| invalid("source account proof is absent"))?;
        content_key_proof::verify(key, proof).map_err(|_| invalid("source account key differs"))
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

    pub(super) fn format(&self) -> &'static str {
        match &self.source {
            Source::Packed { .. } => "packed_v1",
            Source::Runs { .. } => "runs_v2",
        }
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

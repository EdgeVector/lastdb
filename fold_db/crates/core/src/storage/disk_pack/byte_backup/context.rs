//! The retained source differs only in reader ownership, never in file format.
use super::{invalid, standalone::StandaloneContext, PackRuntime, StorageResult};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Digest, Id, NativeRoot};
use laststore::disk_pack::{
    backup::export::{SnapshotExport, SnapshotReader},
    home::{DiskHome, HomeRead},
    Current,
};
use laststore::disk_run::home::ReadBudget;
use laststore::space::SpacePolicy;
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub(super) enum CaptureHome {
    Writer(Arc<PackRuntime>),
    Standalone(Arc<StandaloneContext>),
}
impl CaptureHome {
    /// A capture home over the existing live owner of either physical format.
    pub fn writer(runtime: &Arc<PackRuntime>) -> StorageResult<Self> {
        runtime.require_initialized()?;
        Ok(Self::Writer(Arc::clone(runtime)))
    }
    pub fn path(&self) -> &Path {
        match self {
            Self::Writer(runtime) => runtime.physical.path(),
            Self::Standalone(context) => context.home.path(),
        }
    }
    fn v1(runtime: &PackRuntime) -> &super::super::physical::V1Home {
        runtime
            .v1()
            .expect("a writer capture home is built only over a v1 home")
    }
    pub fn home(&self) -> &DiskHome {
        match self {
            Self::Writer(runtime) => &Self::v1(runtime).home,
            Self::Standalone(context) => &context.home,
        }
    }
    pub fn space_policy(&self) -> &SpacePolicy {
        match self {
            Self::Writer(runtime) => &runtime.space_policy,
            Self::Standalone(context) => &context.space,
        }
    }
    pub fn validate(&self) -> StorageResult<()> {
        if let Self::Standalone(context) = self {
            context.validate()?;
        }
        Ok(())
    }
    pub fn read(&self, source: &SnapshotExport, budget: ReadBudget) -> StorageResult<HomeRead> {
        self.validate()?;
        match self {
            Self::Writer(runtime) => {
                let v1 = Self::v1(runtime);
                source.read_retained(&v1.home, budget, &v1.read_lease)
            }
            Self::Standalone(context) => source.read_committed(&context.home, budget),
        }
        .map_err(super::super::map)
    }
    pub fn reader(&self, source: &SnapshotExport, path: &Path) -> StorageResult<SnapshotReader> {
        self.validate()?;
        match self {
            Self::Writer(runtime) => source.reader(path, &Self::v1(runtime).read_lease),
            Self::Standalone(_) => source.standalone_reader(path),
        }
        .map_err(super::super::map)
    }
    pub fn native_root(&self, current: &Current) -> StorageResult<NativeRoot> {
        self.validate()?;
        native_root(self.home(), current)
    }
}
pub(super) fn native_root(home: &DiskHome, current: &Current) -> StorageResult<NativeRoot> {
    home.validate_current(current).map_err(super::super::map)?;
    let root = NativeRoot {
        home_id: Id::try_from(home.descriptor().identity.to_string()).map_err(super::protocol)?,
        home_digest: Digest::try_from(crate::hex::hex_lower(current.home_digest))
            .map_err(super::protocol)?,
        current_sha256: bytes_sha256(&current.encode().map_err(super::super::map)?),
        generation: DecimalU64::new(current.generation),
        frontier: DecimalU64::new(current.frontier),
        required_features: DecimalU64::new(current.required_features),
    };
    super::validate_client_native_root(&root)?;
    Ok(root)
}
pub(super) fn require_resources(
    resources: lastdb_storage_policy::BackupResources,
) -> StorageResult<()> {
    let expected = lastdb_storage_policy::BACKUP_RESOURCES;
    if resources.working_memory_bytes != expected.working_memory_bytes
        || resources.decoded_values_bytes != expected.decoded_values_bytes
        || resources.stream_buffer_bytes != expected.stream_buffer_bytes
        || resources.concurrent_streams != expected.concurrent_streams
        || resources.durable_spool_bytes != expected.durable_spool_bytes
    {
        return Err(invalid(
            "byte standalone requires central backup resource policy",
        ));
    }
    Ok(())
}

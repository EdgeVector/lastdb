//! Destination-free V2 source beside an existing native writer. Only the
//! existing durable retention reference is written inside the source home.
use super::{context, invalid, store::Store, ByteBackupCapture, StorageResult};
use cloud_backup_protocol::{v2::Begin, NativeRoot};
use lastdb_storage_policy::BackupResources;
use laststore::disk_pack::home::DiskHome;
use laststore::space::SpacePolicy;
use std::{path::Path, sync::Arc};

mod capture;
mod owner;
use owner::Owner;

pub struct StandaloneByteBackupSource {
    context: Arc<StandaloneContext>,
}
pub(super) struct StandaloneContext {
    pub home: DiskHome,
    pub space: SpacePolicy,
    owner: Owner,
}
impl StandaloneByteBackupSource {
    /// Select an existing native home and a disjoint, protected external state
    /// directory. The central resource profile is explicit and remains fixed.
    pub fn open(
        source_native_root: &Path,
        state_root: &Path,
        resources: BackupResources,
        space: SpacePolicy,
    ) -> StorageResult<Self> {
        context::require_resources(resources)?;
        let owner = Owner::open(source_native_root, state_root)?;
        let home = DiskHome::open(source_native_root).map_err(super::super::map)?;
        let context = Arc::new(StandaloneContext { home, space, owner });
        context.bind()?;
        Ok(Self { context })
    }
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.context.home.descriptor().identity
    }
    /// Observe the current durable boundary without a node or writer. This does
    /// not imply that later writes remain covered by an earlier accepted cut.
    pub fn current_native_root(&self) -> StorageResult<NativeRoot> {
        self.context.validate()?;
        let current = self
            .context
            .home
            .current_boundary()
            .map_err(super::super::map)?;
        context::native_root(&self.context.home, &current)
    }
    pub async fn prepare(&self, begin: Begin, key: &[u8; 32]) -> StorageResult<ByteBackupCapture> {
        capture::prepare(Arc::clone(&self.context), begin, key, None).await
    }
    pub async fn prepare_record_backup_capture(
        &self,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        capture::prepare(Arc::clone(&self.context), begin, key, Some(recovery)).await
    }
    pub async fn resume(&self, key: &[u8; 32]) -> StorageResult<ByteBackupCapture> {
        capture::resume(Arc::clone(&self.context), key).await
    }
    /// The durable coordinator owner must prove that Begin was never submitted.
    /// A lost reply or a rejected transfer does not authorize this operation.
    pub async fn cancel_unsubmitted(&self, begin: &Begin, key: &[u8; 32]) -> StorageResult<()> {
        capture::cancel(Arc::clone(&self.context), begin.clone(), key).await
    }
}
impl StandaloneContext {
    pub fn validate(&self) -> StorageResult<()> {
        self.owner.validate()?;
        let live = DiskHome::open(self.home.path()).map_err(super::super::map)?;
        if live.descriptor() != self.home.descriptor() {
            return Err(invalid("byte standalone native identity changed"));
        }
        Ok(())
    }
    fn store(&self) -> StorageResult<Store> {
        self.validate()?;
        Store::open_external(self.owner.state_root(), &self.space)
    }
    fn bind(&self) -> StorageResult<()> {
        let store = self.store()?;
        let binding = self.owner.binding(self.home.descriptor().identity);
        if store.exists("source.json")? {
            let saved: owner::Binding = store.read("source.json")?;
            if saved != binding {
                return Err(invalid("byte standalone saved source differs"));
            }
        } else {
            if store.exists("job.json")? {
                return Err(invalid("byte standalone source binding missing"));
            }
            store.write("source.json", &binding)?;
        }
        Ok(())
    }
}
/// Metadata-only upgrade observation. This requires an existing publication
/// lock with no writer, creates no state, and holds the fence only for this read.
/// The upgrade supervisor must remain inhibited after the returned observation.
pub fn read_stopped_native_root(source_native_root: &Path) -> StorageResult<NativeRoot> {
    owner::require_source(source_native_root)?;
    let home = DiskHome::open(source_native_root).map_err(super::super::map)?;
    let current = home.stopped_boundary().map_err(super::super::map)?;
    context::native_root(&home, &current)
}

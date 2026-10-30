//! Bounded independent page budgets over one retained preload boundary.
use super::*;

impl ReadContext {
    /// `charge` is the caller's work charge. The page keeps it until it drops.
    pub(in crate::storage::disk_pack) fn preload_page<C: std::any::Any + Send + Sync>(
        runtime: &Arc<PackRuntime>,
        base: &Arc<Self>,
        charge: &Arc<C>,
    ) -> StorageResult<PackReadScope> {
        let charge: Arc<dyn std::any::Any + Send + Sync> = Arc::<C>::clone(charge);
        let call = runtime.physical_read(
            &base.current,
            super::physical::ReadMode::Ordinary(ReadBudget {
                pages: 2048,
                blocks: 4096,
                bytes: 16 * 1024 * 1024,
                keys: 4096,
            }),
        );
        Ok(PackReadScope {
            context: Arc::new(Self {
                call: Arc::new(Mutex::new(call)),
                identity: base.identity,
                failure: Arc::new(Mutex::new(None)),
                core_holds: runtime.memory.call_holds(),
                accepted: base.accepted.clone(),
                hidden_prefix: base.hidden_prefix,
                snapshots: Arc::new(snapshots::ReadSnapshots::default()),
                revision: base.revision,
                current: base.current.clone(),
                write_owner: None,
                write_reads: None,
                _preload_charge: Some(charge),
            }),
        })
    }
}

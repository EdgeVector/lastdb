//! Bounded independent page budgets over one retained preload boundary.
use super::*;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn preload_page(
        runtime: &Arc<PackRuntime>,
        base: &Arc<Self>,
        charge: &Arc<crate::resident::core_keys::CoreKeyReservation>,
    ) -> StorageResult<PackReadScope> {
        let call = runtime
            .home
            .read_from_writer(
                base.current.clone(),
                ReadBudget {
                    pages: 2048,
                    blocks: 4096,
                    bytes: 16 * 1024 * 1024,
                    keys: 4096,
                },
                &runtime.read_lease,
            )
            .map_err(map)?;
        Ok(PackReadScope {
            context: Arc::new(Self {
                call: Arc::new(Mutex::new(call)),
                identity: base.identity,
                failure: Arc::new(Mutex::new(None)),
                core_holds: Arc::new(core_holds::CoreCallHolds::new(Arc::clone(&runtime.core))),
                accepted: base.accepted.clone(),
                hidden_prefix: base.hidden_prefix,
                core_model: base.core_model.clone(),
                snapshots: Arc::new(snapshots::ReadSnapshots::default()),
                revision: base.revision,
                current: base.current.clone(),
                write_owner: None,
                _preload_charge: Some(Arc::clone(charge)),
            }),
        })
    }
}

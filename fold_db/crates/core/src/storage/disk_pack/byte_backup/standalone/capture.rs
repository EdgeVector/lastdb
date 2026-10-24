use super::super::{
    capture as common, codec::Bootstrap, context::CaptureHome, invalid, model::State, protocol,
    store::Store, ByteBackupCapture, Inner,
};
use super::{StandaloneContext, StorageResult};
use crate::storage::disk_pack::NativeCloudWorkGuard;
use cloud_backup_protocol::{bytes_sha256, v2::Begin};
use laststore::disk_pack::{backup::export::SnapshotExport, lifetime::GenerationHold};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

pub(super) async fn prepare(
    context: Arc<StandaloneContext>,
    begin: Begin,
    key: &[u8; 32],
) -> StorageResult<ByteBackupCapture> {
    begin.validate().map_err(protocol)?;
    Bootstrap::from_descriptor(&begin.descriptor)?
        .recovery
        .validate_for(&begin.job.stream.scope)
        .map_err(protocol)?;
    let admission = NativeCloudWorkGuard::acquire()?;
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::laststore::LastStoreKvStore::run_blocking(move || {
        let home = CaptureHome::Standalone(Arc::clone(&context));
        let store = context.store()?;
        common::require_v1_cleanup(context.home.path())?;
        if store.exists("job.json")? {
            let old: State = store.read("job.json")?;
            old.validate()?;
            if old.begin == begin {
                validate(&home, &old, &key)?;
                let source = if old.released {
                    None
                } else {
                    Some(
                        SnapshotExport::open(common::retain(&home, &old)?)
                            .map_err(super::super::super::map)?,
                    )
                };
                return common::finish(inner(home, store, old, source, admission), &key);
            }
            if !old.released || old.begin.job == begin.job {
                return Err(invalid("byte saved source needs exact recovery"));
            }
            common::clear_previous(&store, &old)?;
        }
        // This shared deletion guard precedes CURRENT selection and survives
        // until the externally saved intent's exact named reference is durable.
        let (current, hold) =
            GenerationHold::capture(context.home.path()).map_err(super::super::super::map)?;
        let state = common::new_state(&home, begin, &key, &current)?;
        store.write("job.json", &state)?;
        let reference = common::retain(&home, &state)?;
        drop(hold);
        let source = SnapshotExport::open(reference).map_err(super::super::super::map)?;
        common::finish(inner(home, store, state, Some(source), admission), &key)
    })
    .await
}
pub(super) async fn resume(
    context: Arc<StandaloneContext>,
    key: &[u8; 32],
) -> StorageResult<ByteBackupCapture> {
    let admission = NativeCloudWorkGuard::acquire()?;
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::laststore::LastStoreKvStore::run_blocking(move || {
        let home = CaptureHome::Standalone(Arc::clone(&context));
        let store = context.store()?;
        let state: State = store.read("job.json")?;
        validate(&home, &state, &key)?;
        let source = if state.released {
            None
        } else {
            common::retain(&home, &state)
                .and_then(|reference| {
                    SnapshotExport::open(reference).map_err(super::super::super::map)
                })
                .ok()
        };
        common::finish(inner(home, store, state, source, admission), &key)
    })
    .await
}
pub(super) async fn cancel(
    context: Arc<StandaloneContext>,
    begin: Begin,
    key: &[u8; 32],
) -> StorageResult<()> {
    let admission = NativeCloudWorkGuard::acquire()?;
    let key = zeroize::Zeroizing::new(*key);
    crate::storage::laststore::LastStoreKvStore::run_blocking(move || {
        let home = CaptureHome::Standalone(Arc::clone(&context));
        let store = context.store()?;
        if !store.exists("job.json")? {
            return Ok(());
        }
        let mut state: State = store.read("job.json")?;
        validate(&home, &state, &key)?;
        if state.begin != begin || state.terminal.is_some() {
            return Err(invalid(
                "byte local cancel selects different admitted source",
            ));
        }
        state.local_cancelled = true;
        store.write("job.json", &state)?;
        ByteBackupCapture {
            inner: Arc::new(inner(home, store, state, None, admission)),
        }
        .finish_terminal()
    })
    .await
}
fn validate(home: &CaptureHome, state: &State, key: &[u8; 32]) -> StorageResult<()> {
    common::validate_cut(home, state)?;
    if state.key_sha256 != bytes_sha256(key) {
        return Err(invalid("byte source account key differs"));
    }
    Ok(())
}
fn inner(
    home: CaptureHome,
    store: Store,
    state: State,
    source: Option<SnapshotExport>,
    admission: NativeCloudWorkGuard,
) -> Inner {
    Inner {
        home,
        store,
        state: Mutex::new(state),
        source,
        readers: AtomicUsize::new(0),
        _admission: admission,
    }
}

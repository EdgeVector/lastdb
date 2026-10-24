use super::{
    codec::Bootstrap, context::CaptureHome, invalid, model::State, protocol, store::Store,
    ByteBackupCapture, Inner, PackRuntime, StorageResult,
};
use cloud_backup_protocol::bytes_sha256;
use cloud_backup_protocol::v2::Begin;
use laststore::disk_pack::backup::export::SnapshotExport;
use laststore::disk_pack::lifetime::{DurableReference, ReferencePurpose};
use laststore::disk_pack::Current;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

impl PackRuntime {
    /// Save the exact cut and its named hold before hashing or external work.
    pub async fn prepare_byte_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        begin.validate().map_err(protocol)?;
        Bootstrap::from_descriptor(&begin.descriptor)?
            .recovery
            .validate_for(&begin.job.stream.scope)
            .map_err(protocol)?;
        if super::super::transaction::PendingBatch::current(self).is_some() {
            return Err(invalid("byte capture inside pending mutation"));
        }
        let admission = super::super::NativeCloudWorkGuard::acquire()?;
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        let key = zeroize::Zeroizing::new(*key);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            runtime.require_initialized()?;
            if runtime.failed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(invalid("byte capture requires complete writer recovery"));
            }
            require_v1_cleanup(runtime.home.path())?;
            let home = CaptureHome::Writer(Arc::clone(&runtime));
            let store = Store::open(runtime.home.path(), &runtime.space_policy)?;
            if store.exists("job.json")? {
                let old: State = store.read("job.json")?;
                old.validate()?;
                if old.begin == begin {
                    if old.key_sha256 != bytes_sha256(key.as_ref()) {
                        return Err(invalid("byte source account key differs"));
                    }
                    validate_cut(&home, &old)?;
                    drop(gate);
                    let source = if old.released {
                        None
                    } else {
                        Some(
                            SnapshotExport::open(retain(&home, &old)?)
                                .map_err(super::super::map)?,
                        )
                    };
                    return finish(
                        Inner {
                            home,
                            store,
                            state: Mutex::new(old),
                            source,
                            readers: AtomicUsize::new(0),
                            _admission: admission,
                        },
                        &key,
                    );
                }
                if !old.released || old.begin.job == begin.job {
                    return Err(invalid("byte saved source needs exact recovery"));
                }
                clear_previous(&store, &old)?;
            }
            let writer = runtime
                .writer
                .lock()
                .map_err(|_| invalid("byte writer lock"))?;
            let current = writer.current().clone();
            let state = new_state(&home, begin, &key, &current)?;
            store.write("job.json", &state)?;
            let reference = retain(&home, &state)?;
            drop(writer);
            drop(gate);
            let source = SnapshotExport::open(reference).map_err(super::super::map)?;
            let inner = Inner {
                home,
                store,
                state: Mutex::new(state),
                source: Some(source),
                readers: AtomicUsize::new(0),
                _admission: admission,
            };
            finish(inner, &key)
        })
        .await
    }
    /// Resume only the saved CURRENT. A missing source still permits exact close.
    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        let admission = super::super::NativeCloudWorkGuard::acquire()?;
        let runtime = Arc::clone(self);
        let key = zeroize::Zeroizing::new(*key);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let home = CaptureHome::Writer(Arc::clone(&runtime));
            let store = Store::open(runtime.home.path(), &runtime.space_policy)?;
            let state: State = store.read("job.json")?;
            state.validate()?;
            validate_cut(&home, &state)?;
            if state.key_sha256 != bytes_sha256(key.as_ref()) {
                return Err(invalid("byte source account key differs"));
            }
            let source = if state.released {
                None
            } else {
                retain(&home, &state)
                    .and_then(|reference| {
                        SnapshotExport::open(reference).map_err(super::super::map)
                    })
                    .ok()
            };
            let inner = Inner {
                home,
                store,
                state: Mutex::new(state),
                source,
                readers: AtomicUsize::new(0),
                _admission: admission,
            };
            finish(inner, &key)
        })
        .await
    }
}
pub(super) fn finish(inner: Inner, key: &[u8; 32]) -> StorageResult<ByteBackupCapture> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| invalid("byte source state lock"))?;
    if let Some(source) = &inner.source {
        if state.native_files != 0
            && (state.native_files != source.files().len() as u64
                || state.native_bytes != source.bytes())
        {
            return Err(invalid("byte frozen source inventory changed"));
        }
        state.native_files = source.files().len() as u64;
        state.native_bytes = source.bytes();
        inner.store.write("job.json", &*state)?;
    }
    if !state.ready && state.terminal.is_none() && !state.local_cancelled && inner.source.is_some()
    {
        super::certify::certify(&inner, key)?;
        super::planner::build(&inner, &mut state, key)?;
    }
    inner.advance(&mut state)?;
    drop(state);
    let capture = ByteBackupCapture {
        inner: Arc::new(inner),
    };
    capture.finish_terminal()?;
    Ok(capture)
}
pub(super) fn retain(home: &CaptureHome, state: &State) -> StorageResult<DurableReference> {
    let current = validate_cut(home, state)?;
    DurableReference::create_identified(
        home.home().path(),
        &current,
        ReferencePurpose::CloudRecovery,
        0,
        state.reference_id,
    )
    .map_err(super::super::map)
}
pub(super) fn validate_cut(home: &CaptureHome, state: &State) -> StorageResult<Current> {
    state.validate()?;
    let bytes: &[u8; 4096] = state
        .current_bytes
        .as_slice()
        .try_into()
        .map_err(|_| invalid("byte CURRENT length"))?;
    let current = Current::decode(bytes).map_err(super::super::map)?;
    if home.native_root(&current)? != state.native_root {
        return Err(invalid("byte source native cut differs"));
    }
    Ok(current)
}
pub(super) fn require_v1_cleanup(root: &std::path::Path) -> StorageResult<()> {
    let path = root.join("cloud-export-v1/job.json");
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let file = super::store::input(&path)?;
    use std::io::Read;
    let mut bytes = Vec::new();
    file.take(super::store::RECORD_MAX + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > super::store::RECORD_MAX {
        return Err(invalid("V1 source cleanup record bound"));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("V1 source cleanup record format"))?;
    if value["phase"]["phase"] != "terminal" || value["phase"]["source_released"] != true {
        return Err(invalid("V1 source terminal cleanup required before V2"));
    }
    Ok(())
}
pub(super) fn clear_previous(store: &Store, state: &State) -> StorageResult<()> {
    for index in 0..=state.objects {
        for name in [
            super::model::object_name(index),
            format!("payload-{index:08}.enc"),
        ] {
            store.remove(&name)?;
            store.remove(&format!("{name}.pending"))?;
        }
    }
    Ok(())
}

pub(super) fn new_state(
    home: &CaptureHome,
    begin: Begin,
    key: &[u8; 32],
    current: &Current,
) -> StorageResult<State> {
    Ok(State {
        format: 2,
        begin,
        reference_id: uuid::Uuid::new_v4(),
        current_bytes: current.encode().map_err(super::super::map)?.to_vec(),
        native_root: home.native_root(current)?,
        key_sha256: bytes_sha256(key),
        ready: false,
        objects: 0,
        verified: 0,
        native_files: 0,
        native_bytes: 0,
        metadata_pack_objects: 0,
        multi_file_pack_objects: 0,
        terminal: None,
        local_cancelled: false,
        released: false,
    })
}

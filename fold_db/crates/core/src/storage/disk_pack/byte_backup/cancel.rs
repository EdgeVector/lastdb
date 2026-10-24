//! Local-only release requires the owner's durable proof of no remote submission.
use super::{
    invalid, model::State, store::Store, ByteBackupCapture, Inner, PackRuntime, StorageResult,
};
use cloud_backup_protocol::{bytes_sha256, v2::Begin};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

impl PackRuntime {
    /// The caller must hold its owner lock and prove begin_submitted=false from
    /// durable state. This is never evidence for a submitted or unknown cloud job.
    /// No capture or CURRENT selection occurs merely to cancel unused local work.
    pub async fn cancel_unsubmitted_byte_backup_capture(
        self: &Arc<Self>,
        begin: &Begin,
        key: &[u8; 32],
    ) -> StorageResult<()> {
        let admission = super::super::NativeCloudWorkGuard::acquire()?;
        let runtime = Arc::clone(self);
        let begin = begin.clone();
        let key = zeroize::Zeroizing::new(*key);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            if !runtime
                .home
                .path()
                .join("cloud-byte-source-v2")
                .try_exists()?
            {
                return Ok(());
            }
            let store = Store::open(runtime.home.path(), &runtime.space_policy)?;
            if !store.exists("job.json")? {
                return Ok(());
            }
            let mut state: State = store.read("job.json")?;
            state.validate()?;
            if state.begin != begin
                || state.key_sha256 != bytes_sha256(key.as_ref())
                || state.terminal.is_some()
            {
                return Err(invalid(
                    "byte local cancel selects different admitted source",
                ));
            }
            super::capture::validate_cut(&runtime, &state)?;
            state.local_cancelled = true;
            store.write("job.json", &state)?;
            let capture = ByteBackupCapture {
                inner: Arc::new(Inner {
                    runtime,
                    store,
                    state: Mutex::new(state),
                    source: None,
                    readers: AtomicUsize::new(0),
                    _admission: admission,
                }),
            };
            capture.finish_terminal()
        })
        .await
    }
}

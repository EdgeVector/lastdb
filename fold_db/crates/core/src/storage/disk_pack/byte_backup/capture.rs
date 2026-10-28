//! Capture entry points over the v2 home, and the shared finish and cleanup.
use super::{
    invalid, model::State, store::Store, ByteBackupCapture, Inner, PackRuntime, StorageResult,
};
use cloud_backup_protocol::v2::Begin;
use std::sync::Arc;

impl PackRuntime {
    /// Save the exact cut and its named hold before hashing or external work.
    pub async fn prepare_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        super::run_capture::prepare(self, begin, key, recovery, false).await
    }
    /// The coordinator holds its owner lock and proves begin_submitted=false.
    /// Only an intent with no held files or saved source state can renew its cut.
    pub async fn prepare_unsubmitted_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        super::run_capture::prepare(self, begin, key, recovery, true).await
    }
    /// Resume only the saved cut. A missing source still permits exact close.
    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        super::run_capture::resume(self, key).await
    }
}
pub(super) fn finish(
    inner: Inner,
    key: &[u8; 32],
    admit_export: bool,
) -> StorageResult<ByteBackupCapture> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| invalid("byte source state lock"))?;
    if inner.available() {
        let files = inner.files()?;
        let bytes = files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.bytes))
            .ok_or_else(|| invalid("byte source file size overflow"))?;
        if state.native_files != 0
            && (state.native_files != files.len() as u64 || state.native_bytes != bytes)
        {
            return Err(invalid("byte frozen source inventory changed"));
        }
        state.native_files = files.len() as u64;
        state.native_bytes = bytes;
        inner.store.write("job.json", &*state)?;
    }
    let active = state.terminal.is_none() && !state.local_cancelled && !state.released;
    let eligible = if active {
        let validation = super::counters::require_validated(&inner);
        if admit_export {
            validation?;
            true
        } else {
            validation.is_ok()
        }
    } else {
        true
    };
    // Recovery must still obtain a handle for exact terminal/local cancellation.
    // Content APIs recheck eligibility before they expose the retained cut.
    if !state.ready && active && eligible && inner.available() {
        super::run_source::certify(&inner, key)?;
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

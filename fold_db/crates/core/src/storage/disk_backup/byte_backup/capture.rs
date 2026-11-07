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
        prepare_capturing_local_files(self, begin, key, recovery, false).await
    }
    /// The coordinator holds its owner lock and proves begin_submitted=false.
    /// Only an intent with no held files or saved source state can renew its cut.
    pub async fn prepare_unsubmitted_record_backup_capture(
        self: &Arc<Self>,
        begin: Begin,
        key: &[u8; 32],
        recovery: cloud_backup_protocol::RecoveryDescriptor,
    ) -> StorageResult<ByteBackupCapture> {
        prepare_capturing_local_files(self, begin, key, recovery, true).await
    }
    /// Resume only the saved cut. A missing source still permits exact close.
    pub async fn resume_byte_backup_capture(
        self: &Arc<Self>,
        key: &[u8; 32],
    ) -> StorageResult<ByteBackupCapture> {
        let capture = super::run_capture::resume(self, key).await?;
        // A capture that stopped after sealing local files still holds those
        // rows in the saved cut. Drop the live copies. Absent rows do nothing.
        if let Err(error) = crate::storage::local_file_ref::release_catalog(self).await {
            tracing::error!(%error, "local file reference release after backup resume");
        }
        Ok(capture)
    }
}

/// Seal local file references into cas, capture that run, then delete the
/// live rows. The hold keeps the sealed bytes. A release failure after a
/// successful capture still returns the capture: the upload can proceed, and
/// the next capture deletes the leftover live rows.
async fn prepare_capturing_local_files(
    runtime: &Arc<PackRuntime>,
    begin: Begin,
    key: &[u8; 32],
    recovery: cloud_backup_protocol::RecoveryDescriptor,
    renew_unmaterialized: bool,
) -> StorageResult<ByteBackupCapture> {
    let sealed = crate::storage::local_file_ref::materialize(runtime, key).await?;
    let prepared =
        super::run_capture::prepare(runtime, begin, key, recovery, renew_unmaterialized).await;
    if let Err(error) = crate::storage::local_file_ref::release(runtime, &sealed).await {
        tracing::error!(%error, "local file reference release after backup capture");
        if prepared.is_err() {
            return prepared;
        }
    }
    prepared
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
    if active && admit_export {
        inner.require_available()?;
    }
    // Recovery must still obtain a handle for exact terminal/local cancellation.
    // Content APIs recheck eligibility before they expose the retained cut.
    if !state.ready && active && inner.available() {
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

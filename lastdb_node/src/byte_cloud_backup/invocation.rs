use super::{model::Saved, source::Source, store::Store, BackupError};
use crate::backup_runtime::InvocationOutcome;
use fold_db::sync::auth::byte_backup::ByteBackupClient;
use std::fs::File;
use std::sync::{atomic::AtomicBool, Arc};

pub(super) fn run(
    source: Source,
    store: Store,
    mut saved: Saved,
    client: ByteBackupClient,
    permit: File,
    pause: Arc<AtomicBool>,
) -> InvocationOutcome {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        runtime.block_on(super::worker::run(
            &source, &store, &mut saved, &client, &pause,
        ))
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let outcome = match result {
        Ok(()) => InvocationOutcome::Complete,
        Err(error) => {
            let pause_state = error.pause_state();
            saved.blocked_reason = Some(error.to_string());
            match store.reopen().and_then(|fresh| {
                fresh.write("pause.json", &pause_state)?;
                fresh.save(&saved)
            }) {
                Ok(()) => InvocationOutcome::Paused(pause_state),
                Err(checkpoint) => InvocationOutcome::Paused(checkpoint.pause_state()),
            }
        }
    };
    drop(client);
    drop(store);
    drop(source);
    drop(permit);
    drop(pause);
    outcome
}

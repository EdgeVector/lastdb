//! The completion boundary owns every source and transport resource.
use super::{
    identity::Source,
    model::Saved,
    runtime::{InvocationOutcome, RunOrigin},
    store::Store,
    BackupError,
};
use fold_db::sync::auth::native_backup::NativeBackupClient;
use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub(super) fn run(
    source: Source,
    store: Store,
    mut saved: Saved,
    client: Option<NativeBackupClient>,
    permit: File,
    pause: Arc<AtomicBool>,
    origin: RunOrigin,
) -> InvocationOutcome {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        runtime.block_on(async {
            let result =
                super::worker::run(&source, &store, &mut saved, client.as_ref(), &pause).await;
            if saved.begin.is_some() && !pause.load(Ordering::Acquire) {
                if let Some(client) = &client {
                    let _ = super::usage::refresh(&store, client, &source.account).await;
                }
            }
            result
        })
        // Runtime drop joins any owned blocking feeder before source cleanup.
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let outcome = match result {
        Ok(()) => InvocationOutcome::Complete,
        Err(error) => {
            saved.blocked_reason = Some(error.label().into());
            saved.pause = Some(error.pause_state());
            saved.pause_consent_revision = match origin {
                RunOrigin::Auto { consent_revision } => Some(consent_revision),
                RunOrigin::Manual => None,
            };
            // Reconcile an uncertain prior durable replacement before the final checkpoint.
            match Store::open(&source.home, &saved.request_id).and_then(|fresh| fresh.save(&saved))
            {
                Ok(()) => InvocationOutcome::Paused(error.pause_state()),
                Err(checkpoint) => InvocationOutcome::Paused(checkpoint.pause_state()),
            }
        }
    };
    drop(client);
    drop(store);
    drop(source);
    drop(permit);
    outcome
}

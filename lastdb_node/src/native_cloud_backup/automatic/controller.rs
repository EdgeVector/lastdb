use super::{
    model::{Binding, State},
    plan::Next,
    *,
};
use std::{sync::atomic::Ordering, time::Duration};
use tracing::Instrument;

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run(host, shutdown).in_current_span())
}

async fn run(host: Arc<Host>, shutdown: Arc<AtomicBool>) {
    let Some(core) = host.db.db_ops().namespaced_store().disk_pack_runtime() else {
        return;
    };
    // Subscribe before any getter so publication during initial reconciliation is visible.
    let mut boundaries = core.subscribe_native_backup_boundary();
    let mut workers = host.native_backup.subscribe();
    loop {
        boundaries.borrow_and_update();
        workers.borrow_and_update();
        if shutdown.load(Ordering::Acquire) || host.native_backup.snapshot().shutting_down {
            return;
        }
        let delay = match tick(&host).await {
            Ok(Next::Again) => continue,
            Ok(Next::Idle(delay)) => delay,
            Ok(Next::Invoke { .. }) => unreachable!(),
            Err(error) => match save_local_failure(&host, &error) {
                Ok(delay) => delay,
                Err(_) => {
                    tracing::warn!(
                        reason = "automatic_backup_local_state_refused",
                        "automatic backup paused"
                    );
                    None
                }
            },
        };
        tokio::select! {
            changed = boundaries.changed() => { if changed.is_err() { return; } },
            changed = workers.changed() => { if changed.is_err() { return; } },
            () = tokio::time::sleep(delay.unwrap_or(Duration::from_secs(3600))) => {},
        }
    }
}

async fn tick(host: &Host) -> Result<Next> {
    let next = plan::next(host)?;
    let Next::Invoke {
        revision,
        id,
        binding,
    } = next
    else {
        return Ok(next);
    };
    let result = invoke(host, revision, &id, &binding).await;
    if let Err(error) = result {
        save_invocation_failure(host, revision, &id, &error)?;
    }
    Ok(Next::Again)
}

async fn invoke(host: &Host, revision: u64, id: &Id, binding: &Binding) -> Result<()> {
    let (actual, client) = preflight::check(host).await?;
    if &actual != binding {
        return Err(BackupError::Identity);
    }
    // Use Start with the already durable UUID even when the selector write was lost.
    // The normal control path loads that exact saved state and resumes its frozen cut.
    super::super::control::dispatch(
        host,
        super::super::NativeBackupRequest::Start {
            request_id: id.clone(),
        },
        RunOrigin::Auto {
            consent_revision: revision,
        },
        client,
    )?;
    plan::bind_started(host, revision, id)
}

fn save_invocation_failure(host: &Host, revision: u64, id: &Id, error: &BackupError) -> Result<()> {
    let _transition = host.native_backup.transition()?;
    let mut state = State::load(&host.home)?;
    // A late preflight or response cannot undo Off, Close, or a new explicit consent.
    if !state.enabled || state.revision != revision || state.scheduled.as_ref() != Some(id) {
        return Ok(());
    }
    local::record_pause(&mut state, error.pause_state())?;
    state.save(&host.home)
}

fn save_local_failure(host: &Host, error: &BackupError) -> Result<Option<Duration>> {
    let _transition = host.native_backup.transition()?;
    let mut state = State::load(&host.home)?;
    if state.enabled {
        local::record_pause(&mut state, error.pause_state())?;
        state.save(&host.home)?;
        return state
            .retry_at_ms
            .map(|deadline| {
                model::now_ms()
                    .map(|now| Duration::from_millis(deadline.saturating_sub(now).min(30_000)))
            })
            .transpose();
    }
    Ok(None)
}

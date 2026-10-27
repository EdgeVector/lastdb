//! One weak-runtime cleanup worker; durable pending requests survive shutdown.

use super::*;
use tracing::Instrument;

const STEP_PAUSE: std::time::Duration = std::time::Duration::from_millis(25);
const HELD_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);
const RETRY_PAUSE: std::time::Duration = std::time::Duration::from_secs(30);

impl PackRuntime {
    /// Resume a saved request without creating work when no request is active.
    /// The worker releases its strong runtime reference before every pause.
    pub fn resume_native_cleanup(self: &Arc<Self>) {
        if !self.cleanup_job.get().active() || self.cleanup_job.worker.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let weak = Arc::downgrade(self);
        let job = Arc::clone(&self.cleanup_job);
        tokio::spawn(
            async move {
                {
                    let _running = Running(Arc::clone(&job));
                    while let Some(runtime) = weak.upgrade() {
                        let delay = match runtime.cleanup_tick().await {
                            Ok(status) if !status.active() => break,
                            Ok(status) if status.phase == NativeCleanupPhase::WaitingForReads => {
                                HELD_PAUSE
                            }
                            Ok(_) => STEP_PAUSE,
                            Err(error) => {
                                let terminal = runtime.requires_recovery()
                                    || runtime.physical.home.is_poisoned()
                                    || runtime.physical.cas.is_poisoned();
                                let mut status = job.get();
                                status.complete = false;
                                status.phase = if terminal {
                                    NativeCleanupPhase::RecoveryRequired
                                } else {
                                    NativeCleanupPhase::Retrying
                                };
                                status.last_error = Some(message(&error));
                                job.set(status);
                                if terminal {
                                    break;
                                }
                                RETRY_PAUSE
                            }
                        };
                        drop(runtime);
                        tokio::time::sleep(delay).await;
                    }
                }
                // A request can arrive just before the earlier worker clears its
                // mark. Recheck after the clear so that request cannot lose a wake.
                if job.get().active() {
                    if let Some(runtime) = weak.upgrade() {
                        runtime.resume_native_cleanup();
                    }
                }
            }
            .in_current_span(),
        );
    }
}

struct Running(Arc<CleanupJob>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.worker.store(false, Ordering::Release);
    }
}

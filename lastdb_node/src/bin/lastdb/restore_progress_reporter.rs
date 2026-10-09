//! Best-effort progress delivery. A blocked stderr sink cannot block restore or exit.
use fold_db::sync::engine::{RestorePhase, RestoreProgress, RestoreProgressSnapshot};
use observability::progress_reporter;
use std::sync::Arc;

pub(super) struct Reporter {
    pub progress: Arc<RestoreProgress>,
    reporter: progress_reporter::Reporter<RestoreProgressSnapshot>,
}

impl Reporter {
    pub fn stderr() -> Self {
        Self::new(|snapshot| {
            use std::io::Write;
            if let Ok(line) = serde_json::to_string(snapshot) {
                let _ = writeln!(std::io::stderr().lock(), "{line}");
            }
        })
    }

    fn new(mut sink: impl FnMut(&RestoreProgressSnapshot) + Send + 'static) -> Self {
        let (send, events) = progress_reporter::channel();
        let progress = Arc::new(RestoreProgress::new(move |snapshot| {
            send.notify(snapshot.clone());
        }));
        let reporter_progress = Arc::clone(&progress);
        let worker = events.start(
            "restore-progress",
            move || reporter_progress.snapshot(),
            |snapshot| {
                matches!(
                    snapshot.phase,
                    RestorePhase::Complete | RestorePhase::Failed
                )
            },
            move |snapshot| sink(snapshot),
        );
        let reporter = Self {
            progress,
            reporter: worker,
        };
        reporter.progress.emit();
        reporter
    }

    pub fn finish(self, success: bool) {
        self.progress.phase(if success {
            RestorePhase::Complete
        } else {
            RestorePhase::Failed
        });
        // Delivery is best effort. Final stdout JSON is the authoritative result.
        self.reporter.finish();
    }
}

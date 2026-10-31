//! The short rest between two maintenance chunks.
//!
//! A shutdown ends the rest at the next slice. A long rest ends early when no
//! other task has opened a write scope within the quiet period.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::db_operations::WriterWatch;

const SLICE: Duration = Duration::from_millis(250);

/// Rest for `pause`. The slice is 250 ms.
pub(super) async fn rest(pause: Duration, shutdown: &AtomicBool, watch: &mut WriterWatch) {
    let end = Instant::now() + pause;
    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        tokio::time::sleep(left.min(SLICE)).await;
        if left > SLICE && watch.rest_may_end() {
            return;
        }
    }
}

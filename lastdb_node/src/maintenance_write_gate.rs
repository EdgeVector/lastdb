//! Local maintenance uses the same mutation admission as foreground writes.

use crate::Host;
use std::future::Future;

pub async fn run<F: Future>(host: &Host, future: F) -> Option<F::Output> {
    let admission = host.db.acquire_local_mutation_admission().await;
    Some(admission.run(Box::pin(future)).await)
}

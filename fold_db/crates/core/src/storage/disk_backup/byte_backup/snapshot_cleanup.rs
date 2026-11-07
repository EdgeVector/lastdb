//! Prepare a new running snapshot without holding up native writes during merges.

use super::{PackRuntime, StorageError, StorageResult};
use crate::storage::disk_pack::{map, run_blocking, WriteGuard};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_ROUNDS: usize = 64;

pub(super) async fn enter_clean_cut(runtime: &Arc<PackRuntime>) -> StorageResult<WriteGuard> {
    let deadline = Instant::now() + PASS_BUDGET;
    for round in 0..=MAX_ROUNDS {
        if clean(runtime) {
            let gate = runtime.enter_write().await;
            // A write could have published a new run while we acquired the
            // gate. A merge alone cannot bring back a hidden payload.
            if clean(runtime) {
                return Ok(gate);
            }
        }
        if round == MAX_ROUNDS || Instant::now() >= deadline {
            break;
        }
        let runtime = Arc::clone(runtime);
        run_blocking(move || merge_round(&runtime)).await?;
        tokio::task::yield_now().await;
    }
    Err(StorageError::SnapshotCompactionPending)
}

fn clean(runtime: &PackRuntime) -> bool {
    runtime.physical.home.snapshot_has_clean_runs()
        && runtime.physical.cas.snapshot_has_clean_runs()
}

fn merge_round(runtime: &PackRuntime) -> StorageResult<()> {
    let cas_policy = laststore::space::SpacePolicy::default();
    let homes = [
        (&runtime.physical.home, &runtime.space_policy),
        (&runtime.physical.cas, &cas_policy),
    ];
    for (home, policy) in homes {
        if !home.snapshot_has_clean_runs()
            && home
                .snapshot_cleanup_step_with_space(policy)
                .map_err(map)?
                .is_some()
        {
            runtime.adopt_merged_root();
        }
    }
    Ok(())
}

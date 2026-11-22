//! Phase 1 signed-history DEV scenarios (7 proofs).

mod blob;
mod converge;
mod fixture;
mod forge;
mod nongrantee;
mod offchain_revoke;
mod restore;
mod revoke;

use std::path::Path;

use crate::relay::RelayHandle;
use crate::report::{ScenarioResult, ScenarioRun};

/// Run every scenario against the grant-gating reference relay.
pub async fn run_all(relay: &RelayHandle, blob_bytes: u64, work: &Path) -> Vec<ScenarioResult> {
    let mut out = Vec::with_capacity(7);
    out.push(run_named("forge_reject", forge::run(relay)).await);
    out.push(run_named("revoke_cutoff", revoke::run(relay)).await);
    out.push(run_named("offchain_revoke", async { offchain_revoke::run() }).await);
    out.push(
        run_named(
            "converge",
            converge::run(relay, &work.join("converge-stores")),
        )
        .await,
    );
    out.push(
        run_named(
            "restore_replay",
            restore::run(relay, &work.join("restore-store")),
        )
        .await,
    );
    out.push(run_named("nongrantee_reject", nongrantee::run(relay)).await);
    out.push(run_named("blob_perf", blob::run(relay, blob_bytes)).await);
    out
}

async fn run_named(
    name: &'static str,
    fut: impl std::future::Future<Output = Result<String, String>>,
) -> ScenarioResult {
    let run = ScenarioRun::start(name);
    run.finish(fut.await)
}

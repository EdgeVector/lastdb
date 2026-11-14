//! Stage a verified personal recovery without changing the open database.

mod install;

pub use install::install_prepared_native_recovery;

use std::sync::Arc;

use crate::host::Host;

use super::cloud::CloudLog;
use super::run::Ready;
use super::wire::NativeCut;
use super::{owner_recovery, snapshot};

pub(super) fn supports(host: &Host, ready: &Ready) -> bool {
    ready.publisher.is_none()
        && matches!(host.owned.is_private_id(&ready.database_id), Ok(true))
        && matches!(
            host.owned.registered_store(&ready.database_id),
            Ok((crate::host::owned::Resolved::Owner, _))
        )
        && !fold_db::db_operations::search_index::queue_policy::enabled()
}

pub(super) async fn prepare(
    host: &Host,
    log: &CloudLog,
    after: u64,
    cut: &NativeCut,
) -> Result<(), String> {
    let origin = install::freeze(host, log, after).await?;
    let stage = install::stage_home(host)?;
    let base = snapshot::restore_cut(stage.path(), log, cut).await?;
    let staged = Arc::new(Host::boot_for_snapshot_restore(stage.path()).await?);
    let recovery = async {
        let through = owner_recovery::recover_cut(&staged, log, base).await?;
        install::bind_stage_upload(&staged, &origin).await?;
        Ok::<_, String>(through)
    }
    .await;
    let shutdown = staged.db.shutdown().await;
    drop(staged);
    let through = recovery?;
    shutdown.map_err(|error| error.to_string())?;
    install::prepare(host, log, origin, stage, cut, through).await
}

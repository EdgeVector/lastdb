//! Read the exact signed files covering one personal database cutoff.

use std::path::Path;

use cloud_backup_protocol::Id;

use super::{install_base, transport, RestoredBase};
use crate::log_sync::{cloud::CloudLog, wire::NativeCut};

pub(in crate::log_sync) async fn restore_cut(
    home: &Path,
    log: &CloudLog,
    cut: &NativeCut,
) -> Result<RestoredBase, String> {
    let database = Id::try_from(log.database_id().to_owned()).map_err(|error| error.to_string())?;
    let manifest = transport::exact(log, &cut.native_snapshot_id).await?;
    if manifest.through != cut.snapshot_through || manifest.name.is_some() {
        return Err("snapshot does not cover the requested personal database cutoff".into());
    }
    install_base(home, &database, log, &cut.native_snapshot_id, &manifest).await
}

//! Replay a fixed signed owner prefix into an isolated new home.

use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::{CloudScope, Id};
use mutation_log::{ListPage, LogError, MutationLog};

use crate::database_replay;
use crate::host::Host;

use super::cloud::{CloudLog, LogOpen};
use super::cutover::ServiceView;
use super::http::{self, Endpoint};
use super::registry::Database;
use super::{apply, restore, session};

/// Restore an owned log into a private store with the same cloud id.
/// The caller publishes the isolated home after successful shutdown.
pub async fn recover_owned(
    host: &Arc<Host>,
    connection_home: &Path,
    scope: CloudScope,
    database_id: &Id,
    name: &str,
) -> Result<u64, String> {
    if scope.account_id.as_str() != host.user_hash {
        return Err("signed recovery account differs from the recovery key".into());
    }
    let selected = http::selected_service(connection_home)?;
    let device = session::device_id(&host.home)?;
    let log = CloudLog::open(LogOpen {
        host,
        endpoint: Endpoint::open(&selected.api_url, selected.label)?,
        scope,
        database_id: database_id.clone(),
        device: &device,
        member_revision: 0,
        environment_label: selected.label,
    })?;
    let database = Database {
        id: log.database_id().to_owned(),
        kind: "personal".to_owned(),
        spec: None,
        native: true,
    };
    if !matches!(log.service_view(&database).await, ServiceView::Native) {
        return Err("the signed cloud database is unavailable to this owner".into());
    }
    let through = head(&log).await?;
    host.owned
        .register_recovered(&host.home, Arc::clone(&host.keypair), name, database_id)
        .await
        .map_err(|error| error.status().1)?;
    apply_through(&log, through).await?;
    session::store_cursor(&host.home, database_id.as_str(), through)?;
    session::mark_prefix(&host.home, database_id.as_str(), through)?;
    Ok(through)
}

async fn head(log: &CloudLog) -> Result<u64, String> {
    let log = log.clone();
    tokio::task::spawn_blocking(move || log.head(log.principal()))
        .await
        .map_err(|_| "signed recovery head did not complete")?
        .map(|head| head.map_or(0, |head| head.cursor))
        .map_err(|error| error.to_string())
}

async fn read_page(log: &CloudLog, after: u64) -> Result<ListPage, LogError> {
    let log = log.clone();
    tokio::task::spawn_blocking(move || {
        log.list(log.principal(), after, mutation_log::LIST_PAGE_LIMIT, 0)
    })
    .await
    .map_err(|_| LogError::UnexpectedOutcome)?
}

async fn apply_through(log: &CloudLog, through: u64) -> Result<(), String> {
    let mut cursor = 0u64;
    let mut recovered_snapshot = false;
    let mut files = restore::file_source(log.clone());
    while cursor < through {
        let page = match read_page(log, cursor).await {
            Ok(page) => page,
            Err(LogError::CursorExpired { .. }) if !recovered_snapshot => {
                let installed = restore::install(log.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                if installed <= cursor || installed > through {
                    return Err("signed recovery snapshot moved past the selected head".into());
                }
                cursor = installed;
                recovered_snapshot = true;
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        let mut delivered = Vec::new();
        let expected = page.next.min(through);
        for stored in page
            .entries
            .iter()
            .take_while(|entry| entry.delivery_seq <= through)
        {
            if stored.delivery_seq != cursor.saturating_add(1) {
                return Err("signed recovery log has a delivery gap".into());
            }
            cursor = stored.delivery_seq;
            delivered.push(apply::delivered_entry(stored));
        }
        if delivered.is_empty() || cursor != expected {
            return Err("signed recovery log stopped before the selected head".into());
        }
        database_replay::replay_delivered(
            log.host(),
            log.database_id(),
            log.principal().as_str(),
            &delivered,
            &mut files,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

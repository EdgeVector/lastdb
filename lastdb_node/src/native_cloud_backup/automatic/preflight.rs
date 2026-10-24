use super::{model::Binding, *};
use cloud_backup_protocol::{
    context::ContextReadRequest,
    publication::{HeadReadRequest, HeadReadResult},
    Version,
};
use fold_db::sync::auth::native_backup::NativeBackupClient;

/// The exact checked client travels into the admitted worker; no second load.
pub(super) async fn check(host: &Host) -> Result<(Binding, NativeBackupClient)> {
    super::super::require_enabled()?;
    let source = super::super::identity::Source::from_host(host)?;
    let client = crate::native_backup_access::client_from_home(&host.home)
        .map_err(|_| BackupError::Identity)?;
    let context = client
        .context(&ContextReadRequest { version: Version }, &source.account)
        .await?;
    if !context.new_work_enabled || !context.open_for_writes {
        return Err(BackupError::Cloud(
            fold_db::sync::auth::native_backup::NativeBackupError::Protocol(
                cloud_backup_protocol::ProtocolError {
                    code: cloud_backup_protocol::ErrorCode::Unauthorized,
                    field: "auto.context".to_owned(),
                },
            ),
        ));
    }
    Ok((
        Binding {
            scope: context.scope,
            source_home: source.source_home,
            endpoint: client.canonical_endpoint(),
        },
        client,
    ))
}

pub(super) async fn reconcile_head(
    host: &Host,
    binding: &Binding,
    client: &NativeBackupClient,
) -> Result<Option<cloud_backup_protocol::publication::AcceptedBackup>> {
    let home = &host.home;
    let Some(accepted) = local::accepted(home)? else {
        return Ok(None);
    };
    if accepted.job.stream.scope != binding.scope
        || accepted.job.stream.source_home != binding.source_home
    {
        return Err(BackupError::Identity);
    }
    let response = client
        .head(&HeadReadRequest {
            version: Version,
            stream: accepted.job.stream.clone(),
        })
        .await?;
    match response {
        HeadReadResult::Ok { response } => {
            if response.accepted.as_ref() == Some(&accepted) {
                return Ok(Some(accepted));
            }
            let backup = response.accepted.ok_or(BackupError::Identity)?;
            let mut state = State::load(home)?;
            plan::prefer_active(host, &mut state, binding, false)?;
            let id = state
                .scheduled
                .or(Store::active(home)?)
                .ok_or(BackupError::Identity)?;
            local::pending_head(home, binding, &backup, &id)?;
            Ok(Some(backup))
        }
        HeadReadResult::Error { error } => Err(BackupError::Cloud(
            fold_db::sync::auth::native_backup::NativeBackupError::Protocol(error),
        )),
    }
}

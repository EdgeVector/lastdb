use super::{account_from_phrase, client_from_home, enabled, path_text};
use crate::native_cloud_client::NativeCloudRestoreSource;
use crate::native_cloud_restore::{RecoveryKeys, RestoreReport, RestoreSession};
use cloud_backup_protocol::context::ContextReadRequest;
use cloud_backup_protocol::publication::{
    AcceptedBackup, CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult,
};
use cloud_backup_protocol::{BackupJob, BackupStream, DecimalU64, Id, Version};
use std::path::{Path, PathBuf};

pub enum NativeRestoreRequest {
    Begin {
        stream: Id,
        source_home: Id,
        generation: Option<DecimalU64>,
        commit: Option<Id>,
        destination: PathBuf,
    },
    Resume {
        session: PathBuf,
    },
}

/// The exact selection is on disk before the caller starts any object request.
pub struct NativeRestore {
    session: RestoreSession,
    source: NativeCloudRestoreSource,
    keys: RecoveryKeys,
}

impl NativeRestore {
    pub fn path(&self) -> &Path {
        self.session.path()
    }

    pub async fn run(&mut self) -> Result<RestoreReport, String> {
        enabled()?;
        self.session
            .restore(&self.source, &self.keys)
            .await
            .map_err(|error| {
                format!(
                    "native restore retains session {}: {error}",
                    path_text(self.session.path())
                )
            })
    }
}

/// Credentials can come from a paused home; the phrase supplies the expected owner.
pub async fn prepare_restore(
    home: &Path,
    phrase: &str,
    request: NativeRestoreRequest,
) -> Result<NativeRestore, String> {
    enabled()?;
    let account = account_from_phrase(phrase)?;
    let client = client_from_home(home)?;
    let context = client
        .context(
            &ContextReadRequest {
                version: Version::V1,
            },
            &account,
        )
        .await
        .map_err(|error| error.to_string())?;
    let session = match request {
        NativeRestoreRequest::Resume { session } => {
            RestoreSession::resume(&session).map_err(|error| error.to_string())?
        }
        NativeRestoreRequest::Begin {
            stream,
            source_home,
            generation,
            commit,
            destination,
        } => {
            let mut scope = context.scope;
            if let Some(generation) = generation {
                scope.account_generation = generation;
            }
            let stream = BackupStream {
                scope,
                stream_id: stream,
                source_home,
            };
            let accepted = selected(&client, stream, commit).await?;
            RestoreSession::begin(&destination, accepted).map_err(|error| error.to_string())?
        }
    };
    let keys = RecoveryKeys::from_phrase(phrase, session.accepted()).map_err(|error| {
        format!(
            "native restore retains session {}: {error}",
            path_text(session.path())
        )
    })?;
    Ok(NativeRestore {
        session,
        source: NativeCloudRestoreSource::new(client),
        keys,
    })
}

async fn selected(
    client: &fold_db::sync::auth::native_backup::NativeBackupClient,
    stream: BackupStream,
    commit: Option<Id>,
) -> Result<AcceptedBackup, String> {
    if let Some(commit_id) = commit {
        let request = CommitStatusRequest {
            version: Version::V1,
            job: BackupJob { stream, commit_id },
        };
        return match client
            .commit_status(&request)
            .await
            .map_err(|error| error.to_string())?
        {
            CommitResponse::Accepted { backup } => Ok(*backup),
            _ => Err("selected native backup is not accepted".into()),
        };
    }
    let request = HeadReadRequest {
        version: Version::V1,
        stream,
    };
    match client
        .head(&request)
        .await
        .map_err(|error| error.to_string())?
    {
        HeadReadResult::Ok { response } => response
            .accepted
            .ok_or_else(|| "selected stream has no accepted backup".into()),
        HeadReadResult::Error { error } => Err(error.to_string()),
    }
}

use super::*;
use cloud_backup_protocol::v2::{
    context::ContextReadRequest,
    discovery::StreamsListRequest,
    publication::{CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult},
    AcceptedBackup, Job, Stream, Version,
};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Text};
use fold_db::sync::auth::byte_backup::ByteBackupClient;
use futures::{StreamExt, TryStreamExt};
use lastdb_node::byte_cloud_restore::{RecoveryKeys, RestoreSession};
use std::io::Read;

struct Selection {
    stream: Option<String>,
    generation: Option<u64>,
    commit: Option<String>,
    commit_sha256: Option<String>,
    destination: Option<PathBuf>,
    resume: Option<PathBuf>,
}

pub(super) fn run(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    lastdb_node::byte_cloud_backup::require_enabled().map_err(|e| e.to_string())?;
    let phrase = phrase()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct V2 recovery runtime")?;
    runtime.block_on(async {
        let account = lastdb_node::backup_access::account_from_phrase(&phrase)?;
        let client = lastdb_node::byte_cloud_backup::client_from_home(home)?;
        let context = client
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id: account,
                required_destinations: Vec::new(),
            })
            .await
            .map_err(|e| e.to_string())?;
        match action {
            ByteBackupCommand::Discover {
                generation,
                cursor,
                json,
                ..
            } => {
                let scope = generation_scope(context.scope, generation);
                discover(&client, scope, cursor, json).await
            }
            ByteBackupCommand::Restore {
                stream,
                generation,
                commit,
                commit_sha256,
                into,
                resume,
                json,
                ..
            } => {
                let selection = Selection {
                    stream,
                    generation,
                    commit,
                    commit_sha256,
                    destination: into,
                    resume,
                };
                restore(&client, context.scope, &phrase, selection, json).await
            }
            _ => Err("invalid offline V2 command".into()),
        }
    })
}

fn generation_scope(mut scope: CloudScope, generation: Option<u64>) -> CloudScope {
    if let Some(generation) = generation {
        scope.account_generation = DecimalU64::new(generation);
    }
    scope
}

async fn discover(
    client: &ByteBackupClient,
    scope: CloudScope,
    cursor: Option<String>,
    json: bool,
) -> Result<(), String> {
    let request = StreamsListRequest {
        version: Version::V2,
        scope: scope.clone(),
        limit: 100,
        cursor: cursor
            .map(Text::try_from)
            .transpose()
            .map_err(|_| "invalid V2 discovery cursor")?,
    };
    let directory = client.list(&request).await.map_err(|e| e.to_string())?;
    let entries: Vec<_> = futures::stream::iter(directory.streams)
        .map(|stream| async move {
            let request = HeadReadRequest {
                version: Version::V2,
                stream,
            };
            match client.head(&request).await.map_err(|e| e.to_string())? {
                HeadReadResult::Ok { response } => Ok(serde_json::json!({
                    "stream": response.stream, "accepted": response.accepted,
                    "active_commit_id": response.active_commit_id,
                })),
                HeadReadResult::Error { error } => Err(error.to_string()),
            }
        })
        .buffered(4)
        .try_collect()
        .await?;
    print(
        &serde_json::json!({"version":2,"scope":scope,"streams":entries,
        "next_cursor":directory.next_cursor}),
        json,
    )
}

async fn restore(
    client: &ByteBackupClient,
    scope: CloudScope,
    phrase: &str,
    selection: Selection,
    json: bool,
) -> Result<(), String> {
    let mut session = if let Some(path) = selection.resume {
        if selection.stream.is_some()
            || selection.generation.is_some()
            || selection.commit.is_some()
            || selection.commit_sha256.is_some()
            || selection.destination.is_some()
        {
            return Err("resume cannot change the saved V2 selection".into());
        }
        RestoreSession::resume(&path).map_err(|e| e.to_string())?
    } else {
        let stream = Stream {
            scope: generation_scope(scope.clone(), selection.generation),
            stream_id: id(selection.stream.ok_or("V2 restore requires --stream")?)?,
        };
        let accepted = accepted(client, stream, selection.commit.map(id).transpose()?).await?;
        if let Some(expected) = selection.commit_sha256 {
            let expected = Digest::try_from(expected).map_err(|_| "invalid V2 commit SHA256")?;
            if accepted.commit_sha256 != expected {
                return Err("V2 accepted commit digest differs".into());
            }
        }
        require_format_v2(&accepted)?;
        RestoreSession::begin(
            &selection.destination.ok_or("V2 restore requires --into")?,
            accepted,
        )
        .map_err(|e| e.to_string())?
    };
    if session.accepted().job().stream.scope.account_id != scope.account_id {
        return Err("V2 restore account differs".into());
    }
    require_format_v2(session.accepted())?;
    let keys =
        RecoveryKeys::from_scope_phrase(phrase, &scope.account_id).map_err(|e| e.to_string())?;
    eprintln!("V2 restore session: {}", session.path().display());
    let report = session.restore(client, &keys).await.map_err(|e| {
        format!(
            "V2 restore retains session {}: {e}",
            session.path().display()
        )
    })?;
    print(
        &serde_json::to_value(report).map_err(|_| "cannot encode V2 restore result")?,
        json,
    )
}

async fn accepted(
    client: &ByteBackupClient,
    stream: Stream,
    commit: Option<Id>,
) -> Result<AcceptedBackup, String> {
    if let Some(commit_id) = commit {
        let request = CommitStatusRequest {
            version: Version::V2,
            job: Job { stream, commit_id },
        };
        return match client
            .commit_status(&request)
            .await
            .map_err(|e| e.to_string())?
        {
            CommitResponse::Accepted { backup } => Ok(*backup),
            _ => Err("selected V2 commit is not accepted".into()),
        };
    }
    match client
        .head(&HeadReadRequest {
            version: Version::V2,
            stream,
        })
        .await
        .map_err(|e| e.to_string())?
    {
        HeadReadResult::Ok { response } => response
            .accepted
            .ok_or_else(|| "V2 stream has no accepted backup".into()),
        HeadReadResult::Error { error } => Err(error.to_string()),
    }
}

/// A backup of a disk format v1 pack home has a client descriptor. A v1 record
/// backup has a null descriptor; the restore refuses its version 2 root after
/// the root download (`byte_cloud_restore::closure`).
fn require_format_v2(accepted: &AcceptedBackup) -> Result<(), String> {
    if accepted.descriptor().as_value().is_null() {
        return Ok(());
    }
    Err(
        "V2 restore refuses a backup of a disk format v1 home; this program restores only \
         disk format v2"
            .into(),
    )
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "V2 selection requires a canonical UUID".into())
}

fn phrase() -> Result<zeroize::Zeroizing<String>, String> {
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read recovery phrase")?;
    if bytes.len() > 4096 {
        return Err("recovery phrase exceeds its byte limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "recovery phrase must be UTF8")?
        .trim();
    if text.is_empty() {
        return Err("recovery phrase is empty".into());
    }
    Ok(zeroize::Zeroizing::new(text.to_owned()))
}

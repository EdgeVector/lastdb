use super::*;
use cloud_backup_protocol::v2::{
    context::ContextReadRequest,
    discovery::StreamsListRequest,
    publication::{CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResult},
    retention::{recovery_closure, RecoveryPoint, MAX_DIRECTORY_BYTES, MAX_POINTS},
    AcceptedBackup, Head, Job, Stream, Version,
};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, Text};
use futures::{StreamExt, TryStreamExt};
use lastdb_node::byte_cloud_restore::{RecoveryKeys, RestoreSession};
use lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient;
use std::collections::BTreeMap;

pub(super) fn run(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let phrase = super::read_recovery_phrase()?;
    super::block_on_cloud("V2 recovery", async {
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
            ByteBackupCommand::InspectDependencies(BackupDependenciesArgs {
                stream,
                generation,
                commit,
                commit_sha256,
                json,
                ..
            }) => {
                let scope = generation_scope(context.scope, generation);
                let stream = Stream {
                    scope,
                    stream_id: id(stream)?,
                };
                let selected = accepted(&client, stream, Some(id(commit)?)).await?;
                if selected.commit_sha256.as_str() != commit_sha256 {
                    return Err("selected dependency inspection hash differs".into());
                }
                let report = tokio::time::timeout(
                    std::time::Duration::from_secs(90),
                    inspect_dependencies(&client, selected),
                )
                .await
                .map_err(|_| "dependency inspection exceeded its time limit")??;
                print_backup(&report, json)
            }
            ByteBackupCommand::Discover(BackupListArgs {
                generation,
                cursor,
                json,
                ..
            }) => {
                let scope = generation_scope(context.scope, generation);
                discover(&client, &scope, cursor, json).await
            }
            ByteBackupCommand::Restore(args) => {
                restore(&client, &context.scope, &phrase, args).await
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
    scope: &CloudScope,
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
    print_backup(
        &serde_json::json!({"version":2,"scope":scope,"streams":entries,
        "next_cursor":directory.next_cursor}),
        json,
    )
}

async fn restore(
    client: &ByteBackupClient,
    scope: &CloudScope,
    phrase: &str,
    args: BackupRestoreArgs,
) -> Result<(), String> {
    let mut session = if let Some(path) = args.resume {
        if args.stream.is_some()
            || args.generation.is_some()
            || args.commit.is_some()
            || args.commit_sha256.is_some()
            || args.into.is_some()
        {
            return Err("resume cannot change the saved V2 selection".into());
        }
        RestoreSession::resume(&path).map_err(|e| e.to_string())?
    } else {
        let stream = Stream {
            scope: generation_scope(scope.clone(), args.generation),
            stream_id: id(args.stream.ok_or("V2 restore requires --stream")?)?,
        };
        let accepted = accepted(client, stream, args.commit.map(id).transpose()?).await?;
        if let Some(expected) = args.commit_sha256 {
            let expected = Digest::try_from(expected).map_err(|_| "invalid V2 commit SHA256")?;
            if accepted.commit_sha256 != expected {
                return Err("V2 accepted commit digest differs".into());
            }
        }
        RestoreSession::begin(&args.into.ok_or("V2 restore requires --into")?, accepted)
            .map_err(|e| e.to_string())?
    };
    if session.accepted().job().stream.scope.account_id != scope.account_id {
        return Err("V2 restore account differs".into());
    }
    let keys =
        RecoveryKeys::from_scope_phrase(phrase, &scope.account_id).map_err(|e| e.to_string())?;
    eprintln!("V2 restore session: {}", session.path().display());
    let report = session.restore(client, &keys).await.map_err(|e| {
        format!(
            "V2 restore retains session {}: {e}",
            session.path().display()
        )
    })?;
    print_backup(
        &serde_json::to_value(report).map_err(|_| "cannot encode V2 restore result")?,
        args.json,
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
        let CommitResponse::Accepted { backup } = client
            .commit_status(&request)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Err("selected V2 commit is not accepted".into());
        };
        return Ok(*backup);
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

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "V2 selection requires a canonical UUID".into())
}

async fn inspect_dependencies(
    client: &ByteBackupClient,
    selected: AcceptedBackup,
) -> Result<serde_json::Value, String> {
    selected.validate().map_err(|e| e.to_string())?;
    let stream = selected.job().stream.clone();
    let root = RecoveryPoint::of(&selected);
    let mut bytes = 0usize;
    let mut records = BTreeMap::new();
    let mut pending = vec![selected];
    while !pending.is_empty() {
        let mut wanted = BTreeMap::new();
        for backup in pending {
            backup.validate().map_err(|e| e.to_string())?;
            if backup.job().stream != stream {
                return Err("dependency stream differs".into());
            }
            if let Some(previous) = records.get(&backup.job().commit_id) {
                if previous != &backup {
                    return Err("accepted dependency identity conflicts".into());
                }
                continue;
            }
            bytes = bytes
                .checked_add(
                    cloud_backup_protocol::v2::hash::canonical_bytes(&backup)
                        .map_err(|e| e.to_string())?
                        .len(),
                )
                .filter(|count| *count <= MAX_DIRECTORY_BYTES)
                .ok_or("dependency metadata byte limit")?;
            if records.len() >= MAX_POINTS {
                return Err("dependency point limit".into());
            }
            for dependency in backup.begin.dependencies() {
                if dependency.ordinal >= backup.ordinal {
                    return Err("dependency ordinal did not decrease".into());
                }
                if let Some(prior) = wanted.insert(dependency.commit_id.clone(), dependency.clone())
                {
                    if prior != *dependency {
                        return Err("dependency head conflicts".into());
                    }
                }
            }
            records.insert(backup.job().commit_id.clone(), backup);
        }
        let mut fetch = Vec::new();
        for (id, expected) in wanted {
            if let Some(known) = records.get(&id) {
                if known.head() != expected {
                    return Err("accepted dependency hash differs".into());
                }
            } else {
                fetch.push(expected);
            }
        }
        if records.len().saturating_add(fetch.len()) > MAX_POINTS {
            return Err("dependency point limit".into());
        }
        pending = futures::stream::iter(fetch)
            .map(|expected: Head| {
                let stream = stream.clone();
                async move {
                    let backup = accepted(client, stream, Some(expected.commit_id.clone())).await?;
                    if backup.head() != expected {
                        return Err("accepted dependency selection differs".to_owned());
                    }
                    Ok(backup)
                }
            })
            .buffered(4)
            .try_collect()
            .await?;
    }
    let directory: Vec<_> = records.into_values().collect();
    dependency_report(&root, &directory, bytes)
}

fn dependency_report(
    root: &RecoveryPoint,
    directory: &[AcceptedBackup],
    bytes: usize,
) -> Result<serde_json::Value, String> {
    let closure = recovery_closure(
        &root.job.stream.scope,
        directory,
        std::slice::from_ref(root),
    )
    .map_err(|e| e.to_string())?;
    if !closure.candidates.is_empty() {
        return Err("dependency-only inspection produced retirement candidates".into());
    }
    Ok(serde_json::json!({
        "selected": root, "retained_points": closure.retained.len(),
        "dependency_edges": directory.iter().map(|b| b.begin.dependencies().len()).sum::<usize>(),
        "accepted_hashes_verified": true, "metadata_bytes": bytes,
        "directory_sha256": closure.directory_sha256,
        "closure_only": true, "account_directory_frozen": false,
        "provider_deletion_authorized": false, "provider_deletions": 0,
    }))
}

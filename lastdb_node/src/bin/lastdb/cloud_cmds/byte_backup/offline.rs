use super::*;
use cloud_backup_protocol::v2::{
    context::ContextReadRequest,
    discovery::StreamsListRequest,
    publication::{
        CommitResponse, CommitStatusRequest, HeadReadRequest, HeadReadResponse, HeadReadResult,
    },
    retention::{recovery_closure, RecoveryPoint, MAX_DIRECTORY_BYTES, MAX_POINTS},
    AcceptedBackup, Head, Job, Stream, Version,
};
use cloud_backup_protocol::{CloudScope, DecimalU64, Digest, Id, Text};
use futures::{StreamExt, TryStreamExt};
use lastdb_node::byte_cloud_backup::ByteBackupRequest;
use lastdb_node::byte_cloud_restore::{RecoveryKeys, RestoreSession};
use lastdb_node::cloud_backup_client::byte_backup::ByteBackupClient;
use std::collections::BTreeMap;

pub(super) fn run_byte_backup(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let (request, json) = match action {
        ByteBackupCommand::Start(BackupStartArgs { request_id, json }) => {
            let raw = request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let request_id =
                Id::try_from(raw).map_err(|_| "V2 request ID must be a canonical UUID")?;
            eprintln!("V2 backup request ID: {}", request_id.as_str());
            (ByteBackupRequest::Start { request_id }, json)
        }
        ByteBackupCommand::Resume(a) => (ByteBackupRequest::Resume, a.json),
        ByteBackupCommand::Status(a) => (ByteBackupRequest::Status, a.json),
        ByteBackupCommand::Close(a) => (ByteBackupRequest::Close, a.json),
        ByteBackupCommand::EnableAutomatic(a) => (
            ByteBackupRequest::EnableAutomatic {
                log_interval_seconds: a.log_interval_seconds,
                snapshot_interval_seconds: a.snapshot_interval_seconds,
            },
            a.json,
        ),
        ByteBackupCommand::DisableAutomatic(a) => (ByteBackupRequest::DisableAutomatic, a.json),
        action => return run_offline(home, action),
    };
    let body = serde_json::to_value(request).map_err(|_| "cannot encode V2 owner request")?;
    let sock = home.join("data/folddb.sock");
    let resp =
        super::super::socket_client::post_json_admin(&sock, "/api/sync/byte-backup-v2", &body)?;
    print_backup(&parse_json_response(&resp, "V2 backup")?, json)
}

pub(super) fn run_setup(source: Option<&Path>, action: &ByteBackupCommand) -> Result<(), String> {
    if let ByteBackupCommand::Connect(a) = action {
        if a.existing {
            if !a.api_key_stdin || a.dev || a.invite_code_stdin || a.restored {
                return Err(
                    "existing connection requires only an environment and API key stdin".into(),
                );
            }
            let home = source.ok_or("existing connection requires an explicit --data-dir")?;
            let env = a
                .env
                .as_deref()
                .ok_or("existing connection requires --env")?;
            return super::recovery_home::run_existing(
                home,
                env,
                a.expected_user_hash.as_deref(),
                a.json,
            );
        }
    }
    lastdb_node::byte_cloud_backup::require_enabled().map_err(|e| e.to_string())?;
    match action {
        ByteBackupCommand::Init(_) if source.is_some() => {
            Err("native backup init uses --into without --data-dir".into())
        }
        ByteBackupCommand::Init(a) => initialize(&a.into, &a.recovery_phrase_out, a.json),
        ByteBackupCommand::Connect(a) => {
            if !a.dev || a.env.is_some() || a.api_key_stdin || a.expected_user_hash.is_some() {
                return Err("native backup connect requires --dev".into());
            }
            if a.restored && a.invite_code_stdin {
                return Err("restored account reconnect does not accept a new invite".into());
            }
            super::connect(
                source.ok_or("native backup connect requires an explicit --data-dir")?,
                a.invite_code_stdin,
                a.restored,
                a.json,
            )
        }
        _ => Err("invalid native backup setup command".into()),
    }
}

fn initialize(destination: &Path, phrase_path: &Path, json: bool) -> Result<(), String> {
    if phrase_path != destination.join(PHRASE_FILE) {
        return Err("--recovery-phrase-out must name <into>/recovery-phrase.txt".into());
    }
    let target = FreshHome::preflight_empty(destination)?;
    let pair = Ed25519KeyPair::generate().map_err(|_| "cannot create fresh native identity")?;
    let seed = Zeroizing::new(pair.secret_key_bytes());
    let phrase = Zeroizing::new(
        bip39::Mnemonic::from_entropy(seed.as_ref())
            .map_err(|_| "cannot encode fresh native recovery phrase")?
            .to_string(),
    );
    let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    let mut target = target.create()?;
    target.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    target.write(PHRASE_FILE, phrase.as_bytes())?;
    target.commit()?;
    print_backup(
        &serde_json::json!({
            "schema": "lastdb.native-backup-init.v1", "ok": true, "environment": "dev",
            "account_id": account, "home": destination, "recovery_phrase_file": phrase_path,
            "cloud_sync_off": true, "local_store_opened": false, "network_request": false,
            "identity_seed_output": false, "recovery_phrase_output": false,
        }),
        json,
    )
}

fn run_offline(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let phrase = super::read_stdin_secret(4096, "recovery phrase")?;
    super::block_on_cloud("V2 recovery", async {
        let account_id = lastdb_node::backup_access::account_from_phrase(&phrase)?;
        let client = lastdb_node::byte_cloud_backup::client_from_home(home)?;
        let req = ContextReadRequest {
            version: Version::V2,
            account_id,
            required_destinations: Vec::new(),
        };
        let scope = client.context(&req).await.map_err(|e| e.to_string())?.scope;
        match action {
            ByteBackupCommand::InspectDependencies(a) => {
                let s = Stream {
                    scope: with_gen(&scope, a.generation),
                    stream_id: id(a.stream)?,
                };
                let selected = accepted(&client, s, Some(id(a.commit)?)).await?;
                if selected.commit_sha256.as_str() != a.commit_sha256 {
                    return Err("selected dependency inspection hash differs".into());
                }
                let fut = inspect_dependencies(&client, selected);
                let report = tokio::time::timeout(std::time::Duration::from_secs(90), fut)
                    .await
                    .map_err(|_| "dependency inspection exceeded its time limit")??;
                print_backup(&report, a.json)
            }
            ByteBackupCommand::Discover(a) => {
                discover(&client, &with_gen(&scope, a.generation), a.cursor, a.json).await
            }
            ByteBackupCommand::Restore(args) => restore(&client, &scope, &phrase, args).await,
            _ => Err("invalid offline V2 command".into()),
        }
    })
}

fn with_gen(scope: &CloudScope, generation: Option<u64>) -> CloudScope {
    let mut s = scope.clone();
    if let Some(g) = generation {
        s.account_generation = DecimalU64::new(g);
    }
    s
}

async fn head_ok(client: &ByteBackupClient, stream: Stream) -> Result<HeadReadResponse, String> {
    let req = HeadReadRequest {
        version: Version::V2,
        stream,
    };
    match client.head(&req).await.map_err(|e| e.to_string())? {
        HeadReadResult::Ok { response } => Ok(*response),
        HeadReadResult::Error { error } => Err(error.to_string()),
    }
}

async fn discover(
    client: &ByteBackupClient,
    scope: &CloudScope,
    cursor: Option<String>,
    json: bool,
) -> Result<(), String> {
    let cursor = cursor
        .map(Text::try_from)
        .transpose()
        .map_err(|_| "invalid V2 discovery cursor")?;
    let req = StreamsListRequest {
        version: Version::V2,
        scope: scope.clone(),
        limit: 100,
        cursor,
    };
    let dir = client.list(&req).await.map_err(|e| e.to_string())?;
    let entries: Vec<_> = futures::stream::iter(dir.streams)
        .map(|s| async move {
            let r = head_ok(client, s).await?;
            Ok::<_, String>(serde_json::json!({
                "stream": r.stream, "accepted": r.accepted, "active_commit_id": r.active_commit_id,
            }))
        })
        .buffered(4)
        .try_collect()
        .await?;
    print_backup(
        &serde_json::json!({"version":2,"scope":scope,"streams":entries,"next_cursor":dir.next_cursor}),
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
        let raw_s = args.stream.ok_or("V2 restore requires --stream")?;
        let stream = Stream {
            scope: with_gen(scope, args.generation),
            stream_id: id(raw_s)?,
        };
        let acc = accepted(client, stream, args.commit.map(id).transpose()?).await?;
        if let Some(exp) = args.commit_sha256 {
            let exp = Digest::try_from(exp).map_err(|_| "invalid V2 commit SHA256")?;
            (acc.commit_sha256 == exp)
                .then_some(())
                .ok_or("V2 accepted commit digest differs")?;
        }
        RestoreSession::begin(&args.into.ok_or("V2 restore requires --into")?, acc)
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
        let req = CommitStatusRequest {
            version: Version::V2,
            job: Job { stream, commit_id },
        };
        return match client
            .commit_status(&req)
            .await
            .map_err(|e| e.to_string())?
        {
            CommitResponse::Accepted { backup } => Ok(*backup),
            _ => Err("selected V2 commit is not accepted".into()),
        };
    }
    head_ok(client, stream)
        .await?
        .accepted
        .ok_or_else(|| "V2 stream has no accepted backup".into())
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "V2 selection requires a canonical UUID".into())
}

async fn inspect_dependencies(
    client: &ByteBackupClient,
    selected: AcceptedBackup,
) -> Result<serde_json::Value, String> {
    selected.validate().map_err(|e| e.to_string())?;
    let (stream, root) = (selected.job().stream.clone(), RecoveryPoint::of(&selected));
    let (mut bytes, mut records, mut pending) = (0usize, BTreeMap::new(), vec![selected]);
    while !pending.is_empty() {
        let mut wanted = BTreeMap::new();
        for backup in pending {
            backup.validate().map_err(|e| e.to_string())?;
            if backup.job().stream != stream {
                return Err("dependency stream differs".into());
            }
            if let Some(prev) = records.get(&backup.job().commit_id) {
                (prev == &backup)
                    .then_some(())
                    .ok_or("accepted dependency identity conflicts")?;
                continue;
            }
            let len = cloud_backup_protocol::v2::hash::canonical_bytes(&backup)
                .map_err(|e| e.to_string())?
                .len();
            bytes = bytes
                .checked_add(len)
                .filter(|&n| n <= MAX_DIRECTORY_BYTES)
                .ok_or("dependency metadata byte limit")?;
            (records.len() < MAX_POINTS)
                .then_some(())
                .ok_or("dependency point limit")?;
            for dep in backup.begin.dependencies() {
                if dep.ordinal >= backup.ordinal {
                    return Err("dependency ordinal did not decrease".into());
                }
                if wanted
                    .insert(dep.commit_id.clone(), dep.clone())
                    .is_some_and(|p| p != *dep)
                {
                    return Err("dependency head conflicts".into());
                }
            }
            records.insert(backup.job().commit_id.clone(), backup);
        }
        let mut fetch = Vec::new();
        for (id, exp) in wanted {
            match records.get(&id) {
                Some(k) if k.head() != exp => return Err("accepted dependency hash differs".into()),
                Some(_) => {}
                None => fetch.push(exp),
            }
        }
        if records.len().saturating_add(fetch.len()) > MAX_POINTS {
            return Err("dependency point limit".into());
        }
        pending = futures::stream::iter(fetch)
            .map(|exp: Head| {
                let s = stream.clone();
                async move {
                    let b = accepted(client, s, Some(exp.commit_id.clone())).await?;
                    (b.head() == exp)
                        .then_some(b)
                        .ok_or_else(|| "accepted dependency selection differs".to_owned())
                }
            })
            .buffered(4)
            .try_collect()
            .await?;
    }
    let dir: Vec<_> = records.into_values().collect();
    let closure = recovery_closure(&root.job.stream.scope, &dir, std::slice::from_ref(&root))
        .map_err(|e| e.to_string())?;
    if !closure.candidates.is_empty() {
        return Err("dependency-only inspection produced retirement candidates".into());
    }
    Ok(serde_json::json!({
        "selected": root, "retained_points": closure.retained.len(),
        "dependency_edges": dir.iter().map(|b| b.begin.dependencies().len()).sum::<usize>(),
        "accepted_hashes_verified": true, "metadata_bytes": bytes,
        "directory_sha256": closure.directory_sha256,
        "closure_only": true, "account_directory_frozen": false,
        "provider_deletion_authorized": false, "provider_deletions": 0,
    }))
}

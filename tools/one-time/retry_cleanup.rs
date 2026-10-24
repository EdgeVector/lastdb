//! Temporary, owner-authorized CI operation on an already generated native home.
#![recursion_limit = "256"]

use lastdb_node::Host;
use sha2::{Digest, Sha256};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

type Failure = Box<dyn std::error::Error>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn home() -> Result<PathBuf, Failure> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        || std::env::var("RUNNER_ENVIRONMENT").as_deref() != Ok("github-hosted")
        || std::env::var("LASTDB_ISOLATED_COPY").as_deref() != Ok("1")
    {
        return Err("this one-time operation requires an isolated hosted CI home".into());
    }
    let raw = PathBuf::from(std::env::args().nth(2).ok_or("missing generated home")?);
    let home = raw.canonicalize()?;
    let root = home.parent().ok_or("missing owner root")?;
    let metadata = root.symlink_metadata()?;
    let owner: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("owner.json"))?)?;
    let parent = std::fs::read_to_string("/proc/self/status")?
        .lines()
        .find_map(|line| line.strip_prefix("PPid:"))
        .ok_or("missing parent process")?
        .trim()
        .parse::<u64>()?;
    if raw != home
        || root.parent() != Some(std::path::Path::new("/tmp"))
        || !root
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("ldb-bench-"))
        || metadata.mode() & 0o777 != 0o700
        || metadata.uid() != std::fs::metadata("/proc/self")?.uid()
        || owner["root"].as_str() != root.to_str()
        || owner["uid"].as_u64() != Some(u64::from(metadata.uid()))
        || owner["inode"].as_u64() != Some(metadata.ino())
        || owner["pid"].as_u64() != Some(parent)
        || owner["generated_data_only"] != true
        || !root.join(".retry-cleanup-ci").is_file()
        || lastdb_node::cloud::cloud_sync_file_state(&home) != "unset"
    {
        return Err("the exact generated home ownership differs".into());
    }
    for socket in ["q.sock", "f.sock"] {
        if std::os::unix::net::UnixStream::connect(root.join(socket)).is_ok() {
            return Err("the generated daemon still runs".into());
        }
    }
    Ok(home)
}

fn generated() -> (Rows, Rows) {
    let mut old = Vec::new();
    let mut unknown = Vec::new();
    for number in 0..2048 {
        let scope = match number / 512 {
            0 | 1 => "",
            2 => "aaa:",
            _ => "zzz:",
        };
        let hash = Sha256::digest(format!("one-time-old-{number}").as_bytes());
        old.push((
            format!("{scope}idem:{hash:x}").into_bytes(),
            format!("\"generated-old-receipt-{number}\"").into_bytes(),
        ));
    }
    for number in 0..1536 {
        let key = match number {
            0..768 => format!("000:opaque:{number:04}"),
            768..1024 => format!("hhh:idem:not-a-hash:{number:04}"),
            1024..1280 => format!("qqq:idem:{number:063x}"),
            _ => format!("zzzz:idem:{number:064x}:opaque"),
        };
        unknown.push((
            key.into_bytes(),
            format!("\"preserve-{number}\"").into_bytes(),
        ));
    }
    (old, unknown)
}

async fn operation(host: &Host, seed: bool) -> Result<serde_json::Value, Failure> {
    let store = host.db.db_ops().open_namespace("idempotency").await?;
    let (old, unknown) = generated();
    let controls = vec![
        b"retry-cache:v1:legacy-drained".to_vec(),
        b"retry-cache:v1:legacy-cursor".to_vec(),
    ];
    let state = store.get_many(controls).await?;
    if state.len() != 2
        || (seed && state.iter().any(Option::is_some))
        || (!seed && (state[0].is_none() || state[1].is_some()))
    {
        return Err("the cleanup marker or cursor differs from the required phase".into());
    }
    if seed {
        let rows: Vec<_> = old.iter().chain(&unknown).cloned().collect();
        for batch in rows.chunks(128) {
            store.batch_put(batch.to_vec()).await?;
        }
    }
    for (rows, absent) in [(&old, !seed), (&unknown, false)] {
        for batch in rows.chunks(128) {
            let values = store
                .get_many(batch.iter().map(|(key, _)| key.clone()).collect())
                .await?;
            if values.len() != batch.len()
                || values.iter().zip(batch).any(|(value, (_, expected))| {
                    if absent {
                        value.is_some()
                    } else {
                        value.as_ref() != Some(expected)
                    }
                })
            {
                return Err("an old receipt or preserved unknown record differs".into());
            }
        }
    }
    Ok(
        serde_json::json!({"old_receipts":old.len(), "unscoped":1024, "scoped":1024,
        "unknown_records":unknown.len(), "unknown_before_first_legacy":768,
        "old_receipts_absent":!seed, "unknown_values_unchanged":true,
        "cursor_removed":!seed, "completion_marker_present":!seed}),
    )
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let mode = std::env::args()
        .nth(1)
        .ok_or("missing seed or verify phase")?;
    if mode != "seed" && mode != "verify" {
        return Err("unknown operation".into());
    }
    let host = Host::boot(&home()?).await.map_err(std::io::Error::other)?;
    if host.db.sync_engine().is_some() {
        return Err("the generated home has a sync engine".into());
    }
    let result = operation(&host, mode == "seed").await;
    host.db.shutdown().await?;
    println!("{}", result?);
    Ok(())
}

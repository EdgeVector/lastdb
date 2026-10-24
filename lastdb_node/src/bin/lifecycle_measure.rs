//! GitHub-only lifecycle measurement against an exact owned generated home.
#![recursion_limit = "256"]

use lastdb_node::Host;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

fn checked_home() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        || std::env::var("LASTDB_ISOLATED_COPY").as_deref() != Ok("1")
        || std::env::var("LASTDB_SUPPRESS_RECOVERY_PHRASE").as_deref() != Ok("1")
    {
        return Err("lifecycle operations run only in isolated GitHub Actions".into());
    }
    let raw = PathBuf::from(std::env::args().nth(1).ok_or("missing generated home")?);
    let home = raw.canonicalize()?;
    let temporary = PathBuf::from(std::env::var("RUNNER_TEMP")?).canonicalize()?;
    let workspace = PathBuf::from(std::env::var("GITHUB_WORKSPACE")?).canonicalize()?;
    let root = home.parent().ok_or("missing owner root")?;
    let owner: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("owner.json"))?)?;
    let metadata = root.symlink_metadata()?;
    if raw != home
        || root.parent() != Some(temporary.as_path())
        || home.starts_with(workspace)
        || metadata.mode() & 0o777 != 0o700
        || owner["root"].as_str() != root.to_str()
        || owner["uid"].as_u64() != Some(u64::from(metadata.uid()))
        || owner["device"].as_u64() != Some(metadata.dev())
        || owner["inode"].as_u64() != Some(metadata.ino())
        || owner["generated_data_only"] != true
        || !home.join(".lifecycle-ci").is_file()
    {
        return Err("the exact generated lifecycle home ownership differs".into());
    }
    if lastdb_node::cloud::cloud_sync_file_state(&home) != "unset"
        || lastdb_node::cloud::cloud_resume_required_path(&home).exists()
    {
        return Err("the generated lifecycle home has cloud configuration".into());
    }
    for socket in ["q.sock", "f.sock"] {
        if std::os::unix::net::UnixStream::connect(root.join(socket)).is_ok() {
            return Err("the generated home still has an active daemon".into());
        }
    }
    Ok(home)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = checked_home()?;
    let phase = std::env::args().nth(2).ok_or("missing lifecycle phase")?;
    let host = Host::boot(&home).await.map_err(std::io::Error::other)?;
    let outcome = if phase == "ttl" {
        let started = std::time::Instant::now();
        lastdb_node::ttl_sweep::sweep_ttl(&host, &AtomicBool::new(false)).await;
        Ok(serde_json::json!({"phase":"ttl","elapsed_ms":started.elapsed().as_secs_f64()*1000.0}))
    } else {
        fold_db::benchmark_support::lifecycle::measure(&host.db, &phase).await
    };
    let shutdown = host.db.shutdown().await;
    let value = outcome?;
    shutdown?;
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

//! Offline identity preparation leaves the receiver intact and the target inactive.

use clap::Args;
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::logical_capture::SnapshotAdoptionCandidate;
use fold_db::storage::disk_pack::run_restore;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

#[derive(Args, Debug)]
pub(crate) struct SnapshotIdentityArgs {
    /// Existing inactive destination home. No active-home replacement occurs.
    #[arg(long)]
    into: PathBuf,
    /// Private JSON file with the donor snapshot root, profile and signed map.
    #[arg(long)]
    candidate_file: PathBuf,
    /// Existing private scratch directory for native file validation.
    #[arg(long)]
    scratch: PathBuf,
    /// Emit a report without private key material.
    #[arg(long)]
    json: bool,
}

pub(crate) fn run(home: &Path, args: &SnapshotIdentityArgs, prepare: bool) -> Result<(), String> {
    let home = protected_path(home)?;
    let into = protected_path(&args.into)?;
    let scratch = protected_path(&args.scratch)?;
    if home.starts_with(&into) || into.starts_with(&home) {
        return Err("source and destination homes must be separate".into());
    }
    if [&home, &into]
        .iter()
        .any(|root| scratch.starts_with(root) || root.starts_with(&scratch))
    {
        return Err("scratch must be outside both homes".into());
    }
    lastdb_node::offline_home::preflight_read_only_home(&home, &home.join("data"))?;
    refuse_daemon(&into)?;
    let (identity_file, seed, identity_meta) = private_bytes(&home.join("identity.key"), 32)?;
    identity_file
        .try_lock()
        .map_err(|_| "source account identity is already owned")?;
    if seed.len() != 32 {
        return Err("source identity must contain exactly 32 bytes".into());
    }
    let seed: &[u8; 32] = seed.as_slice().try_into().map_err(|_| "invalid identity")?;
    let keys = E2eKeys::from_ed25519_seed(seed).map_err(|_| "invalid source account identity")?;
    let content_key = Zeroizing::new(keys.encryption_key());
    let candidate_path = protected_path(&args.candidate_file)?;
    let (_candidate_file, bytes, _) = private_bytes(&candidate_path, 40 * 1024)?;
    let candidate: SnapshotAdoptionCandidate =
        serde_json::from_slice(&bytes).map_err(|_| "invalid snapshot candidate metadata")?;
    let report = if prepare {
        run_restore::prepare_snapshot_identity(
            &home.join("data"),
            &into.join("data"),
            &content_key,
            &candidate,
            &scratch,
        )
    } else {
        run_restore::inspect_snapshot_identity(
            &home.join("data"),
            &into.join("data"),
            &content_key,
            &candidate,
            &scratch,
        )
    }
    .map_err(|error| error.to_string())?;
    check_entry(&identity_file, &home.join("identity.key"), &identity_meta)?;
    let text =
        serde_json::to_string_pretty(&report).map_err(|_| "cannot encode identity report")?;
    if args.json {
        println!("{text}");
    } else {
        println!("Snapshot identity prepared. The destination stays inactive.\n{text}");
    }
    Ok(())
}

fn protected_path(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("snapshot identity operations require absolute paths".into());
    }
    let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
    let archive = PathBuf::from(user).join(".primary-lastdb-archive-20261014");
    // Refuse the literal archive before any operation resolves that path.
    if path.starts_with(&archive) {
        return Err("snapshot identity operations refuse the primary archive".into());
    }
    lastdb_node::offline_home::refuse_primary(path)?;
    let resolved = path
        .canonicalize()
        .map_err(|_| "snapshot identity path is absent")?;
    if resolved.starts_with(archive) {
        return Err("snapshot identity operations refuse the primary archive".into());
    }
    if resolved != path {
        return Err("snapshot identity operations refuse path aliases".into());
    }
    Ok(resolved)
}

fn refuse_daemon(home: &Path) -> Result<(), String> {
    if lastdb_node::crash_attribution::live_daemon_pid_alive(home)
        || ["folddb.sock", "folddb-full.sock"].iter().any(|name| {
            std::os::unix::net::UnixStream::connect(home.join("data").join(name)).is_ok()
        })
    {
        return Err("stop the destination daemon before snapshot identity preparation".into());
    }
    Ok(())
}

fn private_bytes(
    path: &Path,
    maximum: u64,
) -> Result<(File, Zeroizing<Vec<u8>>, std::fs::Metadata), String> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open private snapshot identity input")?;
    let meta = file
        .metadata()
        .map_err(|_| "cannot inspect private input")?;
    if !meta.is_file()
        || meta.mode() & 0o777 != 0o600
        || meta.nlink() != 1
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.len() > maximum
    {
        return Err("snapshot identity input must be a bounded private owned file".into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    Read::by_ref(&mut file)
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read private input")?;
    if bytes.len() as u64 != meta.len() {
        return Err("snapshot identity input changed size".into());
    }
    check_entry(&file, path, &meta)?;
    Ok((file, bytes, meta))
}

fn check_entry(file: &File, path: &Path, before: &std::fs::Metadata) -> Result<(), String> {
    let held = file.metadata().map_err(|_| "cannot inspect held input")?;
    let current = path
        .symlink_metadata()
        .map_err(|_| "private input disappeared")?;
    for meta in [&held, &current] {
        if (
            before.dev(),
            before.ino(),
            before.uid(),
            before.mode(),
            before.len(),
            before.mtime(),
            before.mtime_nsec(),
            before.ctime(),
            before.ctime_nsec(),
        ) != (
            meta.dev(),
            meta.ino(),
            meta.uid(),
            meta.mode(),
            meta.len(),
            meta.mtime(),
            meta.mtime_nsec(),
            meta.ctime(),
            meta.ctime_nsec(),
        ) {
            return Err("private snapshot identity input changed".into());
        }
    }
    Ok(())
}

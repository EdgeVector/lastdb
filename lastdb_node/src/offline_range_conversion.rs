//! Explicit separate-target range conversion. No daemon or cloud client starts.

use crate::offline_home::{preflight_read_only_home, refuse_primary_or_archive};
use fold_db::atom::{HashKeyEncoding, RangeKeyEncoding};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::range_inventory::{
    InventoryLimits, PreparedRangeConversion, RangeConversionOptions,
};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub struct RangeConversionArgs<'a> {
    pub source_home: &'a Path,
    pub target_home: &'a Path,
    pub source_hash_encoding: &'a str,
    pub source_range_encoding: &'a str,
    pub resume: bool,
    pub max_records: usize,
    pub max_metadata_mib: usize,
    pub json: bool,
}

pub fn convert(args: &RangeConversionArgs<'_>) -> Result<(), String> {
    for home in [args.source_home, args.target_home] {
        refuse_primary_or_archive(home)?;
        refuse_primary_or_archive(&home.join("data"))?;
    }
    let source_home = args
        .source_home
        .canonicalize()
        .map_err(|error| format!("source home: {error}"))?;
    let target_home = target_path(args.target_home)?;
    if source_home.starts_with(&target_home) || target_home.starts_with(&source_home) {
        return Err("source and target must be separate homes".into());
    }
    if args.source_hash_encoding != "blind_v1" {
        return Err("source hash encoding must be blind_v1".into());
    }
    let range = match args.source_range_encoding {
        "plain" => RangeKeyEncoding::Plain,
        "ope_v1" => RangeKeyEncoding::OpeV1,
        _ => return Err("source range encoding must be plain or ope_v1".into()),
    };
    let preflight = preflight_read_only_home(&source_home, &source_home.join("data"))?;
    let seed = zeroize::Zeroizing::new(
        lastdb_identity::load_seed(&source_home)?.ok_or("source account identity is absent")?,
    );
    let keys =
        E2eKeys::from_ed25519_seed(&seed).map_err(|_| "source identity cannot derive its keys")?;
    let mut prepared = PreparedRangeConversion::prepare(
        &source_home.join("data"),
        &target_home.join("data"),
        &keys,
        RangeConversionOptions {
            source_hash_encoding: HashKeyEncoding::BlindV1,
            source_range_encoding: range,
            resume: args.resume,
            limits: InventoryLimits {
                max_records: args.max_records,
                max_metadata_bytes: args
                    .max_metadata_mib
                    .checked_mul(1024 * 1024)
                    .ok_or("metadata allowance overflows")?,
                ..InventoryLimits::default()
            },
        },
    )
    .map_err(|error| error.to_string())?;
    prepared
        .require_capacity()
        .map_err(|error| error.to_string())?;
    prepare_target(&target_home, &seed, args.resume, &mut prepared)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .map_err(|_| "cannot create the offline conversion runtime")?;
    let report = runtime
        .block_on(prepared.execute())
        .map_err(|error| error.to_string())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&serde_json::json!({ "ok": true, "mode": "convert-typed-ranges", "preflight": preflight, "conversion": report })).map_err(|_| "cannot encode conversion result")?);
    } else {
        println!(
            "Typed range conversion complete: {} main records, {} file records.",
            report.records[0], report.records[1]
        );
        println!(
            "Target: {}. The source stays unchanged.",
            target_home.display()
        );
    }
    Ok(())
}

pub(crate) fn target_path(path: &Path) -> Result<PathBuf, String> {
    if path.try_exists().map_err(|error| error.to_string())? {
        return path.canonicalize().map_err(|error| error.to_string());
    }
    let parent = path
        .parent()
        .ok_or("target home has no parent")?
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let name = path.file_name().ok_or("target home has no name")?;
    Ok(parent.join(name))
}

fn prepare_target(
    home: &Path,
    seed: &[u8; 32],
    resume: bool,
    prepared: &mut PreparedRangeConversion,
) -> Result<(), String> {
    if std::fs::symlink_metadata(home).is_ok() {
        if !resume {
            return Err("a new target home must be absent".into());
        }
        if !std::fs::symlink_metadata(home)
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err("target home must be a real directory".into());
        }
        if crate::crash_attribution::live_daemon_pid_alive(home) {
            return Err("target has a live daemon".into());
        }
        let existing = zeroize::Zeroizing::new(
            lastdb_identity::load_seed(home)?.ok_or("target identity is absent")?,
        );
        if *existing != *seed {
            return Err("target identity differs from its source".into());
        }
        return Ok(());
    }
    let parent = home.parent().ok_or("target home has no parent")?;
    let name = home
        .file_name()
        .ok_or("target home has no name")?
        .to_string_lossy();
    let staging = parent.join(format!(".{name}.typed-range-init"));
    match std::fs::symlink_metadata(&staging) {
        Ok(metadata) if metadata.is_dir() && resume => {}
        Ok(_) => {
            return Err(
                "target initialization exists; use explicit resume with the same job".into(),
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&staging)
            .map_err(|error| error.to_string())?,
        Err(error) => return Err(error.to_string()),
    }
    match std::fs::symlink_metadata(staging.join("data")) {
        Ok(metadata) if metadata.is_dir() => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::DirBuilder::new()
            .mode(0o700)
            .create(staging.join("data"))
            .map_err(|error| error.to_string())?,
        _ => return Err("staged target data must be a real directory".into()),
    }
    prepared
        .write_staged_fence(&staging.join("data"))
        .map_err(|error| error.to_string())?;
    resume_target(&staging, seed)?;
    std::fs::File::open(&staging)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    if std::fs::symlink_metadata(home).is_ok() {
        return Err("target appeared during initialization; no files were replaced".into());
    }
    std::fs::rename(&staging, home).map_err(|error| error.to_string())?;
    std::fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn resume_target(home: &Path, seed: &[u8; 32]) -> Result<(), String> {
    let owner = std::fs::File::open(home).map_err(|error| error.to_string())?;
    owner
        .try_lock()
        .map_err(|_| "another converter owns the target identity")?;
    let path = lastdb_identity::seed_path(home);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() && metadata.len() <= 32 => {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)
                .map_err(|_| "cannot open the target identity")?;
            let mut existing = zeroize::Zeroizing::new(Vec::new());
            (&mut file)
                .take(33)
                .read_to_end(&mut existing)
                .map_err(|_| "cannot read the target identity")?;
            if !seed.starts_with(&existing) {
                return Err("target identity differs from its source".into());
            }
            if existing.len() != 32 {
                require_initial_home(home)?;
                file.rewind()
                    .map_err(|_| "cannot finish the target identity")?;
                file.write_all(seed)
                    .and_then(|()| file.sync_all())
                    .map_err(|_| "cannot finish the target identity")?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            require_initial_home(home)?;
            lastdb_identity::write_seed(home, seed)?;
        }
        _ => return Err("target identity is invalid".into()),
    }
    match std::fs::symlink_metadata(home.join("data")) {
        Ok(metadata) if metadata.is_dir() => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            require_initial_home(home)?;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(home.join("data"))
                .map_err(|error| error.to_string())?;
        }
        _ => return Err("target data root is not a real directory".into()),
    }
    owner.sync_all().map_err(|error| error.to_string())?;
    Ok(())
}

fn require_initial_home(home: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(home).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let kind = entry.file_type().map_err(|error| error.to_string())?;
        let name = entry.file_name();
        if name == lastdb_identity::IDENTITY_KEY_FILE && kind.is_file() {
            continue;
        }
        if name == "data" && kind.is_dir() && initial_data_entries(&entry.path())? {
            continue;
        }
        return Err("an incomplete identity cannot resume beside existing data".into());
    }
    Ok(())
}

fn initial_data_entries(root: &Path) -> Result<bool, String> {
    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_file()
            || !["lastdb-range-conversion-v1", "laststore-pack-node-v1"]
                .iter()
                .any(|name| entry.file_name() == *name)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

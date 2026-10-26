//! Explicit separate-target range conversion. No daemon or cloud client starts.

use crate::offline_home::{preflight_read_only_home, refuse_primary_or_archive};
use fold_db::atom::{HashKeyEncoding, RangeKeyEncoding};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::range_inventory::{
    convert_typed_ranges, InventoryLimits, RangeConversionOptions,
};
use std::os::unix::fs::DirBuilderExt;
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
    prepare_target(&target_home, &seed, args.resume)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .map_err(|_| "cannot create the offline conversion runtime")?;
    let report = runtime
        .block_on(convert_typed_ranges(
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
        ))
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

fn target_path(path: &Path) -> Result<PathBuf, String> {
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

fn prepare_target(home: &Path, seed: &[u8; 32], resume: bool) -> Result<(), String> {
    if resume {
        if crate::crash_attribution::live_daemon_pid_alive(home) {
            return Err("target has a live daemon".into());
        }
        let existing = zeroize::Zeroizing::new(
            lastdb_identity::load_seed(home)?.ok_or("target identity is absent")?,
        );
        if *existing != *seed {
            return Err("target identity differs from its source".into());
        }
        if !std::fs::symlink_metadata(home.join("data"))
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err("target data root is not a real directory".into());
        }
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(home)
        .map_err(|error| format!("a new target home must be absent: {error}"))?;
    lastdb_identity::write_seed(home, seed)?;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(home.join("data"))
        .map_err(|error| error.to_string())?;
    std::fs::File::open(home)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

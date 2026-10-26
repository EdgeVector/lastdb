//! The stopped-development-home range-conversion inventory. No node is started.

use crate::offline_home::{preflight_read_only_home, refuse_primary_or_archive};
use fold_db::atom::RangeKeyEncoding;
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::range_inventory::{inspect_range_conversion, InventoryLimits};
use std::path::Path;

pub struct RangeInventoryArgs<'a> {
    pub home: &'a Path,
    pub source_range_encoding: &'a str,
    pub max_records: usize,
    pub max_metadata_mib: usize,
    pub json: bool,
}

pub fn inspect(args: &RangeInventoryArgs<'_>) -> Result<(), String> {
    // Guard before any identity, session, socket, or native file access.
    refuse_primary_or_archive(args.home)?;
    let root = args.home.join("data");
    refuse_primary_or_archive(&root)?;
    let preflight = preflight_read_only_home(args.home, &root)?;
    let seed = zeroize::Zeroizing::new(
        lastdb_identity::load_seed(args.home)?.ok_or("source account identity is absent")?,
    );
    let keys = E2eKeys::from_ed25519_seed(&seed)
        .map_err(|_| "source account identity cannot derive its keys")?;
    let encoding = match args.source_range_encoding {
        "plain" => RangeKeyEncoding::Plain,
        "ope_v1" => RangeKeyEncoding::OpeV1,
        _ => return Err("source range encoding must be plain or ope_v1".into()),
    };
    let limits = InventoryLimits {
        max_records: args.max_records,
        max_metadata_bytes: args
            .max_metadata_mib
            .checked_mul(1024 * 1024)
            .ok_or("metadata limit overflows")?,
        ..InventoryLimits::default()
    };
    let report = inspect_range_conversion(&root, &keys, encoding, limits)
        .map_err(|error| error.to_string())?;
    if args.json {
        let output = serde_json::json!({"ok": true, "mode": "inspect-range-conversion", "preflight": preflight, "inventory": report});
        println!(
            "{}",
            serde_json::to_string_pretty(&output).map_err(|_| "cannot encode inventory")?
        );
    } else {
        println!(
            "Range conversion inventory: {} schemas, {} molecules, {} metadata records",
            report.schemas,
            report.molecules.len(),
            report.metadata_records
        );
        println!(
            "Source format: {}; metadata bytes: {}",
            report.source_format, report.metadata_bytes
        );
        for molecule in &report.molecules {
            let profile = molecule
                .proposed_profile
                .map_or_else(|| "unresolved".into(), |profile| format!("{profile:?}"));
            println!(
                "{}: {profile}; {} distinct keys; {} current tips; {} barriers; {} blockers",
                molecule.molecule,
                molecule.distinct_storage_keys,
                molecule.logical_tips,
                molecule.delete_barriers,
                molecule.blockers.len()
            );
            for blocker in &molecule.blockers {
                println!("  {blocker}");
            }
        }
        println!("The source stays unchanged. Database conversion and activation remain pending.");
    }
    Ok(())
}

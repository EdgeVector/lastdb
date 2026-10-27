//! Target-aware eligibility stays a read-only report, never permission to publish.

use super::RangeInventoryArgs;
use crate::offline_home::refuse_primary_or_archive;
use fold_db::atom::{HashKeyEncoding, RangeKeyEncoding};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::range_inventory::{
    ConversionCapacity, InventoryLimits, PreparedRangeConversion, RangeConversionInventory,
    RangeConversionOptions,
};
use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(Serialize)]
pub(super) struct Readiness {
    target_home: PathBuf,
    source_hash_encoding: &'static str,
    source_hash_encoding_is_assertion: bool,
    copy_scope: &'static str,
    ready_to_convert: bool,
    blockers: Vec<String>,
    capacity: Option<ConversionCapacity>,
}

pub(super) fn target(args: &RangeInventoryArgs<'_>) -> Result<Option<PathBuf>, String> {
    let Some(target) = args.target_home else {
        if args.source_hash_encoding.is_some() {
            return Err("source hash assertion requires a target home".into());
        }
        return Ok(None);
    };
    // All primary guards run before any source identity or native-file read.
    refuse_primary_or_archive(target)?;
    refuse_primary_or_archive(&target.join("data"))?;
    if args.source_hash_encoding != Some("blind_v1") || args.target_range_encoding != "plain" {
        return Err(
            "target readiness requires explicit blind_v1 source hashes and plain target ranges"
                .into(),
        );
    }
    let source = args
        .home
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let target = crate::offline_range_conversion::target_path(target)?;
    refuse_primary_or_archive(&target)?;
    if source.starts_with(&target) || target.starts_with(&source) {
        return Err("source and target must be separate homes".into());
    }
    Ok(Some(target))
}

pub(super) fn inspect(
    args: &RangeInventoryArgs<'_>,
    root: &Path,
    keys: &E2eKeys,
    inventory: &RangeConversionInventory,
    target: Option<PathBuf>,
    limits: InventoryLimits,
) -> Result<Option<Readiness>, String> {
    let Some(target) = target else {
        return Ok(None);
    };
    let mut report = Readiness {
        target_home: target.clone(),
        source_hash_encoding: "blind_v1",
        source_hash_encoding_is_assertion: true,
        copy_scope: "account_identity_and_native_data_only",
        ready_to_convert: false,
        blockers: Vec::new(),
        capacity: None,
    };
    match std::fs::symlink_metadata(&target) {
        Ok(_) => report.blockers.push(
            "new target must be absent; resume an existing job only through the converter".into(),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    if inventory.molecules.iter().any(|m| !m.blockers.is_empty()) {
        report
            .blockers
            .push("resolve the molecule blockers in the inventory before conversion".into());
    }
    if report.blockers.is_empty() {
        let source_range_encoding = match args.source_range_encoding {
            "plain" => RangeKeyEncoding::Plain,
            "ope_v1" => RangeKeyEncoding::OpeV1,
            _ => return Err("unsupported source range encoding".into()),
        };
        let prepared = PreparedRangeConversion::prepare(
            root,
            &target.join("data"),
            keys,
            RangeConversionOptions {
                source_hash_encoding: HashKeyEncoding::BlindV1,
                source_range_encoding,
                limits,
                resume: false,
            },
        )
        .map_err(|error| error.to_string())?;
        if prepared.source_publications_sha256() != inventory.source_publications_sha256 {
            return Err(
                "source publications changed between inventory and readiness; inspect again".into(),
            );
        }
        let capacity = prepared.capacity().map_err(|error| error.to_string())?;
        if !capacity.sufficient_free_space {
            report
                .blockers
                .push("target filesystem lacks the estimated temporary conversion space".into());
        }
        if !capacity.within_disk_cap {
            report.blockers.push(
                "the configured native disk cap cannot fit the estimated target and merge space"
                    .into(),
            );
        }
        report.capacity = Some(capacity);
    }
    report.ready_to_convert = report.blockers.is_empty();
    Ok(Some(report))
}

impl Readiness {
    pub(super) fn print(&self) {
        println!(
            "Ready to convert: {}. Source hash encoding: blind_v1 (caller assertion).",
            self.ready_to_convert
        );
        if let Some(capacity) = &self.capacity {
            println!(
                "Target free bytes: {}; estimated additional bytes: {} (target, merges, reserve).",
                capacity.available_bytes, capacity.estimate.additional_required_bytes
            );
        }
        for blocker in &self.blockers {
            println!("  {blocker}");
        }
        println!("This report creates no target and does not switch the active home.");
    }
}

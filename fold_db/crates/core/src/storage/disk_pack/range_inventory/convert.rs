//! Explicit V2-to-V2 typed conversion. The source never receives a writer.

use super::{
    catalog::Catalog,
    conversion_copy::{self, Phase},
    conversion_plan::ConversionPlan,
    conversion_target::Target,
    invalid,
    source::{FrozenSource, Source},
    InventoryLimits, StorageResult,
};
use crate::atom::{HashKeyEncoding, MoleculeKeyCodec, RangeKeyEncoding};
use crate::crypto::E2eKeys;
use laststore::disk_run::home::HomeMutation;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RangeConversionOptions {
    /// An assertion about the stopped source; the bytes do not declare it.
    pub source_hash_encoding: HashKeyEncoding,
    pub source_range_encoding: RangeKeyEncoding,
    pub limits: InventoryLimits,
    pub resume: bool,
}

#[derive(Debug, Serialize)]
pub struct RangeConversionReport {
    pub version: u8,
    pub job: String,
    pub records: [u64; 2],
    pub logical_bytes: [u64; 2],
    pub source_unchanged: bool,
    pub database_converted: bool,
    pub activation_ready: bool,
}

/// Caller protects identity files, stopped-session admission, and primary paths.
/// This function holds both source publications and the complete target owner.
pub async fn convert_typed_ranges(
    source_root: &Path,
    target_root: &Path,
    keys: &E2eKeys,
    options: RangeConversionOptions,
) -> StorageResult<RangeConversionReport> {
    let source_root = source_root.canonicalize()?;
    let target_root = target_root.canonicalize()?;
    if source_root.starts_with(&target_root) || target_root.starts_with(&source_root) {
        return Err(invalid("source and target must be separate directories"));
    }
    if options.source_hash_encoding != HashKeyEncoding::BlindV1 {
        return Err(invalid(
            "this conversion preserves an explicitly blinded source hash domain",
        ));
    }
    let mut source = FrozenSource::open(&source_root, &keys.encryption_key())?;
    if !matches!(source.source, Source::Runs { .. }) {
        return Err(invalid("typed conversion requires a compact V2 source"));
    }
    let (plan, metadata_digest) = prepare(&mut source, keys, options)?;
    let job = job_identity(
        &source,
        &source_root,
        &target_root,
        &metadata_digest,
        options,
    )?;
    source.check_binding()?;
    let mut target = Target::open(&target_root, &keys.encryption_key(), &job, options.resume)?;
    copy(&source, &plan, &mut target)?;
    target = target.reopen(&target_root, &keys.encryption_key())?;
    verify(&source, &plan, &target)?;
    super::conversion_validate::validate(&target.homes).await?;
    source.check_binding()?;
    target.checkpoint.phase = Phase::Complete;
    conversion_copy::commit(&target.homes[0], vec![target.checkpoint.mutation()?])?;
    source.check_binding()?;
    target.publish(&target_root)?;
    source.check_binding()?;
    Ok(RangeConversionReport {
        version: 1,
        job,
        records: target.checkpoint.records,
        logical_bytes: target.checkpoint.logical_bytes,
        source_unchanged: true,
        database_converted: true,
        activation_ready: true,
    })
}

fn prepare(
    source: &mut FrozenSource,
    keys: &E2eKeys,
    options: RangeConversionOptions,
) -> StorageResult<(ConversionPlan, String)> {
    let limits = options.limits;
    if limits.max_records == 0 || limits.max_metadata_bytes == 0 || limits.max_key_bytes == 0 {
        return Err(invalid("conversion limits must be positive"));
    }
    let records = source.collect(limits)?;
    let codec = MoleculeKeyCodec::with_encodings(
        options.source_hash_encoding,
        options.source_range_encoding,
        Some(keys.index_key()),
        Some(keys.ope_key()),
    );
    let mut catalog = Catalog::load(&records, &keys.encryption_key(), &codec, limits)?;
    let mut families = BTreeMap::new();
    let mut digest = Sha256::new();
    for row in &records {
        super::keys::observe(&mut catalog, row, &mut families, limits)?;
        for bytes in [
            row.collection.as_bytes(),
            row.key.as_bytes(),
            row.value.as_slice(),
        ] {
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        }
    }
    catalog.finish(limits, RangeKeyEncoding::Plain)?;
    let mut plan = ConversionPlan::build(
        &catalog,
        &records,
        &keys.encryption_key(),
        options.source_range_encoding,
        limits,
    )?;
    plan.translate_metadata(&records)?;
    let Source::Runs { main, .. } = &source.source else {
        return Err(invalid("V2 source is required"));
    };
    let mut reader = main.fork(super::source::budget(8 * 1024 * 1024, 2));
    if reader
        .get_bounded(
            conversion_copy::CHECKPOINT_COLLECTION,
            conversion_copy::CHECKPOINT_KEY,
            1024 * 1024,
        )
        .map_err(crate::storage::disk_pack::map)?
        .is_some()
    {
        return Err(invalid("source has reserved conversion state"));
    }
    if reader
        .get_bounded("__native_home", "namespaces\0molecule_keys", 8192)
        .map_err(crate::storage::disk_pack::map)?
        .is_none()
    {
        plan.extras.push(HomeMutation {
            collection: "__native_home".into(),
            key: "namespaces\0molecule_keys".into(),
            body: Some(vec![1]),
        });
    }
    Ok((plan, format!("{:x}", digest.finalize())))
}

fn copy(source: &FrozenSource, plan: &ConversionPlan, target: &mut Target) -> StorageResult<()> {
    let Source::Runs { main, _cas: cas } = &source.source else {
        return Err(invalid("V2 source is required"));
    };
    while matches!(target.checkpoint.phase, Phase::Main | Phase::Cas) {
        source.check_binding()?;
        let input = if target.checkpoint.phase == Phase::Main {
            main
        } else {
            cas.as_ref()
        };
        if !conversion_copy::copy_page(input, &target.homes, plan, &mut target.checkpoint)? {
            continue;
        }
        if target.checkpoint.phase == Phase::Main {
            for extras in plan.extras.chunks(512) {
                conversion_copy::commit(&target.homes[0], extras.to_vec())?;
            }
            target.checkpoint.phase = Phase::Cas;
        } else {
            target.checkpoint.phase = Phase::Copied;
        }
        target.checkpoint.after = None;
        conversion_copy::commit(&target.homes[0], vec![target.checkpoint.mutation()?])?;
    }
    Ok(())
}

fn verify(source: &FrozenSource, plan: &ConversionPlan, target: &Target) -> StorageResult<()> {
    let Source::Runs { main, _cas: cas } = &source.source else {
        return Err(invalid("V2 source is required"));
    };
    let sources = [main, cas.as_ref()];
    for slot in 0..2 {
        source.check_binding()?;
        let checked = conversion_copy::verify(sources[slot], &target.homes[slot], plan, slot)?;
        if checked != target.checkpoint.records[slot] {
            return Err(invalid(
                "verified source count differs from encrypted progress",
            ));
        }
        let expected = checked
            + if slot == 0 {
                plan.extras.len() as u64 + 1
            } else {
                0
            };
        if conversion_copy::count(&target.homes[slot])? != expected {
            return Err(invalid("target contains missing or unexpected records"));
        }
    }
    for extras in plan.extras.chunks(128) {
        conversion_copy::compare(&target.homes[0], extras)?;
    }
    Ok(())
}

fn job_identity(
    source: &FrozenSource,
    source_path: &Path,
    target_path: &Path,
    metadata: &str,
    options: RangeConversionOptions,
) -> StorageResult<String> {
    let mut digest = Sha256::new();
    for part in [
        b"lastdb:typed-range-conversion:v1".as_slice(),
        source_path.as_os_str().as_encoded_bytes(),
        target_path.as_os_str().as_encoded_bytes(),
        metadata.as_bytes(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    for binding in &source.bindings {
        digest.update(binding.digest.as_bytes());
    }
    digest.update(
        serde_json::to_vec(&(
            options.source_hash_encoding,
            options.source_range_encoding,
            options.limits,
        ))
        .map_err(|_| invalid("cannot bind conversion options"))?,
    );
    Ok(format!("{:x}", digest.finalize()))
}

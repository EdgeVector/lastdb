//! Explicit V2-to-V2 typed conversion. The source never receives a writer.

use super::{
    catalog::Catalog,
    conversion_copy::{self, Phase},
    conversion_plan::ConversionPlan,
    conversion_target::Target,
    invalid,
    source::FrozenSource,
    InventoryLimits, StorageResult,
};
use crate::atom::{HashKeyEncoding, MoleculeKeyCodec, RangeKeyEncoding};
use crate::crypto::E2eKeys;
use laststore::disk_run::home::HomeMutation;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
    pub capacity: super::ConversionCapacity,
}

/// A complete source plan keeps both read-only publications held before any
/// target identity or target directory appears.
pub struct PreparedRangeConversion {
    source: FrozenSource,
    plan: ConversionPlan,
    target_root: PathBuf,
    account_key: zeroize::Zeroizing<[u8; 32]>,
    job: String,
    resume: bool,
    fenced: bool,
    estimate: super::ConversionSpaceEstimate,
}

impl PreparedRangeConversion {
    /// Caller protects stopped-session admission and primary paths.
    pub fn prepare(
        source_root: &Path,
        target_root: &Path,
        keys: &E2eKeys,
        options: RangeConversionOptions,
    ) -> StorageResult<Self> {
        let source_root = source_root.canonicalize()?;
        let target_root = destination(target_root)?;
        if source_root.starts_with(&target_root) || target_root.starts_with(&source_root) {
            return Err(invalid("source and target must be separate directories"));
        }
        if options.source_hash_encoding != HashKeyEncoding::BlindV1 {
            return Err(invalid(
                "this conversion preserves an explicitly blinded source hash domain",
            ));
        }
        let mut source = FrozenSource::open(&source_root, &keys.encryption_key())?;
        let (plan, metadata_digest) = prepare(&mut source, keys, options)?;
        let estimate = super::capacity::estimate(&source_root, &source, &plan)?;
        let job = job_identity(
            &source,
            &source_root,
            &target_root,
            &metadata_digest,
            options,
        )?;
        source.check_binding()?;
        Ok(Self {
            source,
            plan,
            target_root,
            account_key: zeroize::Zeroizing::new(keys.encryption_key()),
            job,
            resume: options.resume,
            fenced: false,
            estimate,
        })
    }

    /// The caller atomically installs this fenced staging home at its target path.
    pub fn write_staged_fence(&mut self, data: &Path) -> StorageResult<()> {
        self.require_capacity()?;
        self.source.check_binding()?;
        super::conversion_target::stage_fence(data, &self.job)?;
        self.fenced = true;
        Ok(())
    }

    pub async fn execute(self) -> StorageResult<RangeConversionReport> {
        let capacity = self.require_capacity()?;
        let Self {
            source,
            plan,
            target_root,
            account_key,
            job,
            resume,
            fenced,
            estimate: _,
        } = self;
        if target_root.canonicalize()? != target_root {
            return Err(invalid("target path changed after source admission"));
        }
        source.check_binding()?;
        let mut target = Target::open(&target_root, &account_key, &job, resume || fenced)?;
        copy(&source, &plan, &mut target, &target_root)?;
        target.check_binding(&target_root)?;
        target = target.reopen(&target_root, &account_key)?;
        verify(&source, &plan, &target)?;
        super::conversion_validate::validate(&target.homes).await?;
        source.check_binding()?;
        target.checkpoint.phase = Phase::Complete;
        conversion_copy::commit(&target.homes[0], vec![target.checkpoint.mutation()?])?;
        source.check_binding()?;
        target.publish(&target_root)?;
        source.check_binding()?;
        Ok(RangeConversionReport {
            version: 2,
            job,
            records: target.checkpoint.records,
            logical_bytes: target.checkpoint.logical_bytes,
            source_unchanged: true,
            database_converted: true,
            activation_ready: true,
            capacity,
        })
    }

    /// Recheck the selected target filesystem without a writer or directory creation.
    pub fn capacity(&self) -> StorageResult<super::ConversionCapacity> {
        self.estimate.capacity(&self.target_root)
    }

    pub fn require_capacity(&self) -> StorageResult<super::ConversionCapacity> {
        self.estimate.require(&self.target_root)
    }

    pub fn source_publications_sha256(&self) -> String {
        let mut digest = Sha256::new();
        for binding in &self.source.bindings {
            digest.update(binding.digest.as_bytes());
        }
        format!("{:x}", digest.finalize())
    }
}

fn destination(path: &Path) -> StorageResult<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("target path has no parent"))?;
            let name = path
                .file_name()
                .ok_or_else(|| invalid("target path has no name"))?;
            Ok(destination(parent)?.join(name))
        }
        Err(error) => Err(error.into()),
    }
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
    let main = &source.source.main;
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

fn copy(
    source: &FrozenSource,
    plan: &ConversionPlan,
    target: &mut Target,
    target_root: &Path,
) -> StorageResult<()> {
    let main = &source.source.main;
    let cas = &source.source.cas;
    while matches!(target.checkpoint.phase, Phase::Main | Phase::Cas) {
        source.check_binding()?;
        target.check_binding(target_root)?;
        let input = if target.checkpoint.phase == Phase::Main {
            main
        } else {
            cas
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
    let sources = [&source.source.main, &source.source.cas];
    for (slot, input) in sources.into_iter().enumerate() {
        source.check_binding()?;
        let checked = conversion_copy::verify(input, &target.homes[slot], plan, slot)?;
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

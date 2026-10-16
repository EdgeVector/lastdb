use super::GenerationHold;
use crate::disk_pack::{corrupt, generation, Current, PAGE_BYTES};
use crate::Result;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// An exact relative path. No discovered arbitrary path enters an inventory.
#[derive(Clone, Debug)]
pub struct GenerationFile {
    /// Relative to the explicitly selected native root.
    pub path: PathBuf,
    /// Durable file length, verified against the generation receipt.
    pub bytes: u64,
    /// Original creation generation; compaction never changes source files.
    pub creation_generation: u64,
}

/// Complete physical snapshot metadata. CURRENT and TAIL require snapshot encodings.
pub struct GenerationInventory {
    /// Exact root to encode as the restored CURRENT.
    pub current: Current,
    /// Immutable generation receipt identity.
    pub receipt: Uuid,
    /// Complete dependency parts, receipt pages, and immutable format descriptors.
    pub files: Vec<GenerationFile>,
}

/// Verify every exact dependency while an active read hold prevents reclamation.
pub fn generation_inventory(root: &Path, current: &Current) -> Result<GenerationInventory> {
    GenerationHold::acquire(root, current)?.inventory()
}

pub(super) fn held_inventory(root: &Path, current: &Current) -> Result<GenerationInventory> {
    let generation = generation::verify(root, current)?;
    let mut files = Vec::with_capacity(generation.coordinates.len() + 330);
    for (coordinate, record) in generation.coordinates.iter().zip(&generation.records) {
        let path = coordinate.path(root)?;
        files.push(GenerationFile {
            path: relative(root, &path)?,
            bytes: crate::disk_pack::bytes::u64_at(record, 32),
            creation_generation: crate::disk_pack::bytes::u64_at(record, 72),
        });
    }
    let pages = 1 + generation.dependencies.saturating_sub(150).div_ceil(201);
    for part in 0..pages.div_ceil(512) {
        files.push(GenerationFile {
            path: PathBuf::from(format!(
                "generations/{}/part-{part:08x}",
                current.generation
            )),
            bytes: (pages - part * 512).min(512) * PAGE_BYTES as u64,
            creation_generation: current.generation,
        });
    }
    let native = current.required_features & crate::disk_pack::FEATURE_NATIVE_HOME != 0;
    let names: &[&str] = if native {
        &["laststore-pack-home-v1", "laststore-layout-v1"]
    } else {
        &["disk-pack-artifact-v1"]
    };
    for name in names {
        let metadata = std::fs::symlink_metadata(root.join(name))?;
        if !metadata.is_file() {
            return Err(corrupt("immutable descriptor is not a regular file"));
        }
        files.push(GenerationFile {
            path: PathBuf::from(name),
            bytes: metadata.len(),
            creation_generation: 1,
        });
    }
    Ok(GenerationInventory {
        current: current.clone(),
        receipt: generation.object,
        files,
    })
}

fn relative(root: &Path, path: &Path) -> Result<PathBuf> {
    path.strip_prefix(root)
        .map(Path::to_path_buf)
        .map_err(|_| corrupt("dependency escaped the native root"))
}

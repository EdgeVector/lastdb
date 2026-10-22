//! Complete supported forward conversions without a legacy writer fallback.

use super::conversion_fence as fence;
use crate::{durability, Result};
use std::fs::File;
use std::path::Path;

pub(super) const REVERSE_FENCE: &str = "laststore-legacy-source-sealed-v1";

/// Complete an interrupted cutover only after its durable source fence identifies
/// this exact pending target. A target without a source fence stays dormant.
/// No source data is opened as a database, overwritten, or deleted.
pub fn finish_conversion(source: &Path, destination: &Path) -> Result<()> {
    let value = fence::forward_value(source)?.ok_or_else(|| {
        crate::Error::Config(
            "no supported forward conversion fence; legacy reverse completion is retired".into(),
        )
    })?;
    let destination = std::fs::canonicalize(destination)?;
    if value["format"] != 1
        || value["destination"].as_str().map(Path::new) != Some(destination.as_path())
    {
        return Err(crate::Error::Corrupt(
            "conversion source fence destination differs".into(),
        ));
    }
    // Every path, including an already-completed retry, refuses retired formats
    // before it changes a completion marker or releases a durable reference.
    let current = super::artifact::current(&destination)?;
    super::home::HomeDescriptor::read(&destination)?.check_current(&current)?;
    let token = conversion_uuid(&value, "token")?;
    let reference = conversion_uuid(&value, "reference")?;
    if !destination.join(fence::PENDING).try_exists()? {
        if fence::bounded_file(&destination.join(fence::COMPLETE), 36)?
            != token.to_string().as_bytes()
        {
            return Err(crate::Error::Corrupt(
                "completed conversion token differs from source fence".into(),
            ));
        }
        return release_conversion_reference(&destination, reference);
    }
    fence::validate(&destination, token)?;
    {
        let _writer = super::write_lock::WriteLock::acquire_conversion(&destination, token)?;
        let raw = fence::bounded_file(&destination.join("CURRENT"), 4096)?;
        if raw.len() != 4096 || value["current_digest"] != fence::digest(&raw) {
            return Err(crate::Error::Corrupt(
                "pending native boundary differs from source fence".into(),
            ));
        }
        let fixed: &[u8; 4096] = raw
            .as_slice()
            .try_into()
            .map_err(|_| crate::Error::Corrupt("pending CURRENT length".into()))?;
        let current = super::Current::decode(fixed)?;
        super::home::HomeDescriptor::read(&destination)?.check_current(&current)?;
        let _inventory = super::lifetime::generation_inventory(&destination, &current)?;
        if !source.join(fence::FORWARD_FENCE).try_exists()? {
            fence::publish(source, fence::FORWARD_FENCE, &value)?;
        }
        fence::finish(&destination, token)?;
        release_conversion_reference(&destination, reference)?;
    }
    Ok(())
}

fn conversion_uuid(value: &serde_json::Value, field: &str) -> Result<uuid::Uuid> {
    value[field]
        .as_str()
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .filter(|identity| !identity.is_nil())
        .ok_or_else(|| crate::Error::Corrupt("conversion fence UUID".into()))
}

fn release_conversion_reference(root: &Path, reference: uuid::Uuid) -> Result<()> {
    match super::lifetime::release_reference(root, reference) {
        // A retry after the completed unlink still needs its directory barrier.
        Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            durability::sync_directory(&File::open(root.join("references"))?)
        }
        result => result,
    }
}

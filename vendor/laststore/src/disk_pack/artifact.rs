use super::{corrupt, Current, ReadBudget, ReadCall, ReadCosts};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

pub(super) fn current(root: &Path) -> Result<Current> {
    let native = if root.join(super::home::MARKER).try_exists()? {
        if root.join("disk-pack-artifact-v1").try_exists()? {
            return Err(corrupt("ambiguous artifact and native home markers"));
        }
        Some(super::home::HomeDescriptor::read(root)?)
    } else {
        None
    };
    if native.is_none() {
        require_marker(root)?;
    }
    let _class = ReadClass::FormatProbe.enter();
    let mut file = measured::open(root.join("CURRENT"))?;
    if file.metadata()?.len() != 4096 {
        return Err(corrupt("CURRENT length"));
    }
    let mut bytes = [0; 4096];
    file.read_exact(&mut bytes)?;
    let current = Current::decode(&bytes)?;
    if let Some(descriptor) = native {
        descriptor.check_current(&current)?;
    } else if current.required_features & super::FEATURE_NATIVE_HOME != 0 {
        return Err(corrupt("native CURRENT requires a native home descriptor"));
    }
    Ok(current)
}

pub(super) fn require_marker(root: &Path) -> Result<()> {
    let expected = b"artifact-only=1\nformat=1\n";
    let _class = ReadClass::FormatProbe.enter();
    let mut marker = measured::open(root.join("disk-pack-artifact-v1"))?;
    if marker.metadata()?.len() != expected.len() as u64 {
        return Err(corrupt("not a separate artifact"));
    }
    let mut bytes = vec![0; expected.len()];
    marker.read_exact(&mut bytes)?;
    if bytes != expected {
        return Err(corrupt("not a separate artifact"));
    }
    Ok(())
}

/// Administrative work still has finite per-path allocation and I/O limits.
pub(super) fn metadata_call(root: &Path, current: Current) -> ReadCall {
    ReadCall {
        root: root.to_path_buf(),
        current,
        budget: ReadBudget {
            pages: 4096,
            blocks: 16_384,
            bytes: 256 * 1024 * 1024,
            keys: 10_000,
        },
        costs: ReadCosts::default(),
        pages: HashMap::new(),
        blocks: HashMap::new(),
        parts: HashMap::new(),
        files: super::call_io::CallFiles::default(),
        _generation_hold: None,
        payload_class: ReadClass::Payload,
        native_descriptor: None,
    }
}

pub(super) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

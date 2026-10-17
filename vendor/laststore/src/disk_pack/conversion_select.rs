use super::conversion_fence as fence;
use crate::{Error, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Explicit active storage selected by a complete, verified conversion chain.
pub struct ResolvedStore {
    /// Native generation home when true; legacy LastStore root otherwise.
    pub native: bool,
    /// Canonical selected storage root. The node identity remains at its original home.
    pub path: PathBuf,
}

/// Follow at most eight explicit conversions. Missing, incomplete, cyclic, or
/// mismatched targets fail closed. Normal homes return None and retain selection.
pub fn resolve_conversion(root: &Path) -> Result<Option<ResolvedStore>> {
    if !root.try_exists()? {
        return Ok(None);
    }
    let initial = if root.join("laststore-pack-node-v1").try_exists()? {
        root.join("disk-pack")
    } else {
        root.to_path_buf()
    };
    let mut path = std::fs::canonicalize(initial)?;
    let mut visited = BTreeSet::new();
    let mut selected = None;
    for handoffs in 0..=8 {
        if !visited.insert(path.clone()) {
            return Err(Error::Corrupt("conversion selection cycle".into()));
        }
        let (native, value) = if let Some(value) = fence::forward_value(&path)? {
            (true, value)
        } else if path.join(super::conversion::REVERSE_FENCE).try_exists()? {
            (false, fence::read(&path, super::conversion::REVERSE_FENCE)?)
        } else {
            return Ok(selected);
        };
        if handoffs == 8 {
            return Err(Error::Corrupt(
                "conversion selection exceeds eight handoffs".into(),
            ));
        }
        if value["format"] != 1 {
            return Err(Error::Corrupt("conversion selection format".into()));
        }
        let destination = value["destination"]
            .as_str()
            .ok_or_else(|| Error::Corrupt("conversion selection destination".into()))?;
        let destination = std::fs::canonicalize(destination)?;
        let token = value["token"]
            .as_str()
            .ok_or_else(|| Error::Corrupt("conversion selection token".into()))?;
        if uuid::Uuid::parse_str(token).is_err() {
            return Err(Error::Corrupt("conversion token UUID".into()));
        }
        fence::refuse_pending(&destination)?;
        if fence::bounded_file(&destination.join(fence::COMPLETE), 36)? != token.as_bytes() {
            return Err(Error::Corrupt(
                "conversion completion differs from source fence".into(),
            ));
        }
        path = destination;
        selected = Some(ResolvedStore {
            native,
            path: path.clone(),
        });
    }
    Err(Error::Corrupt(
        "conversion selection exceeds eight handoffs".into(),
    ))
}

//! Client-owned native file extent metadata for verified share snapshots.
use cloud_backup_protocol::{DecimalU64, Digest, RelativePath};
use serde::{Deserialize, Serialize};

const MAX_FILE_EXTENTS: usize = 16384;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientExtent {
    pub receipt_id: Digest,
    pub offset: DecimalU64,
    pub length: DecimalU64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientNativeFile {
    pub path: RelativePath,
    pub native_sha256: Digest,
    pub native_bytes: DecimalU64,
    pub extents: Vec<ClientExtent>,
}

impl ClientNativeFile {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.native_bytes.get() == 0 || !(1..=MAX_FILE_EXTENTS).contains(&self.extents.len()) {
            return Err("byte native file extent count");
        }
        let sum = self.extents.iter().try_fold(0u64, |acc, e| {
            let n = e.length.get();
            (n > 0 && e.offset.get().checked_add(n).is_some())
                .then(|| acc.checked_add(n))
                .flatten()
        });
        match sum {
            Some(b) if b == self.native_bytes.get() => Ok(()),
            Some(_) => Err("byte native file exact length"),
            None => Err("byte native extent bounds"),
        }
    }
}

//! Bounded canonical inventories and stable references. Download grants remain ephemeral.

use super::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::io::Write;

pub const INVENTORY_METADATA_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const INVENTORY_PAGE_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudInventoryRef {
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
}

impl CloudInventoryRef {
    pub fn validate(&self, account: &CloudAccount, operation: &str, kind: &str) -> SyncResult<()> {
        if uuid::Uuid::parse_str(operation).is_err()
            || !matches!(kind, "source" | "replacement")
            || account.id.is_empty()
            || account.id.contains(['/', '\\'])
            || account.id.chars().any(char::is_control)
            || !crate::hex::is_lower_hex_sha256(&self.sha256)
            || self.bytes == 0
            || self.bytes > INVENTORY_METADATA_MAX_BYTES as u64
            || self.key != inventory_key(account, operation, kind, &self.sha256)
        {
            return Err(SyncError::Storage(
                "cloud inventory reference identity differs".into(),
            ));
        }
        Ok(())
    }
}

pub fn inventory_key(account: &CloudAccount, operation: &str, kind: &str, sha: &str) -> String {
    let kind_name = match account.kind {
        CloudAccountKind::Personal => "personal",
        CloudAccountKind::Org => "org",
    };
    format!(
        "__lastdb_control/v1/{kind_name}/{}/inventories/{operation}/{kind}/{sha}.json",
        account.id
    )
}

pub fn canonical_inventory<T: Serialize>(value: &T, maximum: usize) -> SyncResult<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    bounded_json(&value, maximum)
}

pub fn bounded_json<T: Serialize>(value: &T, maximum: usize) -> SyncResult<Vec<u8>> {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        SyncError::Storage(format!(
            "cloud inventory or local receipt exceeds {maximum} bytes before hold"
        ))
    })?;
    Ok(writer.bytes)
}

struct BoundedWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("metadata budget exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

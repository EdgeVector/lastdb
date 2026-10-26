use super::super::{invalid, store, Inner, StorageResult};
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::Read;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::storage::disk_pack::byte_backup) enum Coordinate {
    Inline { collection: String, key: String },
    External { sha256: Digest, bytes: u64 },
}
impl Coordinate {
    pub fn save(inner: &Inner, collection: &str, key: &str) -> StorageResult<Self> {
        if collection.len() + key.len() <= 2048 {
            return Ok(Self::Inline {
                collection: collection.into(),
                key: key.into(),
            });
        }
        let collection_bytes = (collection.len() as u32).to_le_bytes();
        let key_bytes = (key.len() as u64).to_le_bytes();
        let parts: [&[u8]; 4] = [
            &collection_bytes,
            collection.as_bytes(),
            &key_bytes,
            key.as_bytes(),
        ];
        let mut hash = Sha256::new();
        for part in parts {
            hash.update(part);
        }
        let digest =
            cloud_backup_protocol::Digest::try_from(crate::hex::hex_lower(hash.finalize()))
                .map_err(super::super::protocol)?;
        let name = format!("coordinate-{}.raw", digest.as_str());
        inner.store.write_parts(&name, &parts)?;
        Ok(Self::External {
            sha256: digest,
            bytes: 12 + collection.len() as u64 + key.len() as u64,
        })
    }
    pub fn load(&self, inner: &Inner) -> StorageResult<(String, String)> {
        match self {
            Self::Inline { collection, key } => Ok((collection.clone(), key.clone())),
            Self::External { sha256, bytes } => {
                let maximum = laststore::disk_pack::EXTENDED_STORAGE_KEY_BYTES as u64 + 65531 + 12;
                if *bytes > maximum {
                    return Err(invalid("stored coordinate exceeds native key bounds"));
                }
                let path = inner
                    .store
                    .root
                    .join(format!("coordinate-{}.raw", sha256.as_str()));
                let mut file = store::input(&path)?;
                if file.metadata()?.len() != *bytes {
                    return Err(invalid("stored coordinate length"));
                }
                let mut length = [0; 4];
                file.read_exact(&mut length)?;
                let collection_len = u32::from_le_bytes(length) as usize;
                if collection_len > 65531 {
                    return Err(invalid("stored collection length"));
                }
                let mut collection = vec![0; collection_len];
                file.read_exact(&mut collection)?;
                let mut key_size = [0; 8];
                file.read_exact(&mut key_size)?;
                let key_len = u64::from_le_bytes(key_size);
                if key_len > laststore::disk_pack::EXTENDED_STORAGE_KEY_BYTES as u64
                    || 12 + collection_len as u64 + key_len != *bytes
                {
                    return Err(invalid("stored coordinate key length"));
                }
                let mut key = vec![0; key_len as usize];
                file.read_exact(&mut key)?;
                let mut hash = Sha256::new();
                hash.update(length);
                hash.update(&collection);
                hash.update(key_size);
                hash.update(&key);
                if crate::hex::hex_lower(hash.finalize()) != sha256.as_str() {
                    return Err(invalid("stored coordinate checksum"));
                }
                Ok((
                    String::from_utf8(collection).map_err(|_| invalid("stored collection UTF8"))?,
                    String::from_utf8(key).map_err(|_| invalid("stored key UTF8"))?,
                ))
            }
        }
    }
}

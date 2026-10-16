//! Small durable conversion state; incomplete targets never open as active homes.

use crate::{durability, Error, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use uuid::Uuid;

pub(crate) const PENDING: &str = "laststore-conversion-pending-v1";
pub(crate) const FORWARD_FENCE: &str = "laststore-native-source-sealed-v1";

pub(crate) fn pending(root: &Path) -> Result<Uuid> {
    let token = Uuid::new_v4();
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join(PENDING))?;
    file.write_all(token.to_string().as_bytes())?;
    durability::sync_dirty_file(&file)?;
    durability::sync_directory(&File::open(root)?)?;
    Ok(token)
}

pub(crate) fn refuse_pending(root: &Path) -> Result<()> {
    if root.join(PENDING).try_exists()? {
        return Err(Error::Config(
            "conversion destination is pending; complete its source cutover first".into(),
        ));
    }
    Ok(())
}

pub(crate) fn finish(root: &Path, token: Uuid) -> Result<()> {
    validate(root, token)?;
    std::fs::remove_file(root.join(PENDING))?;
    durability::sync_directory(&File::open(root)?)
}

pub(crate) fn validate(root: &Path, token: Uuid) -> Result<()> {
    let mut file = File::open(root.join(PENDING))?;
    if file.metadata()?.len() != 36 {
        return Err(Error::Corrupt("conversion pending token length".into()));
    }
    let mut bytes = String::new();
    file.read_to_string(&mut bytes)?;
    if bytes != token.to_string() {
        return Err(Error::Corrupt("conversion pending token differs".into()));
    }
    Ok(())
}

pub(crate) fn publish(root: &Path, name: &str, value: &Value) -> Result<()> {
    if root.join(name).try_exists()? {
        return Err(Error::Config(
            "source conversion fence already exists".into(),
        ));
    }
    let temporary = root.join(format!(".conversion-fence-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let bytes = serde_json::to_vec(value).map_err(|error| Error::Config(error.to_string()))?;
    file.write_all(&bytes)?;
    durability::sync_dirty_file(&file)?;
    std::fs::rename(temporary, root.join(name))?;
    durability::sync_directory(&File::open(root)?)
}

pub(crate) fn read(root: &Path, name: &str) -> Result<Value> {
    let file = File::open(root.join(name))?;
    if file.metadata()?.len() > 65_536 {
        return Err(Error::Corrupt("conversion fence size".into()));
    }
    let mut bytes = Vec::new();
    file.take(65_537).read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| Error::Corrupt("conversion fence JSON".into()))
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn bounded_file(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit {
        return Err(Error::Corrupt("conversion metadata size".into()));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(Error::Corrupt("conversion metadata grew".into()));
    }
    Ok(bytes)
}

use super::Result;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::RngCore;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use zeroize::Zeroizing;

const DOMAIN: &[u8] = b"lastdb-peer-sync-spike-packet-v1";
const MAX_PACKET: usize = 1024 * 1024;

pub(super) fn create_key(path: &Path) -> Result<()> {
    let mut key = Zeroizing::new([0_u8; 32]);
    rand::rngs::OsRng.fill_bytes(&mut *key);
    let mut file = private_file(path)?;
    file.write_all(&*key)?;
    file.sync_all()?;
    sync_parent(path)
}

pub(super) fn read_key(path: &Path) -> Result<Zeroizing<[u8; 32]>> {
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err("key must be a regular file".into());
    }
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)?.take(33).read_to_end(&mut bytes)?;
    let key: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "key must contain 32 bytes")?;
    Ok(Zeroizing::new(key))
}

pub(super) fn private_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

pub(super) fn sync_parent(path: &Path) -> Result<()> {
    File::open(
        path.parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

pub(super) fn write_packet(
    path: &Path,
    root: &str,
    entries: &[impl serde::Serialize],
) -> Result<usize> {
    let clear = Zeroizing::new(serde_json::to_vec(entries)?);
    if clear.len() > MAX_PACKET {
        return Err("export page exceeds one MiB".into());
    }
    let key = read_key(&Path::new(root).join("ACCOUNT_KEY"))?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| "packet key")?;
    let mut nonce = [0_u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let bytes = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &clear,
                aad: DOMAIN,
            },
        )
        .map_err(|_| "packet encryption")?;
    let mut file = private_file(path)?;
    file.write_all(&nonce)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent(path)?;
    Ok(bytes.len() + nonce.len())
}

pub(super) fn read_packet<T: serde::de::DeserializeOwned>(path: &Path, root: &str) -> Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take((MAX_PACKET + 29) as u64)
        .read_to_end(&mut bytes)?;
    if !(28..=MAX_PACKET + 28).contains(&bytes.len()) {
        return Err("packet size outside the bounded page".into());
    }
    let key = read_key(&Path::new(root).join("ACCOUNT_KEY"))?;
    let cipher = Aes256Gcm::new_from_slice(&*key).map_err(|_| "packet key")?;
    let clear = Zeroizing::new(
        cipher
            .decrypt(
                Nonce::from_slice(&bytes[..12]),
                Payload {
                    msg: &bytes[12..],
                    aad: DOMAIN,
                },
            )
            .map_err(|_| "packet authentication")?,
    );
    Ok(serde_json::from_slice(&clear)?)
}

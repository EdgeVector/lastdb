//! The uploads of a snapshot: the birth, the files that changed, and the manifest.

use std::io::Seek;
use std::path::Path;
use std::time::Instant;

use fold_db::storage::disk_pack::snapshot::DatabaseSnapshot;
use sha2::{Digest, Sha256};

use super::snapshot_manifest::{FileEntry, Manifest};
use super::snapshot_state::{file_of, Last};
use super::Ctx;
use crate::log_sync::blob_transfer::SIZE_REFUSED;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

/// Starts the error of a snapshot that cannot be published until the relay takes larger files.
pub(super) const TOO_LARGE: &str = "snapshot cannot be published";

/// The relay takes a blob only for a database whose birth it holds.
pub(super) async fn ensure_birth(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
) -> Result<(), String> {
    if streams
        .iter()
        .any(|stream| stream.device_id == ctx.birth_device_hex)
    {
        return Ok(());
    }
    let (stored, duplicate) = client
        .append(&ctx.database_id, std::slice::from_ref(&ctx.birth_entry))
        .await?;
    if stored + duplicate == 1 {
        Ok(())
    } else {
        Err("the relay did not take the birth entry".into())
    }
}

/// Upload the manifest as a blob. Returns its digest and length.
pub(super) async fn put_manifest(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    list: &Manifest,
) -> Result<(String, u64), String> {
    let bytes = serde_json::to_vec(list).map_err(|_| "cannot encode the manifest")?;
    let sha256 = app_identity_crypto::hex_lower(&Sha256::digest(&bytes));
    let path = file_of(&ctx.host.home, &ctx.database_id, "snapshot-manifest");
    disk::write_private(&path, &bytes).map_err(|_| "cannot save the manifest".to_owned())?;
    let result = client.blob_put(&ctx.database_id, &sha256, &path).await;
    let _ = disk::remove_private(&path);
    result?;
    Ok((sha256, bytes.len() as u64))
}

/// Upload each file of the cut that the previous snapshot of this device did not hold, as a
/// blob named by its digest. Returns how many files and bytes went up.
pub(super) async fn upload_files(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    files: &[FileEntry],
    previous: Option<&Last>,
) -> Result<(usize, u64), String> {
    let known: Vec<(&str, &str)> = previous.map_or_else(Vec::new, |last| {
        last.manifest
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.sha256.as_str()))
            .collect()
    });
    let (mut uploaded_files, mut uploaded_bytes) = (0, 0);
    for entry in files {
        if known.contains(&(entry.path.as_str(), entry.sha256.as_str())) {
            continue;
        }
        let mut file = cut
            .open_file(Path::new(&entry.path))
            .map_err(|error| error.to_string())?;
        file.rewind().map_err(|error| error.to_string())?;
        client
            .blob_put_file(
                &ctx.database_id,
                &entry.sha256,
                tokio::fs::File::from_std(file),
            )
            .await
            .map_err(|error| {
                if error.starts_with(SIZE_REFUSED) {
                    too_large(client, entry)
                } else {
                    error
                }
            })?;
        uploaded_files += 1;
        uploaded_bytes += entry.bytes;
    }
    Ok((uploaded_files, uploaded_bytes))
}

pub(super) fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// The error of a snapshot whose file the relay refused for its size. It names the run file by
/// its name only, never its content or its path, and says what a person can do.
fn too_large(client: &RelayClient, entry: &FileEntry) -> String {
    let name = Path::new(&entry.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("a data file");
    format!(
        "{TOO_LARGE}: the file {name} is {} bytes ({} MiB), and the relay at {} refuses a file that large. \
         The limit of that relay on one file is smaller. Writes and entries are not affected. \
         The snapshot can be published when the relay's limit is raised or this node can send large files in parts.",
        entry.bytes,
        entry.bytes >> 20,
        super::snapshot_known::origin(&client.base),
    )
}

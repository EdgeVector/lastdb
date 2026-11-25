//! The uploads of a snapshot: the birth, the files the relay lacks, the manifest, and the check
//! after the snapshot is named.

use std::collections::BTreeMap;
use std::io::Seek;
use std::path::Path;
use std::time::Instant;

use fold_db::storage::disk_pack::snapshot::DatabaseSnapshot;
use sha2::{Digest, Sha256};

use super::snapshot_entry::snapshot_head;
use super::snapshot_manifest::{FileEntry, Manifest};
use super::snapshot_state::{file_of, Last};
use super::Ctx;
use crate::host::cloud_sync::SnapshotState;
use crate::log_sync::blob_transfer::SIZE_REFUSED;
use crate::log_sync::relay::{RelayClient, StreamHead as RelayStream};
use crate::profile::private_fs as disk;

/// Starts the error of a snapshot that cannot be published until the relay takes larger files.
pub(super) const TOO_LARGE: &str = "snapshot cannot be published";

/// Outcome of verifying a locally unchanged snapshot against the relay's live head and blobs.
pub(super) enum UnchangedCheck {
    /// Relay head and `blob.list` confirm the manifest and every file in `last`.
    Verified,
    /// Relay lacks the snapshot head or one of its blobs; reuse any `blob.list` map already read.
    NeedsPublish(Option<BTreeMap<String, u64>>),
}

/// Confirm on the relay that `last` is still the head of the snapshot stream and every blob it
/// names (manifest and files) is stored at its expected byte length.
pub(super) async fn check_unchanged(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    streams: &[RelayStream],
    last: &Last,
) -> Result<UnchangedCheck, String> {
    let Some((head, body)) = snapshot_head(ctx, client, streams).await? else {
        return Ok(UnchangedCheck::NeedsPublish(None));
    };
    let raw = serde_json::to_vec(&last.manifest).map_err(|_| "cannot encode the manifest")?;
    let sha256 = app_identity_crypto::hex_lower(&Sha256::digest(&raw));
    let len = raw.len() as u64;
    let head_matches = head.writer_seq == last.report.snapshot_seq
        && body.manifest_sha256.to_hex() == sha256
        && body.manifest_bytes == len
        && body.through == last.through;
    if !head_matches {
        return Ok(UnchangedCheck::NeedsPublish(None));
    }
    let held = client.blob_list(&ctx.database_id).await?;
    let complete = held.get(&sha256) == Some(&len)
        && last
            .manifest
            .files
            .iter()
            .all(|file| held.get(&file.sha256) == Some(&file.bytes));
    if complete {
        Ok(UnchangedCheck::Verified)
    } else {
        Ok(UnchangedCheck::NeedsPublish(Some(held)))
    }
}

/// True when `log/append` definitively refused the entry without storing it.
pub(super) fn definitive_append_rejection(error: &str) -> bool {
    error.contains("HTTP 400") || error.contains("HTTP 403") || error.contains("HTTP 409")
}

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

/// Upload missing files of `cut` and `list`, append the `snapshot` entry, and re-verify on the
/// relay. On failure returns `(error, committed_any)`.
pub(super) async fn upload_and_name(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    list: &Manifest,
    through: &[mutation_log::SnapshotThrough],
    (first, cached_held): (bool, Option<&BTreeMap<String, u64>>),
) -> Result<(mutation_log::SignedEntry, String, usize, u64), (String, bool)> {
    let mut committed_any = false;
    let ((uploaded_files, uploaded_bytes), (sha256, length)) =
        stage_cut_blobs(ctx, client, cut, list, cached_held, &mut committed_any)
            .await
            .map_err(|e| (e, committed_any))?;
    let entry = super::snapshot_entry::append_snapshot(
        ctx,
        client,
        first,
        (sha256.as_str(), length),
        through,
    )
    .await
    .map_err(|e| (e, committed_any))?;
    let named = Named {
        list,
        sha256: &sha256,
        bytes: length,
    };
    let (again_files, again_bytes) = verify_named(ctx, client, cut, &named)
        .await
        .map_err(|e| (e, committed_any))?;
    Ok((
        entry,
        sha256,
        uploaded_files + again_files,
        uploaded_bytes + again_bytes,
    ))
}

/// Upload missing files of `cut` and `list` to the relay, setting `*committed_any = true` once
/// at least one blob commit succeeds.
async fn stage_cut_blobs(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    list: &Manifest,
    cached_held: Option<&BTreeMap<String, u64>>,
    committed_any: &mut bool,
) -> Result<((usize, u64), (String, u64)), String> {
    let fetched;
    let held = match cached_held {
        Some(held) => held,
        None => {
            fetched = client.blob_list(&ctx.database_id).await?;
            &fetched
        }
    };
    let uploaded = upload_files(ctx, client, cut, &list.files, held, committed_any).await?;
    let manifest = put_manifest(ctx, client, list, committed_any).await?;
    Ok((uploaded, manifest))
}

/// Upload the manifest as a blob. Returns its digest and length.
async fn put_manifest(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    list: &Manifest,
    committed_any: &mut bool,
) -> Result<(String, u64), String> {
    let bytes = serde_json::to_vec(list).map_err(|_| "cannot encode the manifest")?;
    let sha256 = app_identity_crypto::hex_lower(&Sha256::digest(&bytes));
    let path = file_of(&ctx.host.home, &ctx.database_id, "snapshot-manifest");
    disk::write_private(&path, &bytes).map_err(|_| "cannot save the manifest".to_owned())?;
    let result = client.blob_put(&ctx.database_id, &sha256, &path).await;
    let _ = disk::remove_private(&path);
    result?;
    *committed_any = true;
    Ok((sha256, bytes.len() as u64))
}

/// Upload each file of the cut that the relay does not hold with that digest and length, as a
/// blob named by its digest. `held` is what `blob.list` answered: the relay, not a record of
/// this device, says what is there. Returns how many files and bytes went up.
async fn upload_files(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    files: &[FileEntry],
    held: &BTreeMap<String, u64>,
    committed_any: &mut bool,
) -> Result<(usize, u64), String> {
    let todo: Vec<&FileEntry> = files
        .iter()
        .filter(|entry| held.get(&entry.sha256) != Some(&entry.bytes))
        .collect();
    let total_bytes: u64 = todo.iter().map(|entry| entry.bytes).sum();
    let (mut uploaded_files, mut uploaded_bytes) = (0, 0);
    let progress = |files: usize, bytes: u64| {
        ctx.host.cloud_sync.mark_snapshot(
            SnapshotState::InProgress,
            Some(format!(
                "uploaded {files} of {} files, {bytes} of {total_bytes} bytes",
                todo.len()
            )),
        );
    };
    progress(0, 0);
    for entry in &todo {
        put_file(ctx, client, cut, entry).await?;
        *committed_any = true;
        uploaded_files += 1;
        uploaded_bytes += entry.bytes;
        progress(uploaded_files, uploaded_bytes);
    }
    Ok((uploaded_files, uploaded_bytes))
}

/// Put one file of the cut on the relay.
async fn put_file(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    entry: &FileEntry,
) -> Result<(), String> {
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
        })
}

/// The manifest of a snapshot whose entry is on the relay, with its digest and length.
struct Named<'a> {
    list: &'a Manifest,
    sha256: &'a str,
    bytes: u64,
}

/// Ask the relay again, after the `snapshot` entry is appended: every file of the manifest and
/// the manifest itself must be there with its length. A sweep that ran before the entry was
/// appended may have dropped one, because nothing named it yet. Such a file goes up again from
/// the cut, which is still pinned. A sweep that runs after the entry keeps them: the entry names
/// the manifest, and the relay drops nothing once the head of the snapshot stream moved. Returns
/// how many files and bytes went up again.
async fn verify_named(
    ctx: &Ctx<'_>,
    client: &RelayClient,
    cut: &DatabaseSnapshot,
    named: &Named<'_>,
) -> Result<(usize, u64), String> {
    let held = client.blob_list(&ctx.database_id).await?;
    let (mut files, mut bytes) = (0, 0);
    for entry in &named.list.files {
        if held.get(&entry.sha256) == Some(&entry.bytes) {
            continue;
        }
        put_file(ctx, client, cut, entry).await?;
        files += 1;
        bytes += entry.bytes;
    }
    if held.get(named.sha256) != Some(&named.bytes) {
        let mut ignored = false;
        let (sha256, length) = put_manifest(ctx, client, named.list, &mut ignored).await?;
        if sha256 != named.sha256 || length != named.bytes {
            return Err("the manifest changed after its snapshot entry was signed".into());
        }
    }
    if files > 0 {
        tracing::info!(target: "lastdbd::log_sync", files, "snapshot files that a sweep dropped were uploaded again");
    }
    Ok((files, bytes))
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

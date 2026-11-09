use super::{read_file, runtime, MAX_SEALED_INVITATION_BYTES};
use crate::{ShareFilesCommand, ShareFilesSelection};
use cloud_backup_protocol::{Digest, Id};
use lastdb_node::share_cache::{ShareCache, ShareSnapshot};
use lastdb_node::share_file::{self, FileShareSchemas};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

mod publisher;

const SCHEMAS: FileShareSchemas<'static> = FileShareSchemas {
    catalog: "lastdb-files/FileCatalog",
    chunks: "lastdb-files/FileChunk",
};

pub(super) fn run(home: &Path, action: ShareFilesCommand) -> Result<(), String> {
    match action {
        ShareFilesCommand::Add { file } => publisher::add(home, &file),
        ShareFilesCommand::Receive {
            invitation,
            publisher,
            cache,
            handle,
        } => {
            let publisher =
                Digest::try_from(publisher).map_err(|_| "invalid publisher public key")?;
            let sealed = read_file(&invitation, MAX_SEALED_INVITATION_BYTES as u64)?;
            let cache = ShareCache::open(&cache, home).map_err(|e| e.to_string())?;
            let handle = id(handle.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()))?;
            let snapshot = runtime()?
                .block_on(cache.accept(&handle, &sealed, &publisher))
                .map_err(|e| {
                    format!("share {} could not finish receiving: {e}", handle.as_str())
                })?;
            print_snapshot(&handle, &snapshot);
            Ok(())
        }
        ShareFilesCommand::Follow {
            share,
            interval_seconds,
        } => {
            let (cache, handle) = open(home, share)?;
            runtime()?.block_on(follow(
                &cache,
                &handle,
                Duration::from_secs(interval_seconds),
            ))
        }
        ShareFilesCommand::List(share) => {
            let (cache, handle) = open(home, share)?;
            let reader = cache.reader(&handle).map_err(|e| e.to_string())?;
            let files = share_file::list_files(&SCHEMAS, |q| {
                reader.read_record(q).map_err(|e| e.to_string())
            })?;
            println!(
                "{}",
                json!({ "snapshot": snapshot_json(&handle, reader.snapshot()), "files": files })
            );
            Ok(())
        }
        ShareFilesCommand::Save { share, file, out } => {
            let (cache, handle) = open(home, share)?;
            let reader = cache.reader(&handle).map_err(|e| e.to_string())?;
            share_file::get_file(&SCHEMAS, &id(file)?, &out, |q| {
                reader.read_record(q).map_err(|e| e.to_string())
            })?;
            println!(
                "{}",
                json!({ "snapshot": snapshot_json(&handle, reader.snapshot()), "path": out })
            );
            Ok(())
        }
    }
}

fn open(home: &Path, selection: ShareFilesSelection) -> Result<(ShareCache, Id), String> {
    let handle = id(selection.handle)?;
    let cache = ShareCache::open(&selection.cache, home).map_err(|e| e.to_string())?;
    Ok((cache, handle))
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "invalid share or file UUID".into())
}

fn snapshot_json(handle: &Id, snapshot: &ShareSnapshot) -> Value {
    json!({
        "handle": handle,
        "commit_id": snapshot.commit_id,
        "commit_sha256": snapshot.commit_sha256,
        "ordinal": snapshot.ordinal,
    })
}

fn print_snapshot(handle: &Id, snapshot: &ShareSnapshot) {
    println!("{}", snapshot_json(handle, snapshot));
}

async fn follow(cache: &ShareCache, handle: &Id, interval: Duration) -> Result<(), String> {
    let mut previous = None;
    let mut last_error = None;
    let stop = tokio::signal::ctrl_c();
    tokio::pin!(stop);
    loop {
        let result = tokio::select! {
            value = cache.refresh(handle) => value,
            signal = &mut stop => return signal.map_err(|_| "share follow signal failed".into()),
        };
        match result {
            Ok(snapshot) => {
                last_error = None;
                if previous.as_ref() != Some(&snapshot.commit_sha256) {
                    print_snapshot(handle, &snapshot);
                    previous = Some(snapshot.commit_sha256);
                }
            }
            Err(error) => {
                let message = error.to_string();
                if last_error.as_ref() != Some(&message) {
                    eprintln!("share refresh pending: {message}");
                    last_error = Some(message);
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(interval) => {},
            signal = &mut stop => return signal.map_err(|_| "share follow signal failed".into()),
        }
    }
}

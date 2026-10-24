//! Exact encrypted history proof on an explicitly CLOSED owned temporary copy.
//! Ordinary native open/load/destruction may write layout, repair, or sidecars.

use super::cli_defs::RetiredIntentCommand;
use super::replay_schema_probe_cmd::{
    guard_home, identity_key, identity_unchanged, DirectoryIdentity, FileIdentity,
};
use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::db_operations::retired_intents;
use fold_db::storage::{EncryptingNamespacedStore, LastStoreNamespacedStore, NamespacedStore};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

struct ClosedCopy {
    home: PathBuf,
    directories: Vec<DirectoryIdentity>,
    seed: File,
    seed_identity: FileIdentity,
    keys: E2eKeys,
    store: Arc<dyn NamespacedStore>,
    crypto: Arc<dyn CryptoProvider>,
}

impl ClosedCopy {
    fn open(data_dir: Option<PathBuf>, closed_owned_copy: bool) -> Result<Self, String> {
        if !closed_owned_copy {
            return Err("EXPLICIT_CLOSED_COPY_ACK_REQUIRED".into());
        }
        let home = data_dir.ok_or("EXPLICIT_DATA_DIR_REQUIRED")?;
        let directories = guard_home(&home)?;
        let (seed, seed_identity, keys) = identity_key(&home)?;
        let opts = laststore::LastStoreOptions::hash_group_frame_aead(keys.encryption_key())
            .with_hash_group_key(laststore::HashGroupKey::PartitionPrefix);
        let raw: Arc<dyn NamespacedStore> = Arc::new(
            LastStoreNamespacedStore::open_with_options(&home.join("data"), opts)
                .map_err(|_| "STORE_OPEN_FAILED")?,
        );
        let crypto: Arc<dyn CryptoProvider> =
            Arc::new(LocalCryptoProvider::from_key(keys.encryption_key()));
        let store: Arc<dyn NamespacedStore> =
            Arc::new(EncryptingNamespacedStore::new(raw, Arc::clone(&crypto)));
        Ok(Self {
            home,
            directories,
            seed,
            seed_identity,
            keys,
            store,
            crypto,
        })
    }

    fn unchanged(&self) -> Result<(), String> {
        identity_unchanged(&self.home, &self.seed, self.seed_identity)?;
        if guard_home(&self.home)? != self.directories {
            return Err("OWNED_DIRECTORY_CHANGED".into());
        }
        Ok(())
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "RUNTIME_UNAVAILABLE".into())
}

fn require_fifo_stdout() -> Result<(), String> {
    let mut meta = std::mem::MaybeUninit::<libc::stat>::uninit();
    // fstat borrows stdout. It neither reads nor closes the caller's descriptor.
    if unsafe { libc::fstat(libc::STDOUT_FILENO, meta.as_mut_ptr()) } != 0 {
        return Err("PRIVATE_FIFO_STDOUT_REQUIRED".into());
    }
    let meta = unsafe { meta.assume_init() };
    if meta.st_mode & libc::S_IFMT != libc::S_IFIFO {
        return Err("PRIVATE_FIFO_STDOUT_REQUIRED".into());
    }
    Ok(())
}

fn exact_source_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn source_id_stdin() -> Result<String, String> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(66)
        .read_to_end(&mut bytes)
        .map_err(|_| "SOURCE_ID_INPUT_FAILED")?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    let value = String::from_utf8(bytes).map_err(|_| "SOURCE_ID_INPUT_REFUSED")?;
    if !exact_source_id(&value) {
        return Err("SOURCE_ID_INPUT_REFUSED".into());
    }
    Ok(value)
}

fn last_source(data_dir: Option<PathBuf>, closed_owned_copy: bool) -> Result<(), String> {
    // This guard must precede identity, namespace, or private metadata reads.
    require_fifo_stdout()?;
    let copy = ClosedCopy::open(data_dir, closed_owned_copy)?;
    let id = runtime()?
        .block_on(retired_intents::read_last_preserved_retired_source_id(
            &copy.store,
        ))
        .map_err(|_| "RETIRED_SOURCE_POINTER_REFUSED")?
        .ok_or("RETIRED_SOURCE_POINTER_ABSENT")?;
    if !exact_source_id(&id) {
        return Err("RETIRED_SOURCE_POINTER_REFUSED".into());
    }
    copy.unchanged()?;
    require_fifo_stdout()?;
    let mut pipe = std::io::stdout().lock();
    writeln!(pipe, "{id}").map_err(|_| "PRIVATE_FIFO_OUTPUT_FAILED")?;
    pipe.flush()
        .map_err(|_| "PRIVATE_FIFO_OUTPUT_FAILED".into())
}

fn proof(
    data_dir: Option<PathBuf>,
    source_id_ack: bool,
    closed_owned_copy: bool,
) -> Result<(), String> {
    if !source_id_ack || !closed_owned_copy {
        return Err("EXPLICIT_ACK_REQUIRED".into());
    }
    let id = source_id_stdin()?;
    let copy = ClosedCopy::open(data_dir, closed_owned_copy)?;
    let report = runtime()?
        .block_on(retired_intents::probe_preserved_retired_intent(
            &copy.store,
            &id,
            &copy.keys.encryption_key(),
            &copy.crypto,
        ))
        .map_err(|_| "RETIRED_INTENT_PROOF_REFUSED")?;
    copy.unchanged()?;
    let public = serde_json::json!({
        "schema": "lastdb_preserved_retired_intent_proof_v1",
        "ok": report.journal_present,
        "cloud_calls": 0,
        "ordinary_local_store_housekeeping_possible": true,
        "proof": report,
    });
    println!(
        "{}",
        serde_json::to_string(&public).map_err(|_| "PUBLIC_REPORT_FAILED")?
    );
    Ok(())
}

pub(super) fn run(data_dir: Option<PathBuf>, command: RetiredIntentCommand) -> Result<(), String> {
    match command {
        RetiredIntentCommand::LastSource { closed_owned_copy } => {
            last_source(data_dir, closed_owned_copy)
        }
        RetiredIntentCommand::Proof {
            source_id_stdin,
            closed_owned_copy,
        } => proof(data_dir, source_id_stdin, closed_owned_copy),
    }
}

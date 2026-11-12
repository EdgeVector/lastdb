//! Guest staging, recovery keys, and verified restored-home publication.
use super::{
    acquire_native_work, load_closure, local, logs, reader_error, runs, snapshot_closure,
    ByteBackupReader, RestoreError, RestoreMode, RestoreReport, RestoreSession, Result,
    RESTORE_RECEIPT_NAME, RESTORE_RECEIPT_VERSION,
};
use crate::byte_cloud_log::MutationLogRoot;
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::publication::{Head, HeadReadResult};
use cloud_backup_protocol::v2::AcceptedBackup;
use cloud_backup_protocol::{AccountId, Digest, Id, NativeRoot, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::{mutation_log::LogPosition, run_restore::RunRestoreGuard};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

/// The caller supplies the phrase again after restart; the session stores no secret.
pub struct RecoveryKeys {
    pub(super) seed: Zeroizing<[u8; 32]>,
    pub(super) decryption: RecoveryDecryption,
}

/// Key material used to verify and decrypt a backup.
pub(super) struct RecoveryDecryption {
    pub(super) content: Zeroizing<[u8; 32]>,
    pub(super) account_public_key: String,
}

impl RecoveryKeys {
    pub fn from_scope_phrase(phrase: &str, account_id: &AccountId) -> Result<Self> {
        let seed = Zeroizing::new(
            crate::cloud::seed_from_phrase(phrase)
                .map_err(|_| RestoreError::Invalid("recovery phrase"))?,
        );
        let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| RestoreError::Invalid("recovery key"))?;
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| RestoreError::Invalid("recovery derivation"))?;
        if crate::cloud::account_from_phrase(phrase).ok().as_ref() != Some(account_id) {
            return Err(RestoreError::Invalid("recovery account"));
        }
        Ok(Self {
            seed,
            decryption: RecoveryDecryption {
                content: Zeroizing::new(keys.encryption_key()),
                account_public_key: fold_db::hex::hex_lower(pair.public_key_bytes()),
            },
        })
    }
}

impl RecoveryDecryption {
    pub(super) fn validate_account(&self, pubkey: &Digest) -> Result<()> {
        (self.account_public_key == pubkey.as_str())
            .then_some(())
            .ok_or(RestoreError::Invalid("recovery public key"))
    }
}

/// Read-only previous state while the cache holds its slot lock.
pub(crate) struct GuestReplayCache<'a> {
    pub(crate) data: &'a Path,
    pub(crate) head: &'a Head,
    pub(crate) native_root: &'a NativeRoot,
    pub(crate) through: Option<&'a LogPosition>,
}

impl RestoreSession {
    pub(crate) async fn guest_accepted(reader: &ByteBackupReader) -> Result<AcceptedBackup> {
        let stream = reader
            .guest_stream()
            .ok_or(RestoreError::Invalid("guest reader"))?;
        let accepted = match reader
            .guest_head()
            .await
            .map_err(|e| reader_error(reader, &e))?
        {
            HeadReadResult::Ok { response } => response
                .accepted
                .ok_or(RestoreError::Source("share has no published backup"))?,
            HeadReadResult::Error { error } => return Err(reader_error(reader, &error.into())),
        };
        (&accepted.job().stream == stream)
            .then_some(accepted)
            .ok_or(RestoreError::Invalid("guest stream"))
    }

    pub(crate) fn begin_guest_at(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        Self::begin_in(destination, accepted, RestoreMode::Recipient, true)
    }

    pub(crate) fn begin_managed_guest(slot: &Path, accepted: AcceptedBackup) -> Result<Self> {
        let invalid = || RestoreError::Invalid("managed guest slot");
        let entry = slot.parent().ok_or_else(invalid)?;
        let registry = entry.parent().ok_or_else(invalid)?;
        fn name_of(p: &Path) -> Option<&str> {
            p.file_name().and_then(std::ffi::OsStr::to_str)
        }
        if !matches!(name_of(slot), Some("snapshot-0" | "snapshot-1"))
            || name_of(entry).is_none_or(|n| Id::try_from(n.to_owned()).is_err())
            || name_of(registry) != Some(super::MANAGED_SHARE_DIRECTORY)
            || slot.canonicalize()? != slot
        {
            return Err(invalid());
        }
        for dir in [registry, entry, slot] {
            local::directory(dir)?;
        }
        Self::begin_in(
            &slot.join("unactivated"),
            accepted,
            RestoreMode::Recipient,
            false,
        )
    }

    pub(crate) async fn stage_guest_recovery(
        &mut self,
        reader: &ByteBackupReader,
        content_key: Zeroizing<[u8; 32]>,
        publisher_public_key: Digest,
        cache: Option<&GuestReplayCache<'_>>,
    ) -> Result<(PathBuf, NativeRoot, Option<LogPosition>)> {
        let accepted = self.accepted();
        if self.selection.mode != RestoreMode::Recipient
            || reader.guest_stream() != Some(&accepted.job().stream)
        {
            return Err(RestoreError::Invalid("recipient restore session"));
        }
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key: publisher_public_key,
        };
        recovery.validate_for(&accepted.job().stream.scope)?;
        let dec = RecoveryDecryption {
            content: content_key,
            account_public_key: recovery.account_public_key.as_str().to_owned(),
        };
        let _admission = acquire_native_work()?;
        self.require_unactivated()?;
        self.authorize_read(reader).await?;
        let (inventory, bytes) = load_closure(self, reader).await?;
        let (proof, through) = if self.accepted().begin.dependencies().is_empty() {
            let root = snapshot_closure(self, &dec, &bytes)?;
            (
                runs::stage_and_verify(self, reader, &dec, &root, inventory).await?,
                None,
            )
        } else {
            let root = MutationLogRoot::decode(&bytes, &dec.content, &self.accepted().begin)
                .map_err(|_| RestoreError::Invalid("recipient mutation log root"))?;
            drop(inventory);
            let (p, t) = logs::stage_guest(self, reader, &dec, root, cache).await?;
            (p, Some(t))
        };
        self.require_unactivated()?;
        Ok((
            self.root.join("home/data"),
            proof.native_root().map_err(RestoreError::native_proof)?,
            through,
        ))
    }

    fn require_unactivated(&self) -> Result<()> {
        let home = self.root.join("home");
        for path in [
            self.selection.destination.clone(),
            home.join(crate::identity::IDENTITY_KEY_FILE),
            crate::cloud::cloud_credentials_path(&home),
        ] {
            match fs::symlink_metadata(path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
                Ok(_) => return Err(RestoreError::Invalid("recipient stage already activated")),
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: RestoreReport,
}

pub fn verify_restore_receipt(bytes: &[u8], destination: &Path) -> Result<()> {
    let r: Receipt =
        serde_json::from_slice(bytes).map_err(|_| RestoreError::Invalid("restore receipt"))?;
    if r.version != RESTORE_RECEIPT_VERSION
        || r.session_id.is_nil()
        || r.report.destination != destination
        || r.report.native_files == 0
        || r.report.native_bytes == 0
        || Digest::try_from(r.report.commit_sha256).is_err()
    {
        return Err(RestoreError::Invalid("restore provenance"));
    }
    Ok(())
}

pub(super) fn published(session: &RestoreSession) -> Result<Option<RestoreReport>> {
    let dest = &session.selection.destination;
    match fs::symlink_metadata(dest) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
        Ok(m) if !m.is_dir() => return Err(RestoreError::Invalid("published destination")),
        Ok(_) => {}
    }
    let r: Receipt = local::read_json(&dest.join(RESTORE_RECEIPT_NAME), 64 * 1024)?;
    if r.version != RESTORE_RECEIPT_VERSION
        || r.session_id != session.selection.session_id
        || r.report.commit_sha256 != session.accepted().commit_sha256.as_str()
        || r.report.destination != *dest
    {
        return Err(RestoreError::Invalid("published receipt"));
    }
    record_published(session, dest, r.report).map(Some)
}

pub(super) fn finish_run(
    session: &RestoreSession,
    native_root: &NativeRoot,
    identity_seed: &[u8; 32],
    proof: &RunRestoreGuard,
) -> Result<RestoreReport> {
    let home = session.root.join("home");
    proof
        .validate_context(&home.join("data"), native_root)
        .map_err(|_| RestoreError::Invalid("verified run publication"))?;
    let (native_files, native_bytes) = proof.totals().map_err(RestoreError::native_proof)?;
    install_identity(&home, identity_seed)?;
    if crate::cloud::cloud_credentials_path(&home).try_exists()? {
        return Err(RestoreError::Invalid("restore cloud configuration"));
    }
    let dest = &session.selection.destination;
    let report = RestoreReport {
        commit_sha256: session.accepted().commit_sha256.as_str().into(),
        native_files,
        native_bytes,
        verified_records: 0,
        destination: dest.clone(),
    };
    let receipt = Receipt {
        version: RESTORE_RECEIPT_VERSION,
        session_id: session.selection.session_id,
        report: report.clone(),
    };
    local::atomic_json(&home.join(RESTORE_RECEIPT_NAME), &receipt)?;
    sync_owned_tree(&home, 0)?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        &home,
        rustix::fs::CWD,
        dest,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)?;
    record_published(session, dest, report)
}

fn record_published(
    session: &RestoreSession,
    destination: &Path,
    report: RestoreReport,
) -> Result<RestoreReport> {
    local::sync_dir(&session.root)?;
    local::sync_dir(
        destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?,
    )?;
    local::atomic_json(&session.root.join("result.json"), &report).map(|()| report)
}

fn install_identity(home: &Path, identity_seed: &[u8; 32]) -> Result<()> {
    let path = home.join(crate::identity::IDENTITY_KEY_FILE);
    if path.try_exists()? {
        let mut file = local::owner_file(&path, false)?;
        let mut seed = Zeroizing::new(Vec::new());
        Read::by_ref(&mut file).take(33).read_to_end(&mut seed)?;
        if seed.len() > 32 || !identity_seed.starts_with(&seed) {
            return Err(RestoreError::Invalid("restore identity"));
        }
        if seed.len() < 32 {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(identity_seed)?;
        }
    } else {
        crate::identity::write_seed(home, identity_seed)
            .map_err(|_| RestoreError::Invalid("restore identity flush"))?;
    }
    std::fs::File::open(&path)?.sync_all()?;
    local::sync_dir(home)
}

fn sync_owned_tree(root: &Path, depth: usize) -> Result<()> {
    if depth > 12 {
        return Err(RestoreError::Limit("destination depth"));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            sync_owned_tree(&entry.path(), depth + 1)?;
        } else if !kind.is_file() {
            return Err(RestoreError::Invalid("destination member type"));
        }
    }
    local::sync_dir(root)
}

//! Verified recipient staging of accepted opaque-byte cloud backups.
mod logs;
mod runs;

use crate::cloud::is_private_file;
use crate::cloud_backup_client::byte_backup::{BackupTransportError, ByteBackupReader};
use cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES;
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::access::{
    ReadGrant, ReadGrantItemResult, ReadGrantRequest, ReadSelector,
};
use cloud_backup_protocol::v2::publication::{
    CommitResponse, CommitStatusRequest, Head, HeadReadResult,
};
use cloud_backup_protocol::v2::{AcceptedBackup, Version};
use cloud_backup_protocol::{Digest, ErrorCode, Id, NativeRoot, ProtocolError, RecoveryDescriptor};
pub use fold_db::storage::disk_pack::run_restore::{StagedRecord, StagedRecordQuery};
use fold_db::storage::disk_pack::{
    mutation_log::LogPosition, run_restore, NativeCloudWorkGuard, RUN_DIR,
};
use fold_db::storage::error::{NativeProofCause, NativeProofStage};
use sha2::{Digest as _, Sha256};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub(crate) const MANAGED_SHARE_DIRECTORY: &str = "received-shares-v1";
const MAX_SPOOL: u64 = DURABLE_LOCAL_SPOOL_MAX_BYTES
    - fold_db::storage_policy::backup_decode::RESTORE_SPOOL_RESERVE_BYTES;
use RestoreError as E;

#[derive(Debug)]
pub enum RestoreError {
    Invalid(&'static str),
    Limit(&'static str),
    Io(io::Error),
    Protocol(ProtocolError),
    Source(&'static str),
    NativeProof {
        stage: NativeProofStage,
        cause: NativeProofCause,
    },
}

pub type Result<T> = std::result::Result<T, RestoreError>;

impl From<io::Error> for RestoreError {
    fn from(v: io::Error) -> Self {
        Self::Io(v)
    }
}
impl From<ProtocolError> for RestoreError {
    fn from(v: ProtocolError) -> Self {
        Self::Protocol(v)
    }
}
impl RestoreError {
    pub(crate) fn native_proof(error: fold_db::storage::StorageError) -> Self {
        let (stage, cause) = error.native_proof_context(NativeProofStage::Restore);
        drop(error);
        Self::NativeProof { stage, cause }
    }
}
impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => write!(f, "invalid restore {s}"),
            Self::Limit(s) => write!(f, "restore limit: {s}"),
            Self::Io(_) => f.write_str("restore disk operation failed"),
            Self::Protocol(_) => f.write_str("restore protocol validation failed"),
            Self::Source(s) => write!(f, "restore source unavailable: {s}"),
            Self::NativeProof { stage, cause } => {
                write!(f, "native restore proof {stage}: {cause}")
            }
        }
    }
}
impl std::error::Error for RestoreError {}

pub(super) struct GuestSecrets {
    pub(super) recovery: RecoveryDescriptor,
    pub(super) key: Zeroizing<[u8; 32]>,
}

pub(crate) struct GuestReplayCache<'a> {
    pub(crate) data: &'a Path,
    pub(crate) head: &'a Head,
    pub(crate) native_root: &'a NativeRoot,
    pub(crate) through: Option<&'a LogPosition>,
}

pub struct RestoreSession {
    pub(super) root: PathBuf,
    pub(super) accepted: AcceptedBackup,
    _lock: File,
}

impl RestoreSession {
    fn begin_in(dest: &Path, accepted: AcceptedBackup, check_primary: bool) -> Result<Self> {
        if check_primary {
            crate::offline_home::refuse_primary(dest).map_err(|_| E::Invalid("destination"))?;
        }
        accepted.validate()?;
        let parent = fs::canonicalize(dest.parent().ok_or(E::Invalid("destination parent"))?)?;
        let name = dest.file_name().ok_or(E::Invalid("destination name"))?;
        match fs::symlink_metadata(parent.join(name)) {
            Ok(_) => return Err(E::Invalid("destination exists")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let root = parent.join(format!(".byte-cloud-restore-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let _lock = open_private(&root.join("owner.lock"), true, false)?;
        _lock
            .try_lock()
            .map_err(|_| E::Invalid("session already owned"))?;
        ensure_directory(&root.join("data"))?;
        sync_dir(&root).and_then(|()| sync_dir(&parent))?;
        Ok(Self {
            root,
            accepted,
            _lock,
        })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub(super) fn ancestor(&self, accepted: AcceptedBackup) -> Result<Self> {
        accepted.validate()?;
        if accepted.job().stream != self.accepted.job().stream
            || accepted.ordinal.get() > self.accepted.ordinal.get()
        {
            return Err(E::Invalid("accepted ancestor stream or order"));
        }
        Ok(Self {
            root: self.root.clone(),
            accepted,
            _lock: self._lock.try_clone()?,
        })
    }

    pub(crate) async fn guest_accepted(reader: &ByteBackupReader) -> Result<AcceptedBackup> {
        let accepted = match reader
            .guest_head()
            .await
            .map_err(|e| reader_error(&e, false))?
        {
            HeadReadResult::Ok { response } => response
                .accepted
                .ok_or(E::Source("share has no published backup"))?,
            HeadReadResult::Error { error } => return Err(reader_error(&error.into(), false)),
        };
        (&accepted.job().stream == reader.guest_stream())
            .then_some(accepted)
            .ok_or(E::Invalid("guest stream"))
    }

    pub(crate) fn begin_guest_at(dest: &Path, accepted: AcceptedBackup) -> Result<Self> {
        Self::begin_in(dest, accepted, true)
    }

    pub(crate) fn begin_managed_guest(slot: &Path, accepted: AcceptedBackup) -> Result<Self> {
        fn name_of(p: &Path) -> Option<&str> {
            p.file_name().and_then(std::ffi::OsStr::to_str)
        }
        let entry = slot.parent().ok_or(E::Invalid("managed guest slot"))?;
        let registry = entry.parent().ok_or(E::Invalid("managed guest slot"))?;
        if !matches!(name_of(slot), Some("snapshot-0" | "snapshot-1"))
            || name_of(entry).is_none_or(|n| Id::try_from(n.to_owned()).is_err())
            || name_of(registry) != Some(MANAGED_SHARE_DIRECTORY)
            || slot.canonicalize()? != slot
        {
            return Err(E::Invalid("managed guest slot"));
        }
        for d in [registry, entry, slot] {
            directory(d)?;
        }
        Self::begin_in(&slot.join("unactivated"), accepted, false)
    }

    pub(crate) async fn stage_guest_recovery(
        &mut self,
        reader: &ByteBackupReader,
        key: Zeroizing<[u8; 32]>,
        account_public_key: Digest,
        cache: Option<&GuestReplayCache<'_>>,
    ) -> Result<(PathBuf, NativeRoot, Option<LogPosition>)> {
        if reader.guest_stream() != &self.accepted.job().stream {
            return Err(E::Invalid("recipient restore session"));
        }
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key,
        };
        recovery.validate_for(&self.accepted.job().stream.scope)?;
        let secrets = GuestSecrets { recovery, key };
        let _guard = NativeCloudWorkGuard::acquire()
            .map_err(|_| E::Limit("active native cloud operation"))?;
        let req = CommitStatusRequest {
            version: Version::V2,
            job: self.accepted.job().clone(),
        };
        let status = reader
            .commit_status(&req)
            .await
            .map_err(|e| reader_error(&e, false))?;
        if !matches!(status, CommitResponse::Accepted { backup } if *backup == self.accepted) {
            return Err(E::Invalid("authoritative accepted selection"));
        }
        let (mut inv, bytes) = runs::load_closure(self, reader).await?;
        let (proof, through) = if self.accepted.begin.dependencies().is_empty() {
            let r = runs::snapshot_closure(self, &secrets, &bytes)?;
            (
                runs::stage_and_verify(self, reader, &secrets, &r, &mut inv).await?,
                None,
            )
        } else {
            let r = logs::MutationLogRoot::decode(&bytes, &secrets.key, &self.accepted.begin)?;
            let (p, t) = logs::stage_guest(self, reader, &secrets, r, cache).await?;
            (p, Some(t))
        };
        let root = proof.native_root().map_err(E::native_proof)?;
        Ok((self.root.join("data"), root, through))
    }
}

pub(super) async fn grants(
    session: &RestoreSession,
    client: &ByteBackupReader,
    objects: Vec<ReadSelector>,
) -> Result<Vec<ReadGrant>> {
    let req = ReadGrantRequest {
        version: Version::V2,
        job: session.accepted.job().clone(),
        objects,
    };
    req.validate()?;
    let resp = client
        .read_grants(&req, &session.accepted)
        .await
        .map_err(|e| reader_error(&e, false))?;
    resp.validate_for(&req, &session.accepted)?;
    resp.items
        .into_iter()
        .map(|i| match i {
            ReadGrantItemResult::Ok { grant, .. } => Ok(grant),
            ReadGrantItemResult::Error { .. } => Err(E::Source("object grant")),
        })
        .collect()
}

pub(super) async fn fetch_bytes(
    client: &ByteBackupReader,
    grant: &ReadGrant,
    max: u64,
) -> Result<Vec<u8>> {
    let len = grant.bytes.get();
    if len > max {
        return Err(E::Limit("object decode"));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    client
        .download_to(grant, &mut bytes)
        .await
        .map_err(|e| reader_error(&e, true))?;
    Ok(bytes)
}

pub(super) async fn fetch_object(
    session: &RestoreSession,
    client: &ByteBackupReader,
    grant: &ReadGrant,
) -> Result<PathBuf> {
    let len = grant.bytes.get();
    if len > MAX_SPOOL {
        return Err(E::Limit("spool bytes"));
    }
    check_capacity(&session.root, len)?;
    let path = session.root.join("payload.object");
    let mut file = open_private(&path, false, true)?;
    client
        .download_to(grant, &mut file)
        .await
        .map_err(|e| reader_error(&e, true))?;
    file.sync_all()?;
    sync_dir(&session.root).map(|()| path)
}

pub(super) fn verify_file(path: &Path, sha: &Digest, len: u64) -> Result<()> {
    let (m, mut h) = (fs::symlink_metadata(path)?, Sha256::new());
    if !m.is_file()
        || m.len() != len
        || io::copy(&mut File::open(path)?.take(len + 1), &mut h)? != len
    {
        return Err(E::Invalid("file length"));
    }
    (fold_db::hex::hex_lower(h.finalize()) == sha.as_str())
        .then_some(())
        .ok_or(E::Invalid("file digest"))
}

pub(super) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    let vfs = rustix::fs::statvfs(root).map_err(io::Error::from)?;
    let avail = (vfs.f_bavail as u64).checked_mul(vfs.f_frsize as u64);
    let req = bytes.checked_add(laststore::space::SpacePolicy::default().free_space_reserve_bytes);
    avail
        .zip(req)
        .is_some_and(|(a, r)| a >= r)
        .then_some(())
        .ok_or(E::Limit("disk capacity"))
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

pub(super) fn directory(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path)?;
    (m.is_dir() && m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o077 == 0)
        .then_some(())
        .ok_or(E::Invalid("owned session directory"))
}

pub(super) fn open_private(path: &Path, create_new: bool, truncate: bool) -> Result<File> {
    let mut opt = OpenOptions::new();
    opt.read(!truncate)
        .write(true)
        .create(true)
        .create_new(create_new);
    let f = opt.mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path)?;
    if !is_private_file(&f.metadata()?) {
        return Err(E::Invalid("owned session file"));
    }
    if truncate {
        f.set_len(0)?;
    }
    Ok(f)
}

pub(super) fn ensure_directory(path: &Path) -> Result<()> {
    if let Err(e) = fs::DirBuilder::new().mode(0o700).create(path) {
        if e.kind() != io::ErrorKind::AlreadyExists {
            return Err(e.into());
        }
    }
    directory(path)
}

pub(super) fn prepare_native_ephemera(data: &Path, key: &[u8; 32]) -> Result<()> {
    let home = data.join(RUN_DIR);
    directory(&home)?;
    ensure_directory(&home.join(laststore::disk_run::home::RUNS))?;
    let lock = open_private(&home.join(laststore::disk_run::home::LOCK), false, false)?;
    if lock.metadata()?.len() != 0 {
        return Err(E::Invalid("run ephemeral lock"));
    }
    lock.sync_all()?;
    sync_dir(&home)?;
    run_restore::install_empty_cas_home(data, key).map_err(E::native_proof)
}

fn reader_error(err: &BackupTransportError, digest: bool) -> RestoreError {
    use BackupTransportError::{Authentication, Integrity, Protocol};
    match err {
        Integrity(_) if digest => E::Invalid("object digest"),
        Authentication
        | Protocol(ProtocolError {
            code: ErrorCode::Unauthorized,
            ..
        }) => E::Source("guest permission refused"),
        Protocol(e)
            if (e.code, e.field.as_str()) == (ErrorCode::RetryLater, "guest.head_changed") =>
        {
            E::Source("share changed, start a new guest stage")
        }
        _ => E::Source("byte transport"),
    }
}

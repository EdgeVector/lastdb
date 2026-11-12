use super::{
    log_capture::LogCapture,
    store::Store,
    work::{Capture, Work},
    BackupError, Phase, Result,
};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::PackRuntime;
use fold_db::storage::StorageError;
use model::{LocalAction, Saved};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

pub mod model {
    use crate::backup_runtime::BackupError;
    pub use crate::backup_runtime::Phase;
    use crate::byte_cloud_backup::{
        work::Work, AutomaticBackupStatus, ManualConnectionStatus, Result,
    };
    use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Commit};
    use cloud_backup_protocol::{AccountId, Id};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
    pub enum ByteBackupRequest {
        Start {
            request_id: Id,
        },
        Resume,
        Status,
        Close,
        EnableAutomatic {
            log_interval_seconds: u64,
            snapshot_interval_seconds: u64,
        },
        DisableAutomatic,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(in crate::byte_cloud_backup) enum LocalAction {
        ReplaceWithSnapshot,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(in crate::byte_cloud_backup) struct Saved {
        pub format: u32,
        pub request_id: Id,
        #[serde(default, skip_serializing_if = "Work::is_default")]
        pub work: Work,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub automatic_revision: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub local_action: Option<LocalAction>,
        pub account: AccountId,
        pub source_home: Id,
        pub endpoint: String,
        pub begin: Option<Begin>,
        pub begin_submitted: bool,
        pub commit: Option<Commit>,
        pub accepted: Option<AcceptedBackup>,
        pub phase: Phase,
        pub objects_verified: u64,
        pub metadata_pack_objects: u64,
        pub multi_file_pack_objects: u64,
        pub native_files: u64,
        pub native_bytes: u64,
        pub retained_source_bytes: Option<u64>,
        pub blocked_reason: Option<String>,
    }

    impl Saved {
        pub fn validate(&self) -> Result<()> {
            self.work.validate(self.begin.as_ref())?;
            let bad_replace = self.local_action.is_some()
                && (self.begin_submitted
                    || self.commit.is_some()
                    || self.accepted.is_some()
                    || self.phase == Phase::Published
                    || self.automatic_revision.is_none()
                    || !matches!(self.work, Work::MutationLog { .. }));
            if bad_replace
                || self.format != 2
                || self.account.as_str().len() != 32
                || self.endpoint.len() > 4096
                || (self.begin_submitted && self.begin.is_none())
                || (self.commit.is_some() && !self.begin_submitted)
                || self.multi_file_pack_objects > self.metadata_pack_objects
                || (self.phase == Phase::Published && self.accepted.is_none())
            {
                return Err(BackupError::Invalid);
            }
            if let Some(begin) = &self.begin {
                begin.validate()?;
                if begin.job.commit_id != self.request_id
                    || begin.job.stream.scope.account_id != self.account
                {
                    return Err(BackupError::Identity);
                }
            }
            if let Some(commit) = &self.commit {
                commit.validate_for(self.begin.as_ref().ok_or(BackupError::Invalid)?)?;
            }
            if let Some(accepted) = &self.accepted {
                accepted.validate_for(
                    self.begin.as_ref().ok_or(BackupError::Invalid)?,
                    self.commit.as_ref().ok_or(BackupError::Invalid)?,
                )?;
            }
            Ok(())
        }

        pub fn terminal(&self) -> bool {
            matches!(self.phase, Phase::Published | Phase::Closed)
                && self.retained_source_bytes == Some(0)
        }
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct ByteBackupStatus {
        pub backup_kind: &'static str,
        pub enabled: bool,
        pub automatic_backup_enabled: bool,
        pub automatic_backup: Option<AutomaticBackupStatus>,
        pub mutation_log: fold_db::storage::disk_pack::mutation_log::MutationLogStatus,
        pub manual_connection: Option<ManualConnectionStatus>,
        pub running: bool,
        pub request_id: Option<Id>,
        pub phase: Option<Phase>,
        pub objects_verified: u64,
        pub metadata_pack_objects: u64,
        pub multi_file_pack_objects: u64,
        pub native_files: u64,
        pub native_bytes: u64,
        pub retained_source_bytes: Option<u64>,
        pub blocked_reason: Option<String>,
        pub last_accepted_backup: Option<AcceptedBackup>,
    }
}

#[derive(Clone)]
pub(super) struct Source {
    pub runtime: Arc<PackRuntime>,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
    pub publication_stream: Option<Id>,
    publication_home: Option<PathBuf>,
}

pub(crate) struct PublicationSource {
    pub(crate) id: Id,
    pub(crate) publisher: Digest,
    pub(crate) account: AccountId,
    pub(crate) content_key: Zeroizing<[u8; 32]>,
}

impl Source {
    fn new(
        runtime: Arc<PackRuntime>,
        account: AccountId,
        account_public_key: Digest,
        key: Zeroizing<[u8; 32]>,
        publication_stream: Option<Id>,
        publication_home: Option<PathBuf>,
    ) -> Result<Self> {
        Ok(Self {
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            runtime,
            account,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key,
            },
            key,
            publication_stream,
            publication_home,
        })
    }

    pub fn from_host(host: &crate::host::Host) -> Result<Self> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
        let pub_hex = fold_db::hex::hex_lower(host.keypair.public_key_bytes());
        Self::new(
            host.db.db_ops().native_runtime(),
            AccountId::try_from(host.user_hash.clone())?,
            Digest::try_from(pub_hex)?,
            Zeroizing::new(keys.encryption_key()),
            None,
            None,
        )
    }

    pub fn from_publication(
        home: &Path,
        runtime: Arc<PackRuntime>,
        id: PublicationSource,
    ) -> Result<Self> {
        Self::new(
            runtime,
            id.account,
            id.publisher,
            id.content_key,
            Some(id.id),
            Some(home.to_owned()),
        )
    }

    pub fn saved(&self, request_id: Id, endpoint: String) -> Saved {
        Saved {
            format: 2,
            request_id,
            work: Work::default(),
            automatic_revision: None,
            local_action: None,
            endpoint,
            account: self.account.clone(),
            source_home: self.source_home.clone(),
            begin: None,
            begin_submitted: false,
            commit: None,
            accepted: None,
            phase: Phase::Preparing,
            objects_verified: 0,
            metadata_pack_objects: 0,
            multi_file_pack_objects: 0,
            native_files: 0,
            native_bytes: 0,
            retained_source_bytes: None,
            blocked_reason: None,
        }
    }

    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account || saved.source_home != self.source_home {
            return Err(BackupError::Identity);
        }
        if let Some(begin) = &saved.begin {
            let bad_pub = self.publication_stream.as_ref().is_some_and(|id| {
                &begin.job.stream.stream_id != id
                    || begin.r2_multipart.is_some()
                    || begin.recovery_dependencies != saved.work.recovery_dependencies()
            });
            if begin.required_destinations != [Destination::R2, Destination::B2] || bad_pub {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }

    pub async fn prepare(&self, saved: &Saved, store: &Store) -> Result<Capture> {
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        saved.work.validate(Some(begin))?;
        let pub_home = self.publication_home.as_deref();
        match &saved.work {
            Work::Snapshot { minimum } => {
                let gate = pub_home.map(|home| move || capture_gate(home));
                let capture = self
                    .runtime
                    .prepare_byte_backup_capture(
                        begin.clone(),
                        &self.key,
                        self.recovery.clone(),
                        !saved.begin_submitted,
                        gate.as_ref().map(|g| {
                            g as &(dyn Fn() -> fold_db::storage::error::StorageResult<std::fs::File>
                                  + Sync)
                        }),
                    )
                    .await?;
                if let Some(m) = minimum {
                    if capture.run_profile()?.main.frontier.get() < m.main {
                        return Err(BackupError::Identity);
                    }
                }
                Ok(Capture::Snapshot(capture))
            }
            Work::MutationLog { context } => {
                let core = if let Some(home) = pub_home {
                    self.runtime
                        .prepare_publication_mutation_log_capture(begin, &context.from, || {
                            capture_gate(home)
                        })
                        .await?
                } else {
                    self.runtime
                        .prepare_mutation_log_capture(begin, &context.from)?
                };
                self.log_capture(core, store, context)
            }
        }
    }

    pub async fn resume(&self, saved: &Saved, store: &Store) -> Result<Capture> {
        match &saved.work {
            Work::Snapshot { .. } => Ok(Capture::Snapshot(
                self.runtime
                    .resume_byte_backup_capture(&self.key, self.publication_stream.is_none())
                    .await?,
            )),
            Work::MutationLog { context } => {
                let core = self
                    .runtime
                    .resume_mutation_log_capture(&saved.request_id)?;
                self.log_capture(core, store, context)
            }
        }
    }

    fn log_capture(
        &self,
        core: fold_db::storage::disk_pack::mutation_log::MutationLogCapture,
        store: &Store,
        context: &super::work::LogContext,
    ) -> Result<Capture> {
        LogCapture::prepare(core, store, context, &self.recovery, &self.key)
            .map(|c| Capture::Log(Box::new(c)))
    }

    pub async fn cancel(&self, saved: &Saved) -> Result<()> {
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        match &saved.work {
            Work::Snapshot { .. } => {
                self.runtime
                    .cancel_unsubmitted_byte_backup_capture(begin, &self.key)
                    .await?;
            }
            Work::MutationLog { .. } => {
                self.runtime
                    .cancel_unsubmitted_mutation_log_capture(begin)?;
            }
        }
        Ok(())
    }

    pub fn finish_replacement(&self, store: &Store, saved: &mut Saved) -> Result<()> {
        self.validate(saved)?;
        if saved.local_action != Some(LocalAction::ReplaceWithSnapshot) {
            return Err(BackupError::Invalid);
        }
        self.runtime
            .cancel_unmaterialized_mutation_log_capture(&saved.request_id)?;
        (
            saved.phase,
            saved.retained_source_bytes,
            saved.blocked_reason,
        ) = (Phase::Closed, Some(0), None);
        store.save(saved)
    }

    pub fn replace_with_snapshot(&self, store: &Store, saved: &mut Saved) -> Result<()> {
        saved.local_action = Some(LocalAction::ReplaceWithSnapshot);
        store.save(saved)?;
        store.select(&saved.request_id)?;
        self.finish_replacement(store, saved)
    }
}

fn capture_gate(home: &Path) -> fold_db::storage::error::StorageResult<std::fs::File> {
    super::share_write_gate::try_capture(home)
        .map_err(|_| StorageError::InvalidOperation("cannot open the share edit gate".into()))?
        .ok_or(StorageError::PublicationEditPending)
}

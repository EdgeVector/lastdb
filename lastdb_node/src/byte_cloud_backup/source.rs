use super::{
    log_capture::LogCapture,
    store::Store,
    work::{Capture, Work},
    BackupError, Phase, Result,
};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Commit, Destination};
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::storage::disk_pack::byte_backup::ByteBackupCaptureStatus;
use fold_db::storage::disk_pack::PackRuntime;
use fold_db::storage::StorageError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LocalAction {
    ReplaceWithSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Saved {
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
    pub fn apply_status(&mut self, st: &ByteBackupCaptureStatus) {
        self.objects_verified = st.verified_objects;
        self.metadata_pack_objects = st.metadata_pack_objects;
        self.multi_file_pack_objects = st.multi_file_pack_objects;
        self.native_files = st.native_files;
        self.native_bytes = st.native_bytes;
        self.retained_source_bytes = Some(st.retained_source_bytes);
    }

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
            let begin = self.begin.as_ref().ok_or(BackupError::Invalid)?;
            accepted.validate_for(begin, self.commit.as_ref().ok_or(BackupError::Invalid)?)?;
        }
        Ok(())
    }

    pub fn terminal(&self) -> bool {
        matches!(self.phase, Phase::Published | Phase::Closed)
            && self.retained_source_bytes == Some(0)
    }
}

#[derive(Clone)]
pub(super) struct Source {
    pub runtime: Arc<PackRuntime>,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
    pub publication_stream: Id,
    publication_home: PathBuf,
}

pub(crate) struct PublicationSource {
    pub(crate) id: Id,
    pub(crate) publisher: Digest,
    pub(crate) account: AccountId,
    pub(crate) content_key: Zeroizing<[u8; 32]>,
}

impl Source {
    pub fn from_publication(
        home: &Path,
        runtime: Arc<PackRuntime>,
        id: PublicationSource,
    ) -> Result<Self> {
        Ok(Self {
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            runtime,
            account: id.account,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: id.publisher,
            },
            key: id.content_key,
            publication_stream: id.id,
            publication_home: home.into(),
        })
    }

    pub fn init_or_validate(
        &self,
        existing: Option<Saved>,
        request_id: Id,
        endpoint: &str,
        auto: Option<(u64, &Work)>,
    ) -> Result<Saved> {
        let has_existing = existing.is_some();
        let mut saved = existing.unwrap_or_else(|| Saved {
            format: 2,
            request_id,
            work: Work::default(),
            automatic_revision: None,
            local_action: None,
            endpoint: endpoint.to_owned(),
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
        });
        if let Some((rev, work)) = auto {
            if saved
                .automatic_revision
                .is_some_and(|r| r != rev || saved.work != *work)
                || (saved.automatic_revision.is_none() && has_existing)
            {
                return Err(BackupError::Identity);
            }
            saved.automatic_revision = Some(rev);
            saved.work = work.clone();
        }
        self.validate(&saved)?;
        (saved.endpoint == endpoint)
            .then_some(saved)
            .ok_or(BackupError::Identity)
    }

    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account || saved.source_home != self.source_home {
            return Err(BackupError::Identity);
        }
        if let Some(begin) = &saved.begin {
            if begin.required_destinations != [Destination::R2, Destination::B2]
                || begin.job.stream.stream_id != self.publication_stream
                || begin.r2_multipart.is_some()
                || begin.recovery_dependencies != saved.work.recovery_dependencies()
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }

    pub async fn prepare(&self, saved: &Saved, store: &Store) -> Result<Capture> {
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        saved.work.validate(Some(begin))?;
        let home = self.publication_home.as_path();
        match &saved.work {
            Work::Snapshot { minimum } => {
                let gate = || capture_gate(home);
                let cap = self
                    .runtime
                    .prepare_byte_backup_capture(
                        begin.clone(),
                        &self.key,
                        self.recovery.clone(),
                        !saved.begin_submitted,
                        Some(&gate),
                    )
                    .await?;
                if let Some(m) = minimum {
                    if cap.run_profile()?.main.frontier.get() < m.main {
                        return Err(BackupError::Identity);
                    }
                }
                Ok(Capture::Snapshot(cap))
            }
            Work::MutationLog { context } => {
                let core = self
                    .runtime
                    .prepare_publication_mutation_log_capture(begin, &context.from, || {
                        capture_gate(home)
                    })
                    .await?;
                LogCapture::prepare(core, store, context, &self.recovery, &self.key)
                    .map(|c| Capture::Log(Box::new(c)))
            }
        }
    }

    pub async fn resume(&self, saved: &Saved, store: &Store) -> Result<Capture> {
        match &saved.work {
            Work::Snapshot { .. } => Ok(Capture::Snapshot(
                self.runtime
                    .resume_byte_backup_capture(&self.key, false)
                    .await?,
            )),
            Work::MutationLog { context } => {
                let core = self
                    .runtime
                    .resume_mutation_log_capture(&saved.request_id)?;
                LogCapture::prepare(core, store, context, &self.recovery, &self.key)
                    .map(|c| Capture::Log(Box::new(c)))
            }
        }
    }

    pub async fn cancel(&self, saved: &Saved) -> Result<()> {
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        match &saved.work {
            Work::Snapshot { .. } => {
                self.runtime
                    .cancel_unsubmitted_byte_backup_capture(begin, &self.key)
                    .await?;
            }
            Work::MutationLog { .. } => self
                .runtime
                .cancel_unsubmitted_mutation_log_capture(begin)?,
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
        saved.phase = Phase::Closed;
        saved.retained_source_bytes = Some(0);
        saved.blocked_reason = None;
        store.save(saved)
    }

    pub fn replace_unmaterialized(
        &self,
        store: &Store,
        saved: &mut Saved,
        limit: Option<u8>,
    ) -> Result<bool> {
        let Work::MutationLog { context } = &saved.work else {
            return Ok(false);
        };
        if saved.begin_submitted {
            return Ok(false);
        }
        let replacing = saved.local_action.is_some();
        if !replacing && limit.is_some() {
            let j = self.runtime.mutation_log_status()?;
            if !j.snapshot_required && j.anchor.as_ref() == Some(&context.anchor) {
                return Ok(false);
            }
        }
        if self
            .runtime
            .mutation_log_capture_materialized(&saved.request_id)?
        {
            return match (replacing, limit.is_some()) {
                (true, _) => Err(BackupError::Identity),
                (false, true) => Ok(false),
                (false, false) => Err(BackupError::AdmissionPending),
            };
        }
        saved.local_action = Some(LocalAction::ReplaceWithSnapshot);
        store.save(saved)?;
        store.select(&saved.request_id)?;
        self.finish_replacement(store, saved).map(|()| true)
    }
}

fn capture_gate(home: &Path) -> fold_db::storage::error::StorageResult<std::fs::File> {
    super::share_write_gate::try_capture(home)
        .map_err(|_| StorageError::InvalidOperation("cannot open the share edit gate".into()))?
        .ok_or(StorageError::PublicationEditPending)
}

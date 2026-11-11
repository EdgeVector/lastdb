//! Explicit V2 byte backup. The native format remains a client responsibility.
mod automatic;
pub(crate) mod automatic_publication;
mod blob_source;
mod blobs;
pub(crate) mod connection;
mod control;
mod log_capture;
mod publication;
pub mod share_write_gate;
mod source;
mod store;
mod transfer;
mod work;
mod worker;

mod capture {
    pub(in crate::byte_cloud_backup) use crate::byte_cloud_backup::work::Capture;
}

mod retry {
    pub(in crate::byte_cloud_backup) use crate::byte_cloud_backup::work::{control, temporary};
}

mod wait {
    pub(in crate::byte_cloud_backup) use crate::byte_cloud_backup::work::{
        poll, record_error, Context,
    };
}

pub(crate) mod credentials {
    use crate::cloud_backup_client::byte_backup::ByteBackupClient;
    use std::path::Path;

    /// Read saved recovery credentials without enrolling or enabling backup.
    pub fn client_from_home(home: &Path) -> Result<ByteBackupClient, String> {
        client_for_recovery(home)
    }

    pub(in crate::byte_cloud_backup) fn client_for_recovery(
        home: &Path,
    ) -> Result<ByteBackupClient, String> {
        let auth = crate::cloud::auth_for_recovery(home)?;
        ByteBackupClient::from_auth(auth).map_err(|error| error.to_string())
    }
}

pub(crate) mod invocation {
    use crate::backup_runtime::{BackupError, InvocationOutcome};
    use crate::byte_cloud_backup::{
        model::Saved, source::Source, store::Store, worker::run as run_worker,
    };
    use crate::cloud_backup_client::byte_backup::ByteBackupClient;
    use std::fs::File;
    use std::sync::{atomic::AtomicBool, Arc};

    pub(in crate::byte_cloud_backup) fn run(
        source: Source,
        store: Store,
        mut saved: Saved,
        client: ByteBackupClient,
        permit: File,
        pause: Arc<AtomicBool>,
    ) -> InvocationOutcome {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| BackupError::Source)?;
            runtime.block_on(run_worker(&source, &store, &mut saved, &client, &pause))
        }))
        .unwrap_or(Err(BackupError::WorkerPanic));
        let outcome = match result {
            Ok(()) => InvocationOutcome::Complete,
            Err(error) => {
                let pause_state = error.pause_state();
                saved.blocked_reason = Some(error.to_string());
                match store.reopen().and_then(|fresh| {
                    fresh.write("pause.json", &pause_state)?;
                    fresh.save(&saved)
                }) {
                    Ok(()) => InvocationOutcome::Paused(pause_state),
                    Err(checkpoint) => InvocationOutcome::Paused(checkpoint.pause_state()),
                }
            }
        };
        drop(client);
        drop(store);
        drop(source);
        drop(permit);
        drop(pause);
        outcome
    }
}

pub mod model {
    use crate::backup_runtime::BackupError;
    pub use crate::backup_runtime::Phase;
    use crate::byte_cloud_backup::{
        work::Work, AutomaticBackupStatus, ManualConnectionStatus, Result,
    };
    use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Commit};
    use cloud_backup_protocol::{AccountId, Id};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(in crate::byte_cloud_backup) struct StreamRecord {
        pub source_home: Id,
        pub stream: cloud_backup_protocol::v2::Stream,
    }

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
            if self.local_action.is_some()
                && (self.begin_submitted
                    || self.commit.is_some()
                    || self.accepted.is_some()
                    || self.phase == Phase::Published
                    || self.automatic_revision.is_none()
                    || !matches!(self.work, Work::MutationLog { .. }))
            {
                return Err(BackupError::Invalid);
            }
            if self.format != 2 || self.account.as_str().len() != 32 || self.endpoint.len() > 4096 {
                return Err(BackupError::Invalid);
            }
            if (self.begin_submitted && self.begin.is_none())
                || (self.commit.is_some() && !self.begin_submitted)
            {
                return Err(BackupError::Invalid);
            }
            if self.multi_file_pack_objects > self.metadata_pack_objects {
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
            if self.phase == Phase::Published && self.accepted.is_none() {
                return Err(BackupError::Invalid);
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
        pub shared_sync: Option<crate::shared_sync::Status>,
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

pub(crate) mod recovery {
    use crate::backup_runtime::BackupError;
    use crate::byte_cloud_backup::{work::LogContext, Result};
    use cloud_backup_protocol::v2::AcceptedBackup;
    use fold_db::storage::disk_pack::mutation_log::{Frontiers, LogAnchor, LogPosition};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(in crate::byte_cloud_backup) struct RecoveryPoint {
        pub accepted: AcceptedBackup,
        pub base: AcceptedBackup,
        pub anchor: Option<LogAnchor>,
        pub through: Option<LogPosition>,
        pub frontiers: Frontiers,
    }
    impl RecoveryPoint {
        pub fn validate(&self) -> Result<()> {
            self.accepted.validate()?;
            self.base.validate()?;
            if self.base.job().stream != self.accepted.job().stream
                || self.base.ordinal.get() > self.accepted.ordinal.get()
                || self.anchor.is_some() != self.through.is_some()
            {
                return Err(BackupError::Identity);
            }
            if let (Some(anchor), Some(through)) = (&self.anchor, &self.through) {
                let start = anchor.position()?;
                if through.epoch != start.epoch
                    || through.frontiers != self.frontiers
                    || (self.accepted == self.base && *through != start)
                {
                    return Err(BackupError::Identity);
                }
            }
            Ok(())
        }
        pub fn log_context(&self) -> Result<LogContext> {
            self.validate()?;
            Ok(LogContext {
                base: self.base.clone(),
                previous: self.accepted.clone(),
                anchor: self.anchor.clone().ok_or(BackupError::Source)?,
                from: self.through.clone().ok_or(BackupError::Source)?,
            })
        }
    }
}

pub use crate::backup_runtime::{BackupError, BackupRuntime};
pub use automatic::{spawn_automatic, AutomaticBackupStatus};
pub use blob_source::register_blob_source;
pub use connection::{connect_existing, ManualConnectionStatus, SensitiveConnectionRequest};
pub use control::request;
pub(crate) use control::status;
pub use credentials::client_from_home;
pub use model::{ByteBackupRequest, ByteBackupStatus, Phase};
pub(crate) use source::PublicationSource;
pub type Result<T> = std::result::Result<T, BackupError>;

pub const PUBLICATION_MARKER: &str = "share-publication.json";

pub(crate) fn is_publication_home(home: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(home.join(PUBLICATION_MARKER)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn require_enabled() -> Result<()> {
    if fold_db::env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED") {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}

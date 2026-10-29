//! A durable, exact local-only Begin cannot become a cloud coordinator job.

use super::*;
use cloud_backup_protocol::v2::{Begin, ClientDescriptor, Destination, Job, Stream, Version};
use cloud_backup_protocol::{CloudScope, DecimalU64};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    version: u8,
    source_home: Id,
    pub begin: Begin,
    pub released: bool,
}

pub(super) fn load(home: &Path) -> Result<Option<Intent>> {
    let intent = super::super::store::read::<Intent>(&Store::base(home).join(LOCAL_INTENT_FILE))?;
    if let Some(value) = &intent {
        value.validate()?;
    }
    Ok(intent)
}

impl Intent {
    fn validate(&self) -> Result<()> {
        self.begin.validate()?;
        if self.version != 1
            || self.begin.job.stream.scope.account_generation.get() != 0
            || !self.begin.descriptor.as_value().is_null()
            || self.begin.predecessor.is_some()
            || self.begin.required_destinations != [Destination::R2, Destination::B2]
            || self.begin.recovery_dependencies != Some(Vec::new())
            || self.begin.r2_multipart.is_some()
        {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }

    pub(super) fn save(&self, home: &Path) -> Result<()> {
        self.validate()?;
        Store::write_configuration(home, LOCAL_INTENT_FILE, self)
    }
}

pub(super) fn admit(
    home: &Path,
    source: &Source,
    action: LocalSnapshotAction,
    id: &Id,
) -> Result<Intent> {
    if let Some(intent) = load(home)? {
        if intent.source_home != source.source_home
            || intent.begin.job.stream.scope.account_id != source.account
        {
            return Err(BackupError::Identity);
        }
        if intent.begin.job.commit_id == *id {
            return if intent.released && matches!(action, LocalSnapshotAction::Prepare) {
                Err(BackupError::Invalid)
            } else {
                Ok(intent)
            };
        }
        if !intent.released {
            return Err(BackupError::Busy);
        }
    }
    if !matches!(action, LocalSnapshotAction::Prepare) {
        return Err(BackupError::Missing);
    }
    let intent = Intent {
        version: 1,
        source_home: source.source_home.clone(),
        released: false,
        begin: Begin {
            version: Version::V2,
            job: Job {
                stream: Stream {
                    scope: CloudScope {
                        account_id: source.account.clone(),
                        // This local scope is never submitted or treated as authority.
                        account_generation: DecimalU64::new(0),
                    },
                    stream_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
                },
                commit_id: id.clone(),
            },
            descriptor: ClientDescriptor::empty(),
            required_destinations: vec![Destination::R2, Destination::B2],
            predecessor: None,
            recovery_dependencies: Some(Vec::new()),
            r2_multipart: None,
        },
    };
    intent.save(home)?;
    Ok(intent)
}

pub(super) fn require_no_remote(home: &Path) -> Result<()> {
    let Some(id) = Store::active(home)? else {
        return Ok(());
    };
    let saved = super::super::store::state(home, &id)?.ok_or(BackupError::Invalid)?;
    if !saved.terminal() {
        return Err(BackupError::Busy);
    }
    Ok(())
}

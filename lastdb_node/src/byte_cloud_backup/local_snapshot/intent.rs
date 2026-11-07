//! A durable, exact local-only Begin cannot become a cloud coordinator job.

use super::{BackupError, Result, LOCAL_INTENT_FILE};
use cloud_backup_protocol::v2::{Begin, Destination};
use cloud_backup_protocol::Id;
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    version: u8,
    #[serde(rename = "source_home")]
    _source_home: Id,
    begin: Begin,
    pub(super) released: bool,
}

pub(super) fn load(home: &Path) -> Result<Option<Intent>> {
    let intent = super::super::store::read::<Intent>(
        &super::super::store::Store::base(home).join(LOCAL_INTENT_FILE),
    )?;
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
            || !self.begin.recovery_dependencies.is_empty()
            || self.begin.r2_multipart.is_some()
        {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
}

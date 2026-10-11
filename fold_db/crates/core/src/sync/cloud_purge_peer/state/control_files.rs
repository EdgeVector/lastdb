//! Exact immutable controls keep manifests outside the bounded peer account index.

use super::*;
use crate::sync::cloud_purge::inventory::bounded_json;
use crate::sync::cloud_purge::protocol::CloudPurgePhase;
use std::collections::VecDeque;

pub(super) const MAX_CONTROL_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlRef {
    pub(super) sha256: String,
    pub(super) bytes: u64,
    pub(super) account: CloudAccount,
    pub(super) generation: u64,
    pub(super) revision: u64,
    pub(super) operation_id: String,
    pub(super) phase: CloudPurgePhase,
}

#[derive(Default)]
pub(super) struct ControlCache(VecDeque<(ControlRef, Arc<CloudPurgeControl>)>);

impl ControlRef {
    fn for_control(control: &CloudPurgeControl, bytes: &[u8]) -> Self {
        Self {
            sha256: crate::hex::sha256_hex(bytes),
            bytes: bytes.len() as u64,
            account: control.account.clone(),
            generation: control.generation,
            revision: control.revision,
            operation_id: control.operation_id.clone(),
            phase: control.phase.clone(),
        }
    }

    pub(super) fn validate(&self) -> SyncResult<()> {
        if !crate::hex::is_lower_hex_sha256(&self.sha256)
            || self.bytes == 0
            || self.bytes > MAX_CONTROL_BYTES as u64
            || self.generation == 0
            || self.account.id.is_empty()
            || uuid::Uuid::parse_str(&self.operation_id).is_err()
        {
            return Err(held("the peer control reference is invalid"));
        }
        Ok(())
    }

    fn matches(&self, control: &CloudPurgeControl) -> bool {
        self.account == control.account
            && self.generation == control.generation
            && self.revision == control.revision
            && self.operation_id == control.operation_id
            && self.phase == control.phase
    }
}

impl CloudPurgePeer {
    pub(super) fn load_control(
        &self,
        reference: &ControlRef,
    ) -> SyncResult<Arc<CloudPurgeControl>> {
        reference.validate()?;
        let mut cache = self
            .control_cache
            .lock()
            .map_err(|_| held("peer control cache unavailable"))?;
        if let Some((cached, control)) = cache
            .0
            .iter()
            .find(|(entry, _)| entry.sha256 == reference.sha256)
        {
            if cached.bytes != reference.bytes || !reference.matches(control) {
                return Err(held("the peer control header changed"));
            }
            return Ok(Arc::clone(control));
        }
        let control = Arc::new(read(&self.directory, reference, &self.source_id)?);
        cache.insert(reference.clone(), Arc::clone(&control));
        Ok(control)
    }

    pub(super) fn store_control(&self, control: &CloudPurgeControl) -> SyncResult<ControlRef> {
        let reference = store(&self.directory, control, &self.source_id)?;
        self.control_cache
            .lock()
            .map_err(|_| held("peer control cache unavailable"))?
            .insert(reference.clone(), Arc::new(control.clone()));
        Ok(reference)
    }
}

impl ControlCache {
    fn insert(&mut self, reference: ControlRef, control: Arc<CloudPurgeControl>) {
        if self
            .0
            .iter()
            .any(|(entry, _)| entry.sha256 == reference.sha256)
        {
            return;
        }
        while !self.0.is_empty()
            && (self.0.len() >= 8
                || self.0.iter().map(|(entry, _)| entry.bytes).sum::<u64>() + reference.bytes
                    > MAX_CONTROL_BYTES as u64)
        {
            self.0.pop_front();
        }
        self.0.push_back((reference, control));
    }
}

pub(super) fn store(
    directory: &Path,
    control: &CloudPurgeControl,
    source_id: &str,
) -> SyncResult<ControlRef> {
    validate(control, source_id)?;
    let encoded = bounded_json(control, MAX_CONTROL_BYTES)?;
    let reference = ControlRef::for_control(control, &encoded);
    let path = path(directory, &reference);
    if path.exists() {
        read(directory, &reference, source_id)?;
    } else {
        write_atomic(&path, &encoded)?;
    }
    Ok(reference)
}

pub(super) fn read(
    directory: &Path,
    reference: &ControlRef,
    source_id: &str,
) -> SyncResult<CloudPurgeControl> {
    reference.validate()?;
    let path = path(directory, reference);
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.len() != reference.bytes {
        return Err(held("the immutable peer control file type or size changed"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() as u64 != reference.bytes || crate::hex::sha256_hex(&bytes) != reference.sha256 {
        return Err(held("the immutable peer control file checksum changed"));
    }
    let control: CloudPurgeControl = serde_json::from_slice(&bytes)?;
    if !reference.matches(&control) {
        return Err(held("the immutable peer control header differs"));
    }
    validate(&control, source_id)?;
    Ok(control)
}

fn path(directory: &Path, reference: &ControlRef) -> PathBuf {
    directory
        .join("controls")
        .join(format!("{}.json", reference.sha256))
}

fn validate(control: &CloudPurgeControl, source_id: &str) -> SyncResult<()> {
    control.validate_identity().map_err(held)?;
    if control.phase == CloudPurgePhase::Ready {
        AdoptionPlan::from_control(control, source_id)?;
    }
    if let Some(reference) = &control.replacement_manifest_ref {
        reference.validate(&control.account, &control.operation_id, "replacement")?;
        if control
            .replacement_manifest
            .as_ref()
            .map(super::super::sha256)
            .transpose()?
            .as_deref()
            != Some(reference.sha256.as_str())
        {
            return Err(held("the peer control replacement reference differs"));
        }
    }
    if let Some(previous) = control
        .previous_ready_control
        .as_ref()
        .filter(|value| !value.is_null())
    {
        let previous: CloudPurgeControl = serde_json::from_value(previous.clone())?;
        if previous
            .previous_ready_control
            .as_ref()
            .is_some_and(|value| !value.is_null())
            || previous.account != control.account
        {
            return Err(held("the peer historical control is invalid"));
        }
        AdoptionPlan::from_control(&previous, source_id)?;
    }
    Ok(())
}

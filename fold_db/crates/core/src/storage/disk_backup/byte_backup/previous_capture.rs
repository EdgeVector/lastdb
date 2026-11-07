//! Retire a snapshot capture saved by a program with an older capture format.
//!
//! A personal capture of the previous format also held the CAS home, and no
//! current job can resume it: the automatic policy of that program is
//! retired with it, so the next job takes a full snapshot. This reads only
//! the fields that locate the capture's local files, releases its held run
//! files, and removes its saved records. It converts nothing. A capture of
//! the current format is never touched here.

use super::{run_capture::held_root, run_source::RunSource, store::Store, PackRuntime};
use super::{StorageResult, RUN_INTENT_FILE};
use serde::Deserialize;

/// The location fields that every capture format has saved.
#[derive(Deserialize)]
struct Located {
    format: u16,
    reference_id: uuid::Uuid,
    objects: u64,
}

#[derive(Deserialize)]
struct LocatedIntent {
    reference_id: uuid::Uuid,
    begin: LocatedBegin,
}

#[derive(Deserialize)]
struct LocatedBegin {
    job: LocatedJob,
}

#[derive(Deserialize)]
struct LocatedJob {
    commit_id: String,
}

/// True when the saved capture is of a previous format and was retired. An
/// intent goes with it, or alone when it belongs to `dropped`: the job that
/// the retired automatic policy of the previous program still owned. A
/// missing or current capture returns false.
pub(super) fn retire(
    runtime: &PackRuntime,
    store: &Store,
    dropped: Option<&str>,
) -> StorageResult<bool> {
    let job = if store.exists("job.json")? {
        let job: Located = store.read("job.json")?;
        if job.format >= super::model::STATE_FORMAT {
            return Ok(false);
        }
        Some(job)
    } else {
        None
    };
    let intent = if store.exists(RUN_INTENT_FILE)? {
        Some(store.read::<LocatedIntent>(RUN_INTENT_FILE)?)
    } else {
        None
    };
    let dropped_intent = intent
        .as_ref()
        .is_some_and(|intent| Some(intent.begin.job.commit_id.as_str()) == dropped);
    if job.is_none() && !dropped_intent {
        return Ok(false);
    }
    let references = job
        .as_ref()
        .map(|job| job.reference_id)
        .into_iter()
        .chain(intent.map(|intent| intent.reference_id))
        .filter(|id| !id.is_nil());
    for reference in references {
        let root = held_root(runtime, reference);
        if root.try_exists()? {
            RunSource::release_saved(&root)?;
        }
    }
    if let Some(job) = &job {
        let last = job.objects.min(super::model::MAX_OBJECTS);
        for index in 0..=last {
            for name in [
                super::model::object_name(index),
                format!("payload-{index:08}.enc"),
            ] {
                store.remove(&name)?;
                store.remove(&format!("{name}.pending"))?;
            }
        }
    }
    store.remove(RUN_INTENT_FILE)?;
    store.remove("job.json")?;
    Ok(true)
}

impl PackRuntime {
    /// Retire a personal snapshot capture saved by a program with an older
    /// capture format, and the intent of `dropped`, the job that an older
    /// automatic policy still owned. Returns whether anything was retired.
    /// It runs on the caller's thread: the automatic backup tick calls it
    /// once, before it writes the new policy.
    pub fn retire_previous_backup_capture(&self, dropped: Option<String>) -> StorageResult<bool> {
        self.require_ordinary_backup_home()?;
        let _admission = super::super::NativeCloudWorkGuard::acquire()?;
        let store = Store::open(self.physical.path(), &self.space_policy)?;
        retire(self, &store, dropped.as_deref())
    }
}

//! Recover a local-only log intent after native recovery requires a new snapshot.
use super::*;
use crate::byte_cloud_backup::{model::LocalAction, source::Source};

pub(super) fn retire(host: &crate::host::Host, policy: &Policy, pending: &Pending) -> Result<bool> {
    if !matches!(pending.work, Work::MutationLog { .. }) {
        return Ok(false);
    }
    let previous = store::state(&host.home, &pending.id)?;
    if let Some(saved) = &previous {
        policy.validate_job(saved)?;
        if saved.terminal() || saved.begin_submitted {
            return Ok(false);
        }
    }
    let runtime = native(host)?;
    let journal = runtime.mutation_log_status()?;
    let Work::MutationLog { context } = &pending.work else {
        return Err(BackupError::Invalid);
    };
    let already_replacing = previous.as_ref().is_some_and(|s| s.local_action.is_some());
    if !already_replacing
        && !journal.snapshot_required
        && journal.anchor.as_ref() == Some(&context.anchor)
    {
        return Ok(false);
    }
    let _permit = Store::lock(&host.home)?;
    super::super::control::ensure_previous(&host.home, &pending.id)?;
    if runtime.mutation_log_capture_materialized(&pending.id)? {
        return if already_replacing {
            Err(BackupError::Identity)
        } else {
            Ok(false)
        };
    }
    let source = Source::from_host(host)?;
    let client = super::super::connection::client_for_host(host)?;
    let mut saved = previous.unwrap_or_else(|| {
        let mut saved = source.saved(pending.id.clone(), client.canonical_endpoint());
        saved.work = pending.work.clone();
        saved.automatic_revision = Some(pending.revision);
        saved
    });
    policy.validate_job(&saved)?;
    source.validate(&saved)?;
    let store = Store::open(&host.home, &pending.id)?;
    saved.local_action = Some(LocalAction::ReplaceWithSnapshot);
    store.save(&saved)?;
    store.select(&pending.id)?;
    source.cancel_replacement(&saved)?;
    saved.phase = Phase::Closed;
    saved.retained_source_bytes = Some(0);
    saved.blocked_reason = None;
    store.save(&saved)?;
    Ok(true)
}

use crate::db_operations::DbOperations;
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// The database an injection was armed for. A `Weak` keeps the allocation
/// alive, so no later database can reuse the address and match by accident.
pub type Owner = Weak<DbOperations>;
/// One slot of the table: at most one arm per database. Several
/// databases can hold an arm in the same slot at once, so a parallel
/// test never overwrites another test's arm.
pub type Armed<T> = Vec<(Owner, T)>;
type Pause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

fn is_owner(owner: &Owner, db_ops: &Arc<DbOperations>) -> bool {
    std::ptr::eq(owner.as_ptr(), Arc::as_ptr(db_ops))
}

/// Set (`Some`) or clear (`None`) the arm of `db_ops` only.
pub fn arm<T>(slot: &mut Armed<T>, db_ops: &Arc<DbOperations>, value: Option<T>) {
    slot.retain(|(owner, _)| !is_owner(owner, db_ops));
    if let Some(value) = value {
        slot.push((Arc::downgrade(db_ops), value));
    }
}

/// The arm of exactly this database, if any.
pub fn armed<'a, T>(slot: &'a Armed<T>, db_ops: &Arc<DbOperations>) -> Option<&'a T> {
    slot.iter()
        .find(|(owner, _)| is_owner(owner, db_ops))
        .map(|(_, value)| value)
}

pub fn armed_mut<'a, T>(slot: &'a mut Armed<T>, db_ops: &Arc<DbOperations>) -> Option<&'a mut T> {
    slot.iter_mut()
        .find(|(owner, _)| is_owner(owner, db_ops))
        .map(|(_, value)| value)
}

/// True when this database armed `slot` for `schema`.
pub fn armed_for_schema(slot: &Armed<String>, db_ops: &Arc<DbOperations>, schema: &str) -> bool {
    armed(slot, db_ops).is_some_and(|armed| armed == schema)
}

/// Consume one attempt of a counted schema arm of this database.
pub fn take_attempt(
    slot: &mut Armed<(String, u32)>,
    db_ops: &Arc<DbOperations>,
    schema: &str,
) -> bool {
    match armed_mut(slot, db_ops) {
        Some((armed, attempts)) if armed == schema && *attempts > 0 => {
            *attempts -= 1;
            true
        }
        _ => false,
    }
}

fn take_owned_pause(slot: &mut Armed<Pause>, db_ops: &Arc<DbOperations>) -> Option<Pause> {
    let index = slot.iter().position(|(owner, _)| is_owner(owner, db_ops))?;
    Some(slot.swap_remove(index).1)
}

#[derive(Default)]
pub struct Inject {
    pub before_publish_schemas: Armed<Vec<String>>,
    pub after_schema: Armed<String>,
    pub persist_reserve_schema: Armed<String>,
    pub protein_fold_before_publish: Armed<()>,
    pub after_publish_persist_schema: Armed<String>,
    pub persist_lane_wait_after_publish: Armed<()>,
    pub pause_after_purge_publish: Armed<Pause>,
    pub deferred_persist: Armed<(String, u32)>,
    pub purge_after_durable_apply: Armed<(String, u32)>,
    pub purge_finalize_replays: Armed<u32>,
    pub pause_after_cas_check: Armed<Pause>,
    pub pause_replay_after_filter: Armed<Pause>,
}

impl Inject {
    fn prune_dropped_owners(&mut self) {
        fn prune<T>(slot: &mut Armed<T>) {
            slot.retain(|(owner, _)| owner.strong_count() > 0);
        }
        prune(&mut self.before_publish_schemas);
        prune(&mut self.after_schema);
        prune(&mut self.persist_reserve_schema);
        prune(&mut self.protein_fold_before_publish);
        prune(&mut self.after_publish_persist_schema);
        prune(&mut self.persist_lane_wait_after_publish);
        prune(&mut self.pause_after_purge_publish);
        prune(&mut self.deferred_persist);
        prune(&mut self.purge_after_durable_apply);
        prune(&mut self.purge_finalize_replays);
        prune(&mut self.pause_after_cas_check);
        prune(&mut self.pause_replay_after_filter);
    }
}

/// Consume the pause armed for this database, if any — fires at most once
/// per arm, and never for another test's database.
pub fn take_pause_after_cas_check(db_ops: &Arc<DbOperations>) -> Option<Pause> {
    take_owned_pause(&mut lock().pause_after_cas_check, db_ops)
}

#[cfg(feature = "cloud-sync")]
pub fn take_pause_replay_after_filter(db_ops: &Arc<DbOperations>) -> Option<Pause> {
    take_owned_pause(&mut lock().pause_replay_after_filter, db_ops)
}

pub fn lock() -> std::sync::MutexGuard<'static, Inject> {
    static STATE: OnceLock<Mutex<Inject>> = OnceLock::new();
    STATE
        .get_or_init(|| Mutex::new(Inject::default()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn prune_dropped_owners() {
    lock().prune_dropped_owners();
}

pub fn take_pause_after_purge_publish(db_ops: &Arc<DbOperations>) -> Option<Pause> {
    take_owned_pause(&mut lock().pause_after_purge_publish, db_ops)
}

//! Core publication helpers: the receipt lock and typed definition protection.

use super::*;

mod protection;

pub(crate) use protection::{PathProtection, PathPublication};

/// The published view, the call receipt, and the core for one matching revision.
/// A newer view returns no guard and does not take the receipt or the core.
/// The core drops before the receipt, and the receipt drops before the view.
pub(super) fn lock_current_receipt<'a>(
    runtime: &'a PackRuntime,
    context: &'a super::read::ReadContext,
    view_label: &str,
    core_label: &str,
) -> Option<CurrentReceipt<'a>> {
    let view = runtime.view.read().expect(view_label);
    if view.revision != context.revision {
        return None;
    }
    let call = crate::read_receipts::lock(&context.call);
    let core = runtime.core.lock().expect(core_label);
    Some(CurrentReceipt {
        core,
        call,
        _view: view,
    })
}

pub(super) struct CurrentReceipt<'a> {
    pub(super) core: std::sync::MutexGuard<'a, crate::resident::CoreWarmSet>,
    pub(super) call: std::sync::MutexGuard<'a, super::physical::PhysicalRead>,
    _view: std::sync::RwLockReadGuard<'a, super::view::NativeReadView>,
}

//! Runtime `EnvFilter` control for the `lastdbd` daemon.
//!
//! The observability crate has shipped a tested reload layer
//! (`observability::layers::reload::build_reload_layer`) since Phase 5, but no
//! binary ever held its [`ReloadHandle`]: `lastdbd` installed a plain,
//! non-reloadable `EnvFilter` at startup. The only way to change log verbosity
//! on the primary was therefore `RUST_LOG=… ` at process start, i.e. a
//! primary-brain restart — the one operation agents are standing-ordered never
//! to perform. So the safe default (quiet logs) was gated behind the most
//! dangerous operation in the system, in both directions: the level could not
//! go down to cut noise, and could not go up to chase a symptom.
//!
//! This module owns the process-global handle plus the directive currently in
//! force, so `POST /api/system/log-filter` can swap the filter in place.
//!
//! **Why a process global rather than a field on `Host`.** The subscriber this
//! handle drives is itself a process global — `tracing_subscriber` installs one
//! per process, and `init_tracing` runs before any host exists. A handle stored
//! on the host would be a second lifetime for a thing that only ever has one.
//!
//! The directive is tracked here because [`ReloadHandle`] is write-only
//! upstream: it can install a filter and cannot report the installed one. A
//! control surface that can set a value it cannot read back is a surface an
//! operator cannot trust, so the setter records what it installed and
//! [`current`] serves that.

use std::sync::{Mutex, OnceLock};

use observability::layers::reload::{ReloadError, ReloadHandle};

/// Reason a filter change could not be applied.
///
/// Hand-written rather than derived: `lastdb_node` does not depend on
/// `thiserror`, and a two-variant error is not worth a new dependency in the
/// daemon's tree.
#[derive(Debug)]
pub enum LogFilterError {
    /// `init_tracing` never installed a handle in this process. Every request
    /// path in the daemon runs after `init_tracing`, so this is reachable only
    /// from a test binary or an embedder that built its own subscriber.
    NotInstalled,
    /// The supplied directive is not valid `EnvFilter` syntax. The active
    /// filter is unchanged.
    Rejected(ReloadError),
}

impl std::fmt::Display for LogFilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInstalled => {
                f.write_str("runtime log-filter control is not installed in this process")
            }
            Self::Rejected(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LogFilterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotInstalled => None,
            Self::Rejected(e) => Some(e),
        }
    }
}

impl From<ReloadError> for LogFilterError {
    fn from(e: ReloadError) -> Self {
        Self::Rejected(e)
    }
}

struct Control {
    handle: ReloadHandle,
    /// The directive currently in force. Held separately because
    /// [`ReloadHandle`] cannot report it.
    active: Mutex<String>,
}

static CONTROL: OnceLock<Control> = OnceLock::new();

/// Register the process-wide reload handle and the directive the subscriber
/// was built with.
///
/// Idempotent by construction: a second call is a no-op and returns `false`,
/// matching `tracing_subscriber`'s own install-once contract rather than
/// panicking on a double init in a test binary.
pub fn install(handle: ReloadHandle, initial_directive: impl Into<String>) -> bool {
    CONTROL
        .set(Control {
            handle,
            active: Mutex::new(initial_directive.into()),
        })
        .is_ok()
}

/// Whether this process can change its log filter at runtime.
pub fn is_installed() -> bool {
    CONTROL.get().is_some()
}

/// The directive currently in force, or `None` when no handle is installed.
pub fn current() -> Option<String> {
    let control = CONTROL.get()?;
    Some(lock_active(control).clone())
}

/// Parse `directive` and install it as the active filter, returning the
/// directive it replaced.
///
/// On a parse failure the active filter is untouched — [`ReloadHandle::update`]
/// validates before it reloads — so a typo at the control surface cannot leave
/// the daemon logging at an unintended level.
pub fn apply(directive: &str) -> Result<String, LogFilterError> {
    let control = CONTROL.get().ok_or(LogFilterError::NotInstalled)?;
    control.handle.update(directive)?;
    let mut active = lock_active(control);
    let previous = std::mem::replace(&mut *active, directive.to_string());
    Ok(previous)
}

/// Read the recorded directive, recovering from a poisoned lock.
///
/// The mutex guards a `String` and every writer replaces it wholesale, so there
/// is no torn state a panic could leave behind — treating poisoning as fatal
/// would turn an unrelated panic elsewhere into a permanently unreadable
/// control surface.
fn lock_active(control: &Control) -> std::sync::MutexGuard<'_, String> {
    control
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

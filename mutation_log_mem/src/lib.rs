//! In-memory [`MutationLog`] adapter for the unified mutation log.
//!
//! The product does not construct this adapter. [`MutationLogConfig`] defaults
//! off, and [`MutationLogConfig::open`] returns [`None`] until a caller passes
//! an enabled value. This crate does not read the process environment, open a
//! socket, or call Exemem. The live Share path is unchanged.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod memory;
mod port;

pub use memory::InMemoryMutationLog;
pub use port::MutationLog;

use mutation_log::{LogState, NamespacePolicy};

/// Caller-supplied switch for the in-memory log.
///
/// The default is off. A product shell that wants the flag reads
/// [`Self::FLAG`] itself and passes the string here. This crate does not
/// read the process environment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MutationLogConfig {
    enabled: bool,
}

impl MutationLogConfig {
    /// Name a product shell may read. This crate does not read it.
    pub const FLAG: &'static str = "LASTDB_MUTATION_LOG";

    /// `None`, empty, and every non-truthy string stay off.
    ///
    /// Truthy values are `env_flag`'s set: `1`, `true`, `yes`, and `on`.
    #[must_use]
    pub fn from_value(raw: Option<&str>) -> Self {
        Self {
            enabled: raw.is_some_and(env_flag::truthy),
        }
    }

    /// Whether [`Self::open`] installs an adapter.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        self.enabled
    }

    /// Install the adapter when this config is enabled.
    ///
    /// A disabled config returns [`None`]. The offered state is dropped, so
    /// the caller keeps the live path and does not call this with the only
    /// copy of a log it still needs.
    #[must_use]
    pub fn open<P: NamespacePolicy>(
        self,
        state: LogState,
        policy: P,
    ) -> Option<InMemoryMutationLog<P>> {
        self.enabled
            .then(|| InMemoryMutationLog::new(state, policy))
    }
}

#[cfg(test)]
mod tests;

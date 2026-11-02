//! Optional query reply shapes. The read, access rules and integrity checks stay shared.

use serde::Deserialize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Which query reply shape the current task should build.
pub enum QueryResponseFormat {
    #[default]
    /// Rows include the record metadata.
    Full,
    /// Rows keep the keys, the fields and the conflict flag, without metadata.
    Values,
    /// Return a count for the keyed scope and do not build rows.
    Count,
}

tokio::task_local! {
    static FORMAT: QueryResponseFormat;
}

impl QueryResponseFormat {
    /// Scope one query item without changing the format of concurrent batch items.
    pub async fn run<T>(self, future: impl std::future::Future<Output = T>) -> T {
        FORMAT.scope(self, future).await
    }

    pub(super) fn current() -> Self {
        FORMAT.try_with(|format| *format).unwrap_or_default()
    }
}

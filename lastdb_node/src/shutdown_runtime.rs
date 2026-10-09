use std::time::Duration;

/// Finish runtime teardown after the daemon's writer drain and final flush.
///
/// Tokio's default runtime drop waits forever for started blocking tasks. A
/// telemetry purge can outlive the bounded writer drain, so even a refused
/// flush proof must use bounded teardown. On that path the daemon returns an
/// error and leaves no receipt; a supervisor still requires process exit
/// before it copies the source home.
pub(crate) fn finish(
    runtime: tokio::runtime::Runtime,
    shutdown_result: Result<(), String>,
) -> Result<(), String> {
    runtime.shutdown_timeout(Duration::from_secs(1));
    shutdown_result
}

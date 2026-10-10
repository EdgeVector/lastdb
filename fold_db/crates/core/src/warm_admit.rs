//! Schema-owner class for one query or mutation.
//!
//! The warm set reads a thread-local inside `spawn_blocking`. A task-local
//! set here survives the await. The blocking wrapper copies it onto the
//! worker thread. `X-LastDB-Client` is not an input.

tokio::task_local! {
    static WARM_ADMIT_CODE: u8;
}

/// Admission code visible to the storage call on this task. `0` when unset.
pub fn current_code() -> u8 {
    WARM_ADMIT_CODE.try_with(|code| *code).unwrap_or(0)
}

/// `lastgit` is the only background owner.
pub fn owner_is_background(owner: Option<&str>) -> bool {
    owner == Some("lastgit")
}

/// Run `fut` with the owner's admission code.
///
/// Code `2` is `lastgit`. Every other owner, including `None`, is code `1`.
pub async fn with_schema_owner<T>(
    owner: Option<&str>,
    fut: impl std::future::Future<Output = T>,
) -> T {
    let code = if owner_is_background(owner) { 2 } else { 1 };
    WARM_ADMIT_CODE.scope(code, fut).await
}

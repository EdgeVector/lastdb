//! Restart intent read and cause classification. Moved verbatim from `session_ledger.rs`.

use super::*;

pub(super) fn read_restart_intent(
    home: &Path,
    previous: Option<&SessionRecord>,
    now: u64,
) -> RestartIntentRead {
    let path = Ledger::restart_intent_path(home);
    let Ok(metadata) = std::fs::metadata(&path) else {
        return RestartIntentRead::default();
    };
    let mut result = RestartIntentRead {
        marker_present: true,
        cause: None,
    };
    if metadata.len() > RESTART_INTENT_MAX_BYTES {
        return result;
    }
    let Ok(bytes) = std::fs::read(&path) else {
        return result;
    };
    let Ok(intent) = serde_json::from_slice::<RestartIntent>(&bytes) else {
        return result;
    };
    let valid_cause = matches!(
        intent.cause.as_str(),
        RESTART_CAUSE_UPGRADE | RESTART_CAUSE_GUARD_MEMORY | RESTART_CAUSE_OPERATOR
    );
    let matches_previous = previous.is_some_and(|record| record.pid == intent.previous_pid);
    let age = now.checked_sub(intent.created_at);
    let fresh = age.is_some_and(|age| age <= RESTART_INTENT_MAX_AGE_SECS);
    if valid_cause && matches_previous && fresh {
        result.cause = Some(intent.cause);
    }
    result
}

pub(super) fn classify_restart_cause(
    previous: Option<&SessionRecord>,
    current_build: &str,
    previous_exit: Option<&str>,
    previous_reason: Option<&str>,
    shutdown_cause: Option<&str>,
) -> String {
    let Some(previous) = previous else {
        return "initial".to_string();
    };
    if previous.build_version != current_build && previous_exit == Some(EXIT_CLEAN) {
        return RESTART_CAUSE_UPGRADE.to_string();
    }
    let reason = previous_reason.unwrap_or("").to_ascii_lowercase();
    if reason.contains("guard-memory") {
        return "guard-memory".to_string();
    }
    if reason.contains("upgrade") {
        return "upgrade".to_string();
    }
    if reason.contains("supersession") {
        return "supersession".to_string();
    }
    if previous_exit == Some(EXIT_CLEAN) || previous_exit == Some(EXIT_SHUTDOWN_STARTED) {
        return "operator".to_string();
    }
    if previous_exit == Some(EXIT_STARTUP_FAILED) {
        return "startup-failed".to_string();
    }
    if shutdown_cause.is_some_and(|cause| cause.to_ascii_lowercase().contains("panic")) {
        return "panic".to_string();
    }
    // An open prior record had no clean or supervised terminal mark. It is a
    // crash for the canary verdict until a policy explicitly says otherwise.
    "crash".to_string()
}

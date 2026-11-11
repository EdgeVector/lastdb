use super::*;

pub(super) fn execute_auto_identity_route(host: &Host) -> UdsResponse {
    let public_key = host.public_key();
    let body = serde_json::json!({
        "user_id": public_key,
        "user_hash": host.user_hash,
        "public_key": public_key,
    });
    json_ok(&body)
}

pub(super) fn execute_boot_identity_route(host: &Host) -> UdsResponse {
    let body = serde_json::json!({
        "pid": std::process::id(),
        "process_start_ts": host.process_start_ts,
        "build": crate::version::build_version(),
        "restart_cause": "clean",
    });
    json_ok(&body)
}

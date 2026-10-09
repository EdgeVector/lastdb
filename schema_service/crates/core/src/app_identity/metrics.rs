//! Structured log lines for app-registry operations (CloudWatch-Insights-friendly).

pub(super) fn record_app_register(env: &str, status: &str, duration_secs: f64) {
    tracing::info!(
        target: "schema_service::app_identity",
        metric = "app_register_total",
        env = %env,
        status = %status,
        duration_secs = duration_secs,
        "app register outcome"
    );
}

pub(super) fn record_app_update(env: &str, status: &str, duration_secs: f64) {
    tracing::info!(
        target: "schema_service::app_identity",
        metric = "app_update_total",
        env = %env,
        status = %status,
        duration_secs = duration_secs,
        "app update outcome"
    );
}

pub(super) fn record_app_promote(env: &str, status: &str, duration_secs: f64) {
    tracing::info!(
        target: "schema_service::app_identity",
        metric = "app_promote_total",
        env = %env,
        status = %status,
        duration_secs = duration_secs,
        "app promote outcome"
    );
}

pub(super) fn record_apps_registry_size(size: usize) {
    tracing::info!(
        target: "schema_service::app_identity",
        metric = "apps_registry_size",
        size = size,
        "apps registry size"
    );
}

pub(super) fn record_schema_claim(status: &str, owner_app_id: Option<&str>) {
    tracing::info!(
        target: "schema_service::app_identity",
        metric = "schema_claim_total",
        status = %status,
        owner_app_id = owner_app_id.unwrap_or("-"),
        "schema claim outcome"
    );
}

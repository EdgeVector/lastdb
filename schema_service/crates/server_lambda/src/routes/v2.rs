//! `/v2` read routes for app registry: apps and releases.

use crate::http::*;
use lambda_http::{Body, Error, Response};
use schema_service_core::app_release::release_verification_location;
use schema_service_core::invalid_app_id_body;
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::json;

/// The release log base URL the verification redirect points into.
const RELEASE_LOG_URL_ENV: &str = "RELEASE_LOG_URL";

// Anonymous: redirect to the verifier's result for one release. Needs no
// state, so it is served before the cold-start state is touched.
pub(crate) fn get_release_verification(p: &str) -> Result<Response<Body>, Error> {
    let release_id = p
        .trim_start_matches("/v2/releases/")
        .trim_end_matches("/verification");
    let release_log_url = std::env::var(RELEASE_LOG_URL_ENV).ok();
    match release_verification_location(release_log_url.as_deref(), release_id) {
        Ok(location) => cors_builder(307, "application/json")
            .header("Location", location.as_str())
            .body(Body::from(json!({ "location": location }).to_string()))
            .map_err(|e| Error::from(format!("Failed to build response: {e}"))),
        Err((status, body)) => json_response(status, &body),
    }
}

// Anonymous read: list all apps (ordered by app_id).
pub(crate) async fn list_apps_v2(state: &SchemaServiceState) -> Result<Response<Body>, Error> {
    match state.list_all_apps().await {
        Ok(apps) => {
            let apps_json: Vec<_> = apps
                .iter()
                .map(|record| {
                    let revoked = state
                        .app_identity_config()
                        .revoked_dev_pubkeys
                        .contains(&record.owner_dev_pubkey);
                    json!({
                        "app_id": record.app_id,
                        "owner_dev_pubkey": record.owner_dev_pubkey,
                        "metadata": record.metadata,
                        "version": record.version,
                        "registered_at": record.registered_at,
                        "tier": record.tier,
                        "revoked": revoked,
                    })
                })
                .collect();
            json_response(200, &json!({ "apps": apps_json }))
        }
        Err(e) => json_response(
            500,
            &schema_service_core::error_only_body(&format!("Failed to list apps: {e}")),
        ),
    }
}

// Anonymous read: list all releases for an app (ordered by published_at desc).
pub(crate) fn list_releases_for_app(
    state: &SchemaServiceState,
    p: &str,
) -> Result<Response<Body>, Error> {
    let app_id = p
        .trim_start_matches("/v2/apps/")
        .trim_end_matches("/releases");
    let Some(app_id) = v2_app_id(app_id) else {
        return json_response(400, &invalid_app_id_body());
    };
    let releases = state.list_releases_for_app(app_id);
    let releases_json: Vec<_> = releases
        .iter()
        .map(|r| {
            json!({
                "release_id": r.release_id,
                "app_id": r.manifest.app_id,
                "app_uuid": r.manifest.app_uuid,
                "source_commit": r.manifest.source_commit,
                "artifact_digest": r.manifest.artifact_digest,
                "published_at": r.published_at,
                "revoked": r.revoked(),
                "revoked_at": r.revoked_at,
                "revocation_reason": r.revocation_reason,
            })
        })
        .collect();
    json_response(200, &json!({ "releases": releases_json }))
}

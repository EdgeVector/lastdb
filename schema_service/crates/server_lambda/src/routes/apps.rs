//! App registry routes (`/v1/apps/*` and `/v2/apps/*`, releases, channels, revocations).

use crate::http::*;
use lambda_http::{Body, Error, Request, Response};
use schema_service_server_shared::state::SchemaServiceState;
use serde_json::{json, Value};

// App registry (app_identity v3.1, Lane B2b). Auth is the cert +
// signature envelope pair, not X-API-Key.
pub(crate) async fn post_app_v1(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let body_value: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({"reason": "invalid_metadata", "detail": format!("invalid JSON: {e}")}),
            );
        }
    };
    let (Some(cert), Some(sig)) = (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) else {
        return json_response(
            401,
            &json!({"reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header"}),
        );
    };
    match state.register_app(cert, sig, &body_value).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http(&state.deployment_env_label());
            json_response(status, &body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

// Public, lookup-by-known-id app registry read
// (app_identity v3.1, Step 1 of
// `app_identity_node_as_verifier.md`). Returns the registered
// AppRecord shape + a derived `revoked` flag (true when the
// owner's dev pubkey is on the offline denylist). 404 when
// unknown. Auth: **public** — nodes call this on bootstrap so
// they no longer have to present a developer `em_` bearer just
// to read public verification material. Bulk browse is handled
// by the literal `GET /v1/apps` arm above and is filtered to the
// promoted, non-revoked public shelf. The cross-env mirror's
// `POST /v1/apps/mirror` receiver was decommissioned in #517.
pub(crate) fn get_app_v1(state: &SchemaServiceState, p: &str) -> Result<Response<Body>, Error> {
    let app_id = p.trim_start_matches("/v1/apps/").trim_end_matches('/');
    if app_id.is_empty() || app_id.contains('/') {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    }
    match state.get_app(app_id) {
        Some(record) => {
            let revoked = state
                .app_identity_config()
                .revoked_dev_pubkeys
                .contains(&record.owner_dev_pubkey);
            // Field parity with `list_live_apps` / actix `get_app`:
            // install uses GET-by-id and requires `source` (and
            // SemVer `version`). Omitting them made list green while
            // `lastdb app install` rejected live apps as "no source".
            json_response(
                200,
                &json!({
                    "app_id": record.app_id,
                    "owner_dev_pubkey": record.owner_dev_pubkey,
                    "metadata": record.metadata,
                    "version": record.version,
                    "registered_at": record.registered_at,
                    "tier": record.tier,
                    "code_signature": record.code_signature,
                    "source": record.source,
                    "artifact": record.artifact,
                    "uses": record.uses,
                    "revoked": revoked,
                }),
            )
        }
        None => json_response(404, &json!({"reason": "unknown_app", "app_id": app_id})),
    }
}

// Owner-authenticated metadata update for an already-registered
// app (app_identity v3.1, `PUT /v1/apps/{app_id}`). Same cert +
// signature gating as POST, but the envelope `purpose` must be
// `app_update` and the signer pubkey MUST equal the registered
// owner. display_name is immutable (409 on change).
pub(crate) async fn put_app_v1(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let app_id = p.trim_start_matches("/v1/apps/").trim_end_matches('/');
    if app_id.is_empty() || app_id.contains('/') {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    }
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let body_value: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({"reason": "invalid_metadata", "detail": format!("invalid JSON: {e}")}),
            );
        }
    };
    let (Some(cert), Some(sig)) = (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) else {
        return json_response(
            401,
            &json!({"reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header"}),
        );
    };
    match state.update_app(app_id, cert, sig, &body_value).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http(&state.deployment_env_label());
            json_response(status, &body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

// Owner-authenticated sandbox→live promotion (app_identity v3.1,
// `POST /v1/apps/{app_id}/promote`). Cert + signature gated; the
// envelope `purpose` must be `app_promote`, the signer pubkey MUST
// equal the registered owner, and the cert must carry
// `authorized_publisher`. Idempotent (already-live → 200).
pub(crate) async fn promote_app_v1(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let app_id = p
        .trim_start_matches("/v1/apps/")
        .trim_end_matches("/promote");
    if app_id.is_empty() || app_id.contains('/') {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    }
    let (Some(cert), Some(sig)) = (
        header_str(event, "x-exemem-dev-cert"),
        header_str(event, "x-signature"),
    ) else {
        return json_response(
            401,
            &json!({"reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header"}),
        );
    };
    match state.promote_app(app_id, cert, sig).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http();
            json_response(status, &body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

// ─── `/v2` app release registry ──────────────────────────────
//
// Same lockstep rule as the declared-field arms below: the actix
// table in `server_http::v2_scope` and these arms are one surface
// served by two routers, so a route added to only one of them 404s
// wherever the other router runs.
//
// Ordering matters here the way it does in actix: the literal
// suffixes (`/releases`, `/revocations`, `/channels/…`) are matched
// before the bare `/v2/apps/{app_id}` read, or the read arm would
// swallow them.
pub(crate) async fn post_apps_v2(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let body = match parse_body(event) {
        Ok(b) => b,
        Err(r) => return Ok(r),
    };
    let body_value: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                400,
                &json!({"reason": "invalid_metadata", "detail": format!("invalid JSON: {e}")}),
            );
        }
    };
    let (cert, sig) = match v2_cert_headers(event) {
        Ok(pair) => pair,
        Err(r) => return r,
    };
    match state.register_app(cert, sig, &body_value).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http(&state.deployment_env_label());
            json_response(status, &body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

pub(crate) async fn post_apps_releases(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let app_id = p
        .trim_start_matches("/v2/apps/")
        .trim_end_matches("/releases");
    let Some(app_id) = v2_app_id(app_id) else {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    };
    let body_value = match v2_body(event) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (cert, sig) = match v2_cert_headers(event) {
        Ok(pair) => pair,
        Err(r) => return r,
    };
    match state.publish_release(app_id, cert, sig, &body_value).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http();
            json_response(status, &body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

pub(crate) async fn post_apps_revocations(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let app_id = p
        .trim_start_matches("/v2/apps/")
        .trim_end_matches("/revocations");
    let Some(app_id) = v2_app_id(app_id) else {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    };
    let body_value = match v2_body(event) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (cert, sig) = match v2_cert_headers(event) {
        Ok(pair) => pair,
        Err(r) => return r,
    };
    match state.revoke_release(app_id, cert, sig, &body_value).await {
        Ok(record) => json_response(200, &record.to_public_json()),
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

pub(crate) async fn put_apps_channels(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some((app_id, channel)) = v2_channel_path(p) else {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    };
    let body_value = match v2_body(event) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let (cert, sig) = match v2_cert_headers(event) {
        Ok(pair) => pair,
        Err(r) => return r,
    };
    match state
        .set_channel(app_id, channel, cert, sig, &body_value)
        .await
    {
        Ok(record) => json_response(200, &record.to_public_json()),
        Err(e) => {
            let (status, body) = e.to_http();
            json_response(status, &body)
        }
    }
}

// Anonymous read: the desired release id + the channel generation.
pub(crate) fn get_apps_channels(
    state: &SchemaServiceState,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some((app_id, channel)) = v2_channel_path(p) else {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    };
    match state.get_channel(app_id, channel) {
        Some(record) => json_response(200, &record.to_public_json()),
        None => json_response(
            404,
            &json!({"reason": "unknown_channel", "app_id": app_id, "channel": channel}),
        ),
    }
}

// Anonymous read: one release manifest by its exact key.
pub(crate) fn get_releases(state: &SchemaServiceState, p: &str) -> Result<Response<Body>, Error> {
    let release_id = p.trim_start_matches("/v2/releases/").trim_end_matches('/');
    if release_id.is_empty() || release_id.contains('/') {
        return json_response(400, &json!({"reason": "invalid_release_id"}));
    }
    match state.get_release(release_id) {
        Some(record) => json_response(200, &record.to_public_json()),
        None => json_response(
            404,
            &json!({"reason": "unknown_release", "release_id": release_id}),
        ),
    }
}

// Anonymous read: the app record. Same body as `GET /v1/apps/{id}`.
pub(crate) fn get_app_v2(state: &SchemaServiceState, p: &str) -> Result<Response<Body>, Error> {
    let app_id = p.trim_start_matches("/v2/apps/").trim_end_matches('/');
    let Some(app_id) = v2_app_id(app_id) else {
        return json_response(400, &json!({"reason": "invalid_app_id"}));
    };
    match state.get_app(app_id) {
        Some(record) => {
            let revoked = state
                .app_identity_config()
                .revoked_dev_pubkeys
                .contains(&record.owner_dev_pubkey);
            json_response(
                200,
                &json!({
                    "app_id": record.app_id,
                    "owner_dev_pubkey": record.owner_dev_pubkey,
                    "metadata": record.metadata,
                    "version": record.version,
                    "registered_at": record.registered_at,
                    "tier": record.tier,
                    "code_signature": record.code_signature,
                    "source": record.source,
                    "artifact": record.artifact,
                    "uses": record.uses,
                    "revoked": revoked,
                }),
            )
        }
        None => json_response(404, &json!({"reason": "unknown_app", "app_id": app_id})),
    }
}

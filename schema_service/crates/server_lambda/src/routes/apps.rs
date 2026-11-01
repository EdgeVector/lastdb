//! App registry routes (`/v1/apps/*` and `/v2/apps/*`, releases, channels, revocations).

use crate::http::*;
use crate::verifier_queue;
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::app_release::{PublishReleaseReply, RevokeReleaseReply};
use schema_service_core::{invalid_app_id_body, reason_only_body, PromoteAppReply};
use schema_service_server_shared::state::SchemaServiceState;

// App registry (app_identity v3.1, Lane B2b). Auth is the cert +
// signature envelope pair, not X-API-Key.
// `POST /v1/apps` and `POST /v2/apps` both use this handler.
pub(crate) async fn post_app_v1(
    state: &SchemaServiceState,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let (body_value, cert, sig) = match signed_json_body(event, app_metadata_body) {
        Ok(parts) => parts,
        Err(response) => return response,
    };
    let (status, body) = state
        .register_app_reply(cert, sig, &body_value)
        .await
        .into_http();
    json_response(status, &body)
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
    let Some(app_id) = known_app_id(p, "/v1/apps/") else {
        return json_response(400, &invalid_app_id_body());
    };
    public_app_response(state, app_id)
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
    let Some(app_id) = known_app_id(p, "/v1/apps/") else {
        return json_response(400, &invalid_app_id_body());
    };
    let (body_value, cert, sig) = match signed_json_body(event, app_metadata_body) {
        Ok(parts) => parts,
        Err(response) => return response,
    };
    let (status, body) = state
        .update_app_reply(app_id, cert, sig, &body_value)
        .await
        .into_http();
    json_response(status, &body)
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
    let Some(app_id) = segment_before(p, "/v1/apps/", "/promote") else {
        return json_response(400, &invalid_app_id_body());
    };
    let (cert, sig) = match v2_cert_headers(event) {
        Ok(pair) => pair,
        Err(r) => return r,
    };
    match state.promote_app_reply(app_id, cert, sig).await {
        PromoteAppReply::Ready { status, body } | PromoteAppReply::Failed { status, body } => {
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
pub(crate) async fn post_apps_releases(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some(app_id) = app_id_before(p, "/v2/apps/", "/releases") else {
        return json_response(400, &invalid_app_id_body());
    };
    let (body_value, cert, sig) = match signed_json_body(event, v2_body) {
        Ok(parts) => parts,
        Err(response) => return response,
    };
    match state
        .publish_release_reply(app_id, cert, sig, &body_value)
        .await
    {
        PublishReleaseReply::Ready {
            status,
            body,
            record,
        } => {
            // A repeat publish (same release id) is queued again too: it is
            // how a developer recovers a lost job, and the verifier skips
            // contract versions that already have a result.
            verifier_queue::send_publish_job(state, &record, cert, sig, &body_value).await;
            json_response(status, &body)
        }
        PublishReleaseReply::Failed { status, body } => json_response(status, &body),
    }
}

pub(crate) async fn post_apps_revocations(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some(app_id) = app_id_before(p, "/v2/apps/", "/revocations") else {
        return json_response(400, &invalid_app_id_body());
    };
    let (body_value, cert, sig) = match signed_json_body(event, v2_body) {
        Ok(parts) => parts,
        Err(response) => return response,
    };
    match state
        .revoke_release_reply(app_id, cert, sig, &body_value)
        .await
    {
        RevokeReleaseReply::Ready {
            status,
            body,
            record,
        } => {
            // The registry flag is not what a node reads; the verifier's
            // revoke entry in the release log is.
            verifier_queue::send_revoke_job(state, &record, cert, sig, &body_value).await;
            json_response(status, &body)
        }
        RevokeReleaseReply::Failed { status, body } => json_response(status, &body),
    }
}

pub(crate) async fn put_apps_channels(
    state: &SchemaServiceState,
    event: &Request,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some((app_id, channel)) = v2_channel_path(p) else {
        return json_response(400, &invalid_app_id_body());
    };
    let (body_value, cert, sig) = match signed_json_body(event, v2_body) {
        Ok(parts) => parts,
        Err(response) => return response,
    };
    let (status, body) = state
        .set_channel_reply(app_id, channel, cert, sig, &body_value)
        .await
        .into_http();
    json_response(status, &body)
}

// Anonymous read: the desired release id + the channel generation. Read from
// storage, not this instance's memory: another instance may have moved it.
pub(crate) async fn get_apps_channels(
    state: &SchemaServiceState,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some((app_id, channel)) = v2_channel_path(p) else {
        return json_response(400, &invalid_app_id_body());
    };
    let (status, body) = state.channel_read_reply(app_id, channel).await.into_http();
    json_response(status, &body)
}

// Anonymous read: one release manifest by its exact key, from storage.
pub(crate) async fn get_releases(
    state: &SchemaServiceState,
    p: &str,
) -> Result<Response<Body>, Error> {
    let Some(release_id) = segment_before(p, "/v2/releases/", "/") else {
        return json_response(400, &reason_only_body("invalid_release_id"));
    };
    let (status, body) = state.release_read_reply(release_id).await.into_http();
    json_response(status, &body)
}

// Anonymous read: the app record. Same body as `GET /v1/apps/{id}`.
pub(crate) fn get_app_v2(state: &SchemaServiceState, p: &str) -> Result<Response<Body>, Error> {
    let Some(app_id) = known_app_id(p, "/v2/apps/") else {
        return json_response(400, &invalid_app_id_body());
    };
    public_app_response(state, app_id)
}

fn known_app_id<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    segment_before(path, prefix, "/")
}

fn public_app_response(state: &SchemaServiceState, app_id: &str) -> Result<Response<Body>, Error> {
    let (status, body) = state.public_app_reply(app_id).into_http();
    json_response(status, &body)
}

// Anonymous read: list all apps (ordered by app_id).

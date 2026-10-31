//! Mutation challenge and app/release/channel registry routes.

use schema_service_core::app_release::{
    ChannelReadReply, PublishReleaseReply, ReleaseReadReply, RevokeReleaseReply, SetChannelReply,
};
use schema_service_core::{
    ListLiveAppsReply, PromoteAppReply, PublicAppReply, RegisterAppReply,
    SchemaMutationChallengeReply, UpdateAppReply,
};

use super::*;

#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn issue_schema_mutation_challenge(
    req: HttpRequest,
    payload: web::Json<SchemaMutationChallengeRequest>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    match state
        .issue_schema_mutation_challenge_reply(&payload.into_inner(), request_ip(&req).as_deref())
    {
        SchemaMutationChallengeReply::Created(response) => HttpResponse::Created().json(response),
        SchemaMutationChallengeReply::Failed { status, body } => json_status(status, body),
    }
}

/// Extract the app_identity v3.1 `X-Exemem-Dev-Cert` + `X-Signature` header
/// pair, returning a `401 cert_invalid` response when either is missing.
/// Shared by `register_app`, `update_app`, and `promote_app` so the missing-
/// header 401 body shape (`reason: "cert_invalid"`, `detail: "missing ..."`)
/// stays in lockstep across the three handlers.
fn require_dev_cert_headers(req: &HttpRequest) -> Result<(String, String), HttpResponse> {
    match (
        header_value(req, "X-Exemem-Dev-Cert"),
        header_value(req, "X-Signature"),
    ) {
        (Some(cert), Some(sig)) => Ok((cert, sig)),
        _ => Err(json_status(
            401,
            json!({ "reason": "cert_invalid", "detail": "missing X-Exemem-Dev-Cert or X-Signature header" }),
        )),
    }
}

/// `POST /v1/apps` — register an app namespace (app_identity v3.1, Lane B2b).
///
/// Gated by `X-Exemem-Dev-Cert` + `X-Signature` (purpose `app_register`).
/// The signed payload is the request body `{ app_id, metadata }`.
pub async fn register_app(
    req: HttpRequest,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let body = payload.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    match state.register_app_reply(&cert, &sig, &body).await {
        RegisterAppReply::Ready { status, body } | RegisterAppReply::Failed { status, body } => {
            json_status(status, body)
        }
    }
}

// ─── `/v2` app release registry ───────────────────────────────────────────
//
// Three writes, each gated by one DevCert + one `X-Signature` envelope with
// a per-route purpose, and three anonymous reads. The reads resolve one
// exact key each: the app id, the release id, and the (app id, channel)
// pair. Nothing here scans.

/// `POST /v2/apps/{app_id}/releases` — publish an immutable release.
///
/// The signed payload is the whole body `{ "manifest": … }`; the release id
/// is the SHA-256 of that manifest, so the developer signs exactly the bytes
/// the registry keys on.
pub async fn publish_release(
    req: HttpRequest,
    path: web::Path<String>,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let app_id = path.into_inner();
    let body = payload.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    match state
        .publish_release_reply(&app_id, &cert, &sig, &body)
        .await
    {
        PublishReleaseReply::Ready { status, body, .. }
        | PublishReleaseReply::Failed { status, body } => json_status(status, body),
    }
}

/// `PUT /v2/apps/{app_id}/channels/{channel}` — point a channel at a
/// release under a generation check. A stale generation is a 409.
pub async fn set_channel(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let (app_id, channel) = path.into_inner();
    let body = payload.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    match state
        .set_channel_reply(&app_id, &channel, &cert, &sig, &body)
        .await
    {
        SetChannelReply::Ready(body) => json_status(200, body),
        SetChannelReply::Failed { status, body } => json_status(status, body),
    }
}

/// `POST /v2/apps/{app_id}/revocations` — revoke a published release.
pub async fn revoke_release(
    req: HttpRequest,
    path: web::Path<String>,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let app_id = path.into_inner();
    let body = payload.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    match state
        .revoke_release_reply(&app_id, &cert, &sig, &body)
        .await
    {
        RevokeReleaseReply::Ready { status, body, .. }
        | RevokeReleaseReply::Failed { status, body } => json_status(status, body),
    }
}

/// `GET /v2/releases/{release_id}` — anonymous read of one release
/// manifest by its exact key, from storage.
pub async fn get_release(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let release_id = path.into_inner();
    match state.release_read_reply(&release_id).await {
        ReleaseReadReply::Ready(body) => json_status(200, body),
        ReleaseReadReply::Missing(body) => json_status(404, body),
        ReleaseReadReply::Unavailable(body) => json_status(503, body),
    }
}

/// `GET /v2/releases/{release_id}/verification` — anonymous redirect to the
/// release verifier's result for one release, under `RELEASE_LOG_URL`.
/// 503 `verifier_not_configured` when the variable is unset.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_release_verification(path: web::Path<String>) -> impl Responder {
    let release_id = path.into_inner();
    let release_log_url = std::env::var("RELEASE_LOG_URL").ok();
    match schema_service_core::app_release::release_verification_location(
        release_log_url.as_deref(),
        &release_id,
    ) {
        Ok(location) => HttpResponse::TemporaryRedirect()
            .insert_header((actix_web::http::header::LOCATION, location.clone()))
            .json(json!({ "location": location })),
        Err((status, body)) => json_status(status, body),
    }
}

/// `GET /v2/apps/{app_id}/channels/{channel}` — anonymous read of the
/// desired release id and the channel generation, from storage.
pub async fn get_channel(
    path: web::Path<(String, String)>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let (app_id, channel) = path.into_inner();
    match state.channel_read_reply(&app_id, &channel).await {
        ChannelReadReply::Ready(body) => json_status(200, body),
        ChannelReadReply::Missing(body) => json_status(404, body),
        ChannelReadReply::Unavailable(body) => json_status(503, body),
    }
}

/// `PUT /v1/apps/{app_id}` — owner-authenticated metadata update
/// (app_identity v3.1, `app_update`).
///
/// Gated by `X-Exemem-Dev-Cert` + `X-Signature` (purpose `app_update`).
/// The signed payload is `{ app_id, metadata }` — the path's `app_id`
/// folded into the body so the signature binds to the target namespace.
/// `display_name` is immutable (409 on change); the signer pubkey MUST
/// equal the registered `owner_dev_pubkey` (401 otherwise). Local to
/// this deployment env: dev and prod are independent registries since
/// the cross-env mirror was decommissioned in #517.
pub async fn update_app(
    req: HttpRequest,
    path: web::Path<String>,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let app_id = path.into_inner();
    let body = payload.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    match state.update_app_reply(&app_id, &cert, &sig, &body).await {
        UpdateAppReply::Ready { status, body } | UpdateAppReply::Failed { status, body } => {
            json_status(status, body)
        }
    }
}

/// `POST /v1/apps/{app_id}/promote` — promote a sandbox app to live
/// (app_identity v3.1). Gated by `X-Exemem-Dev-Cert` + `X-Signature`
/// (purpose `app_promote`). The signer must be the registered owner and
/// the cert must carry `authorized_publisher`. Idempotent — promoting an
/// already-live app is a 200 no-op.
pub async fn promote_app(
    req: HttpRequest,
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let app_id = path.into_inner();
    let (cert, sig) = match require_dev_cert_headers(&req) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    match state.promote_app_reply(&app_id, &cert, &sig).await {
        PromoteAppReply::Ready { status, body } | PromoteAppReply::Failed { status, body } => {
            json_status(status, body)
        }
    }
}

/// `GET /v1/apps/{app_id}` — public, lookup-by-known-id app registry read
/// (app_identity v3.1, Step 1 of `app_identity_node_as_verifier.md`).
///
/// Returns the registered [`AppRecord`] for `app_id` (including its `tier`),
/// with a derived `revoked` flag (true when `owner_dev_pubkey` is on the
/// schema service's offline revocation denylist). 404 when the app is
/// unknown.
///
/// Auth: **public**. Nodes use this on bootstrap to populate the local app
/// registry without holding a developer `em_` bearer — fold PR #445's
/// snapshot-fetch path forced the node to impersonate a developer to read
/// public verification material. Bulk browse is handled by `GET /v1/apps`,
/// which returns only promoted, non-revoked apps rather than the raw registry.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_app(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let app_id = path.into_inner();
    match state.public_app_reply(&app_id) {
        PublicAppReply::Ready(body) => json_status(200, body),
        PublicAppReply::Missing(body) => json_status(404, body),
    }
}

/// `GET /v1/apps` — public app shelf read.
///
/// Returns promoted, non-revoked apps only. Publishing and promotion remain
/// DevCert-gated, but browsing the visible shelf does not require a developer
/// credential.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn list_apps(state: web::Data<SchemaServiceState>) -> impl Responder {
    let ListLiveAppsReply { status, body } = state.list_live_apps_reply();
    json_status(status, body)
}

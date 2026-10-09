//! Mutation challenge and app/release/channel registry routes.

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
    match state.issue_schema_mutation_challenge(&payload.into_inner(), request_ip(&req).as_deref())
    {
        Ok(response) => HttpResponse::Created().json(response),
        Err(error) => {
            let (status, body) = error.to_http();
            json_status(status, body)
        }
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

    match state.register_app(&cert, &sig, &body).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http(&state.deployment_env_label());
            json_status(status, body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
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
    match state.publish_release(&app_id, &cert, &sig, &body).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http();
            json_status(status, body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
            json_status(status, body)
        }
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
        .set_channel(&app_id, &channel, &cert, &sig, &body)
        .await
    {
        Ok(record) => json_status(200, record.to_public_json()),
        Err(e) => {
            let (status, body) = e.to_http();
            json_status(status, body)
        }
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
    match state.revoke_release(&app_id, &cert, &sig, &body).await {
        Ok(record) => json_status(200, record.to_public_json()),
        Err(e) => {
            let (status, body) = e.to_http();
            json_status(status, body)
        }
    }
}

/// `GET /v2/releases/{release_id}` — anonymous read of one release
/// manifest by its exact key.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_release(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let release_id = path.into_inner();
    match state.get_release(&release_id) {
        Some(record) => json_status(200, record.to_public_json()),
        None => json_status(
            404,
            json!({ "reason": "unknown_release", "release_id": release_id }),
        ),
    }
}

/// `GET /v2/apps/{app_id}/channels/{channel}` — anonymous read of the
/// desired release id and the channel generation.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_channel(
    path: web::Path<(String, String)>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let (app_id, channel) = path.into_inner();
    match state.get_channel(&app_id, &channel) {
        Some(record) => json_status(200, record.to_public_json()),
        None => json_status(
            404,
            json!({ "reason": "unknown_channel", "app_id": app_id, "channel": channel }),
        ),
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

    match state.update_app(&app_id, &cert, &sig, &body).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http(&state.deployment_env_label());
            json_status(status, body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
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

    match state.promote_app(&app_id, &cert, &sig).await {
        Ok(outcome) => {
            let (status, body) = outcome.to_http();
            json_status(status, body)
        }
        Err(e) => {
            let (status, body) = e.to_http();
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
    let Some(record) = state.get_app(&app_id) else {
        return json_status(404, json!({ "reason": "unknown_app", "app_id": app_id }));
    };
    let revoked = state
        .app_identity_config()
        .revoked_dev_pubkeys
        .contains(&record.owner_dev_pubkey);
    // Field parity with `list_live_apps` and the Lambda GET-by-id arm:
    // install clients call this path and require `source` + SemVer `version`.
    json_status(
        200,
        json!({
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
    json_status(200, json!(state.list_live_apps()))
}

//! Declared-field routes.

use super::*;

/// `POST /v1/fields/declare` — declare a field handle and receive its scoped
/// identities (brain `design-lastdb-declared-fields`).
///
/// **Always gated** by `X-Exemem-Dev-Cert` + `X-Signature` (purpose
/// `field_declare`). The cert must permit publication and match the registered
/// app owner. New schema registrations use the same developer-account gate.
///
/// Idempotent: re-declaring the same `(owner_app_id, handle)` returns the same
/// declaration id and byte-identical identities, so a client may call it on
/// every startup.
pub async fn declare_field(
    req: HttpRequest,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let body = payload.into_inner();

    let request: schema_service_core::DeclareFieldRequest =
        match serde_json::from_value(body.clone()) {
            Ok(parsed) => parsed,
            Err(e) => {
                return json_status(
                    400,
                    json!({
                        "reason": "invalid_body",
                        "detail": format!("expected {{ owner_app_id, handle, fields[] }}: {e}"),
                    }),
                )
            }
        };

    // The signed payload is the whole request body, so the declaration's
    // owner, handle, and field list are all covered by the signature.
    let cert = header_value(&req, "X-Exemem-Dev-Cert");
    let sig = header_value(&req, "X-Signature");
    if let Err(e) = state
        .authorize_field_declare(
            &request.owner_app_id,
            cert.as_deref(),
            sig.as_deref(),
            &body,
        )
        .await
    {
        let (status, body) = e.to_http();
        return json_status(status, body);
    }

    match state.declare_field(&request).await {
        Ok(record) => HttpResponse::Ok().json(record.declare_json()),
        Err(e) => {
            let (status, body) = e.to_http();
            json_status(status, body)
        }
    }
}

/// `GET /v1/fields/{declaration_id}` — read one declaration.
///
/// Unauthenticated, like `GET /v1/apps/{app_id}`: a node needs to resolve a
/// declaration it already holds an id for, and the id is not a secret. What is
/// gated is *declaring* and *referencing*, not looking up.
#[allow(
    clippy::unused_async,
    reason = "actix-web route handler — signature must be async to satisfy the Handler trait"
)]
pub async fn get_declared_field(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let declaration_id = path.into_inner();
    match state.get_declared_field(&declaration_id) {
        Ok(record) => HttpResponse::Ok().json(record),
        Err(e) => {
            let (status, body) = e.to_http();
            json_status(status, body)
        }
    }
}

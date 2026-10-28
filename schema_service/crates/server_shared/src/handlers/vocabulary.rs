//! Standard vocabulary lookup and operator publication.
use super::*;

pub async fn publish_canonical_vocabulary(
    req: HttpRequest,
    payload: web::Json<Value>,
    state: web::Data<SchemaServiceState>,
) -> impl Responder {
    let signature = header_value(&req, "X-Canonical-Signature");
    match state
        .publish_canonical_vocabulary(signature.as_deref(), &payload.into_inner())
        .await
    {
        Ok(count) => HttpResponse::Ok().json(json!({"approved_fields": count})),
        Err(error) => json_status(
            error.status,
            json!({"reason": error.reason, "detail": error.detail}),
        ),
    }
}

pub async fn get_canonical_vocabulary(state: web::Data<SchemaServiceState>) -> impl Responder {
    match state.canonical_vocabulary().await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => internal_error("Cannot read canonical vocabulary", error),
    }
}

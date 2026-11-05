use super::{json_status, web, HttpResponse, SchemaServiceState};
use schema_service_core::portable_schema::PinPortableSchemaRequest;

pub async fn get_portable_schema(
    path: web::Path<String>,
    state: web::Data<SchemaServiceState>,
) -> HttpResponse {
    let (status, body) = state.portable_schema_read_reply(&path).await.into_http();
    json_status(status, body)
}

pub async fn pin_portable_schema(
    path: web::Path<String>,
    request: web::Json<PinPortableSchemaRequest>,
    state: web::Data<SchemaServiceState>,
) -> HttpResponse {
    let (status, body) = state
        .portable_schema_pin_reply(&path, &request)
        .await
        .into_http();
    json_status(status, body)
}

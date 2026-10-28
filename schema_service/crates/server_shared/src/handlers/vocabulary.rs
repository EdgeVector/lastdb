//! Public standard vocabulary lookup. Fields enter through normal declarations.
use super::*;

pub async fn get_canonical_vocabulary(state: web::Data<SchemaServiceState>) -> impl Responder {
    match state.canonical_vocabulary().await {
        Ok(value) => HttpResponse::Ok().json(value),
        Err(error) => internal_error("Cannot read canonical vocabulary", error),
    }
}

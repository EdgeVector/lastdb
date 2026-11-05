use crate::http::{json_response, typed_json_body};
use lambda_http::{Body, Error, Request, Response};
use schema_service_core::portable_schema::PinPortableSchemaRequest;
use schema_service_server_shared::state::SchemaServiceState;

pub(crate) async fn handle(
    state: &SchemaServiceState,
    event: &Request,
    id: &str,
) -> Result<Response<Body>, Error> {
    let reply = if event.method() == lambda_http::http::Method::PUT {
        let request: PinPortableSchemaRequest =
            match typed_json_body(event, "Invalid portable schema pin request") {
                Ok(request) => request,
                Err(response) => return response,
            };
        state.portable_schema_pin_reply(id, &request).await
    } else {
        state.portable_schema_read_reply(id).await
    };
    let (status, body) = reply.into_http();
    json_response(status, &body)
}

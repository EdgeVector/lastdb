//! Translate ordinary record mutations for a registered receiver into private submissions.
use super::{
    content_free, envelope, error_response, parse_mutation_body, AccessContext, UdsRequest,
    UdsResponse,
};
use crate::registered_share::RegisteredShare;
use crate::share_contribution::{submit_registered, SubmitError, WriteRequest};
use cloud_backup_protocol::{Digest, Id};
use fold_db::schema::types::{cas::CasExpectation, MutationType};
use lastdb_host::handlers::MutationComponents;

const MAX_WIRE_BYTES: usize = 16 * 1024;

pub(super) async fn mutate(
    req: &UdsRequest,
    ctx: &AccessContext,
    share: &RegisteredShare,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(
            403,
            "registered share mutations require the owner socket",
            ctx,
        );
    }
    let Some(grant) = share.contributor_grant() else {
        return error_response(403, "this share is read-only", ctx);
    };
    if req.body.len() > MAX_WIRE_BYTES {
        return error_response(413, "share mutation exceeds its size limit", ctx);
    }
    let Some(id) = mutation_id(req) else {
        return error_response(
            400,
            "share mutation requires one UUID Idempotency-Key header",
            ctx,
        );
    };
    let components = match parse_mutation_body(&req.body) {
        Ok(components) => components,
        Err(reject) => return reject.response(),
    };
    if !contribution_supported(&components) {
        return error_response(
            422,
            "share contributions support an absent create, or a replace or delete at an expected revision, without files, metadata, or synchronous convergence",
            ctx,
        );
    }
    let Some(definition_id) = components
        .definition_id
        .and_then(|id| Digest::try_from(id).ok())
    else {
        return error_response(400, "share mutation requires a valid definition_id", ctx);
    };
    let request = WriteRequest {
        mutation_id: id.clone(),
        schema: components.schema,
        definition_id,
        action: components.mutation_type,
        key: components.key_value,
        record: components.record_fields.into_iter().collect(),
        alternates: components.alternates,
        revision: match &components.expected {
            CasExpectation::Revision { revision } => Some(revision.clone()),
            CasExpectation::Absent => None,
        },
    };
    let sequence = match submit_registered(
        share.identity_home(),
        grant,
        share.publisher(),
        share.stream(),
        share.environment(),
        request,
    )
    .await
    {
        Ok(sequence) => sequence,
        Err(error) => return submission_error(&error, ctx),
    };
    let response = envelope(
        &serde_json::json!({
            "mutation_id": id,
            "submission_sequence": sequence,
            "status": "submitted",
        }),
        ctx.user_id.as_str(),
    );
    match serde_json::to_vec(&response) {
        Ok(bytes) => {
            UdsResponse::new(202, "Accepted", bytes).with_header("Content-Type", "application/json")
        }
        Err(_) => content_free(500, "Internal Server Error"),
    }
}

fn contribution_supported(components: &MutationComponents) -> bool {
    if components.source_file_name.is_some()
        || components.metadata.is_some()
        || components.file.is_some()
        || components.file_write.is_some()
        || components.inline_write.is_some()
        || components.convergence.waits()
        || (components.mutation_type == MutationType::Delete
            && !components.record_fields.is_empty())
    {
        return false;
    }
    match (&components.mutation_type, &components.expected) {
        (MutationType::Create, CasExpectation::Absent) => true,
        (MutationType::Replace | MutationType::Delete, CasExpectation::Revision { revision })
            if !revision.is_empty() =>
        {
            true
        }
        _ => false,
    }
}

fn mutation_id(req: &UdsRequest) -> Option<Id> {
    let mut headers = req
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("Idempotency-Key"));
    let (_, value) = headers.next()?;
    if headers.next().is_some() {
        return None;
    }
    Id::try_from(value.to_owned()).ok()
}

fn submission_error(error: &SubmitError, ctx: &AccessContext) -> UdsResponse {
    match error {
        SubmitError::Invalid(_) | SubmitError::Forbidden => error_response(
            403,
            "contributor append permission is invalid, expired, or refused",
            ctx,
        ),
        SubmitError::TooLarge => {
            error_response(413, "share contribution exceeds its size limit", ctx)
        }
        SubmitError::Conflict(message) => error_response(409, message, ctx),
        SubmitError::Unavailable(_) => error_response(
            503,
            "share submission is unavailable; retry with the same Idempotency-Key and body",
            ctx,
        ),
    }
}

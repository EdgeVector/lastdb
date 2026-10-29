use super::*;

/// `POST /api/apps/shared-surface/publish-attach` — explicit shared publish
/// or attach via the live schema-service facade.
///
/// The install-owned `schema_resolver.json` controls the request timeout.
/// Private declare routes never call this path.
pub async fn execute_apps_shared_surface_publish_attach_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use schema_service_client::{validate_shared_surface_request_pub, SharedSurfacePublishRequest};

    let body: SharedSurfacePublishRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/shared-surface/publish-attach body: {e}"),
                ctx,
            )
        }
    };

    if let Err(e) = crate::shared_surface::validate_publish_body(
        &body.request.local_schema_id,
        &body.descriptive_name,
        &body.fields,
    ) {
        return error_response(400, &e, ctx);
    }
    if let Err(e) = validate_shared_surface_request_pub(&body.request) {
        return error_response(400, &e, ctx);
    }

    let url = folddb_profile::endpoints::schema_service_url();
    let result =
        match crate::schema_resolver_host::publish_attach_with_host_config(&host.home, &url, body)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return error_response(
                    502,
                    &format!("shared-surface publish/attach failed: {e}"),
                    ctx,
                )
            }
        };

    // Persist attachment locally for audit (atomic write under node home).
    let path = crate::shared_surface::SharedSurfaceAttachmentStore::path_for_home(&host.home);
    let mut store = match crate::shared_surface::SharedSurfaceAttachmentStore::load(&path) {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                500,
                &format!("failed to load shared-surface attachments: {e}"),
                ctx,
            )
        }
    };
    store.upsert(result.attachment.clone());
    if let Err(e) = store.save_atomic(&path) {
        return error_response(
            500,
            &format!("failed to persist shared-surface attachment: {e}"),
            ctx,
        );
    }

    json_ok(&envelope(
        &serde_json::to_value(&result).unwrap_or_default(),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/apps/shared-surface/attachments` — list local attachment records.
#[allow(
    clippy::unused_async,
    reason = "route handlers share the async UdsResponse signature with sibling routes"
)]
pub async fn execute_apps_shared_surface_attachments_route(
    _req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let path = crate::shared_surface::SharedSurfaceAttachmentStore::path_for_home(&host.home);
    let store = match crate::shared_surface::SharedSurfaceAttachmentStore::load(&path) {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                500,
                &format!("failed to load shared-surface attachments: {e}"),
                ctx,
            )
        }
    };
    let payload = serde_json::json!({ "attachments": store.list() });
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// `POST /api/apps/verify-distribution-ready` — confirm every required schema
/// identity exists on Schema Service before packaging/sharing an app.
pub async fn execute_apps_verify_distribution_ready_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    _host: &Host,
) -> UdsResponse {
    use crate::distribution::{
        distribution_ready, DistributionReadyItem, DistributionReadyStatus,
        VerifyDistributionReadyRequest, VerifyDistributionReadyResponse,
    };

    let body: VerifyDistributionReadyRequest = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/apps/verify-distribution-ready body: {e}"),
                ctx,
            )
        }
    };

    let app_id = body.app_id.trim().to_string();
    if app_id.is_empty() {
        return error_response(400, "app_id must be non-empty", ctx);
    }
    if body.schema_identities.is_empty() {
        return error_response(400, "schema_identities must be a non-empty array", ctx);
    }

    let url = folddb_profile::endpoints::schema_service_url();
    let client = schema_service_client::SchemaServiceClient::new(&url);
    let mut items = Vec::with_capacity(body.schema_identities.len());

    for identity in body.schema_identities {
        let id = identity.trim().to_string();
        if id.is_empty() {
            items.push(DistributionReadyItem {
                identity,
                status: DistributionReadyStatus::Error,
                error: Some("empty identity".into()),
            });
            continue;
        }
        match client.get_schema(&id).await {
            Ok(_) => items.push(DistributionReadyItem {
                identity: id,
                status: DistributionReadyStatus::Present,
                error: None,
            }),
            Err(e) => {
                let msg = e.to_string();
                let missing = msg.contains("404") || msg.to_ascii_lowercase().contains("not found");
                items.push(DistributionReadyItem {
                    identity: id,
                    status: if missing {
                        DistributionReadyStatus::Missing
                    } else {
                        DistributionReadyStatus::Error
                    },
                    error: Some(msg),
                });
            }
        }
    }

    let ready = distribution_ready(&items);
    let payload = VerifyDistributionReadyResponse {
        app_id,
        items,
        ready,
    };
    json_ok(&envelope(
        &serde_json::to_value(payload).unwrap_or_default(),
        ctx.user_id.as_str(),
    ))
}

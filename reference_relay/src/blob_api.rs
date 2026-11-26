//! `blob.put` and `blob.get` handlers.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use relay_protocol::{
    BlobGet, BlobGetAnswer, BlobPut, BlobPutAnswer, BlobPutRequest, BlobPutStage, Endpoint, Upload,
};
use serde_json::Value;

use crate::auth;
use crate::blob_store::checksum_header;
use crate::error::{ApiError, ApiResult};
use crate::gate;
use crate::handlers::{authenticate, parse_body, AppState};
use crate::presign;
use crate::store::{now_ms, require_hex64};

pub(crate) async fn blob_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<BlobPutAnswer>> {
    let verified = auth::verify_request(&headers, BlobPut::ROUTE.path(), &body)?;
    let (req, stage) = parse_blob_put(&body)?;
    verified.check_database(&req.database_id)?;
    require_hex64(&req.database_id, "database_id")?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_blob_put(&db, &caller).await?;
    require_hex64(&req.sha256, "sha256")?;
    if req.bytes == 0 {
        return Err(ApiError::invalid("bytes"));
    }
    match stage {
        Some(BlobPutStage::Begin) => blob_begin(&state, &req).await,
        Some(BlobPutStage::Commit) => {
            let _blob_guard = state.blob_lock.lock().await;
            state
                .store
                .stage_commit(&req.database_id, &req.sha256, req.bytes)
                .await?;
            Ok(Json(BlobPutAnswer {
                stage: BlobPutStage::Commit,
                sha256: req.sha256,
                upload: None,
            }))
        }
        None => Err(ApiError::invalid("stage")),
    }
}

async fn blob_begin(state: &AppState, req: &BlobPutRequest) -> ApiResult<Json<BlobPutAnswer>> {
    if state.max_blob_bytes.is_some_and(|limit| req.bytes > limit) {
        return Err(ApiError::invalid("bytes"));
    }
    let token = {
        let _blob_guard = state.blob_lock.lock().await;
        if state
            .store
            .has_blob(&req.database_id, &req.sha256, req.bytes)
            .await?
        {
            return Ok(Json(BlobPutAnswer {
                stage: BlobPutStage::Begin,
                sha256: req.sha256.clone(),
                upload: None,
            }));
        }
        state
            .store
            .stage_begin(&req.database_id, &req.sha256, req.bytes)
            .await?
    };
    let (url, expires_at_ms) = presign::make_put_url(
        &state.public_base,
        &state.secret,
        &req.database_id,
        &token,
        now_ms(),
    );
    let headers = BTreeMap::from([
        (
            "content-type".to_owned(),
            "application/octet-stream".to_owned(),
        ),
        (
            "x-amz-checksum-sha256".to_owned(),
            checksum_header(&req.sha256),
        ),
    ]);
    Ok(Json(BlobPutAnswer {
        stage: BlobPutStage::Begin,
        sha256: req.sha256.clone(),
        upload: Some(Upload {
            url,
            headers,
            expires_at_ms,
        }),
    }))
}

pub(crate) async fn blob_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<BlobGetAnswer>> {
    let (verified, req) = authenticate::<BlobGet>(&headers, &body)?;
    require_hex64(&req.database_id, "database_id")?;
    let caller = verified.node_key;
    let db = gate::parse_id(&req.database_id, "database_id")?;
    state.check_read(&db, &caller).await?;
    require_hex64(&req.sha256, "sha256")?;
    let (bytes, _) = {
        let _blob_guard = state.blob_lock.lock().await;
        state.store.blob_meta(&req.database_id, &req.sha256).await?
    };
    if bytes == 0 {
        return Err(ApiError::not_found("blob.sha256"));
    }
    let (url, expires_at_ms) = presign::make_get_url(
        &state.public_base,
        &state.secret,
        &req.database_id,
        &req.sha256,
        now_ms(),
    );
    Ok(Json(BlobGetAnswer {
        url,
        headers: BTreeMap::new(),
        expires_at_ms,
        bytes,
        sha256: req.sha256,
    }))
}

/// A `blob.put` body and its stage. A `stage` string other than `begin` or `commit` parses with
/// no stage, so it is refused as field `stage` after the checks before it.
fn parse_blob_put(body: &[u8]) -> ApiResult<(BlobPutRequest, Option<BlobPutStage>)> {
    if let Ok(req) = serde_json::from_slice::<BlobPutRequest>(body) {
        let stage = Some(req.stage);
        return Ok((req, stage));
    }
    let mut value: Value = parse_body(body)?;
    let Some(stage) = value.get_mut("stage").filter(|stage| stage.is_string()) else {
        return Err(ApiError::invalid("body"));
    };
    *stage = Value::from("begin");
    let req = serde_json::from_value(value).map_err(|_| ApiError::invalid("body"))?;
    Ok((req, None))
}

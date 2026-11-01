use super::*;

pub(super) fn execute_auto_identity_route(host: &Host) -> UdsResponse {
    let public_key = host.public_key();
    let body = serde_json::json!({
        "user_id": public_key,
        "user_hash": host.user_hash,
        "public_key": public_key,
    });
    json_ok(&body)
}

pub(super) fn execute_boot_identity_route(host: &Host) -> UdsResponse {
    // This must be THIS process's own captured identity, never a fresh read
    // of the shared per-home ledger file: a different process sharing this
    // home can append a newer row after this process booted, and the old
    // `read_last_record(&host.home)` here would then serve that foreign
    // pid/build as the live daemon's own identity
    // (papercut-lastdb-primary-boot-identity-stale-phantom-pid-20260927).
    let Some(boot) = host.own_boot_identity.get() else {
        // A missing boot row is a build failure for Canary Pipeline v2. Keep
        // the response content-free: only the owner-side observer needs the
        // named evidence, and a jailed app cannot reach this route at all.
        return content_free(503, "Service Unavailable");
    };
    let body = serde_json::json!({
        "pid": boot.pid,
        "process_start_ts": boot.start_ts,
        "build": boot.build_version,
        "restart_cause": boot.restart_cause,
    });
    json_ok(&body)
}

pub(super) fn execute_boot_ledger_route(host: &Host) -> UdsResponse {
    let boots: Vec<_> = crate::session_ledger::read_recent_records(
        &host.home,
        crate::session_ledger::RECENT_RECORD_LIMIT,
    )
    .into_iter()
    .map(|boot| {
        serde_json::json!({
            "pid": boot.pid,
            "process_start_ts": boot.start_ts,
            "build": boot.build_version,
            "restart_cause": boot.restart_cause,
        })
    })
    .collect();
    // A missing ledger is canary build evidence. The empty response must not
    // masquerade as a healthy absence.
    if boots.is_empty() {
        return content_free(503, "Service Unavailable");
    }
    json_ok(&serde_json::json!({ "boots": boots }))
}

/// `GET /api/native-index/search` — owner-wide index search over live schemas.
pub(super) fn execute_native_index_search_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let raw_term = query_value(&req.target, "q").or_else(|| query_value(&req.target, "term"));
    let term = match raw_term {
        Some(t) if !t.trim().is_empty() => t.trim().to_string(),
        _ => return Reject::missing(WireKey::Query).response(),
    };

    let include_internal = query_flag(&req.target, "include_internal");
    let exact = query_flag(&req.target, "exact");
    let Ok(min_score) = parse_min_score(query_value(&req.target, "min_score").as_deref()) else {
        return Reject::invalid(WireKey::MinScore).response();
    };
    let schemas: Option<Vec<String>> = query_value(&req.target, "schemas").and_then(|raw| {
        let mut seen = HashSet::new();
        let parsed: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .filter(|s| seen.insert(s.clone()))
            .collect();
        (!parsed.is_empty()).then_some(parsed)
    });

    render(
        handlers::native_index_search(
            host,
            NativeSearchParams {
                term,
                include_internal,
                exact,
                min_score,
                schemas,
            },
            ctx,
        ),
        ctx,
    )
}

/// `GET /api/search/query` — Search app text query scoped to Search-owned
/// schemas.
pub(super) fn execute_search_app_query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let raw_term = query_value(&req.target, "q").or_else(|| query_value(&req.target, "term"));
    let term = match raw_term {
        Some(t) if !t.trim().is_empty() => t.trim().to_string(),
        _ => return content_free(400, "Bad Request"),
    };

    let include_internal = query_flag(&req.target, "include_internal");
    let exact = query_flag(&req.target, "exact");
    let Ok(min_score) = parse_min_score(query_value(&req.target, "min_score").as_deref()) else {
        return Reject::invalid(WireKey::MinScore).response();
    };
    let schemas: Option<Vec<String>> = query_value(&req.target, "schemas").and_then(|raw| {
        let mut seen = HashSet::new();
        let parsed: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .filter(|s| seen.insert(s.clone()))
            .collect();
        (!parsed.is_empty()).then_some(parsed)
    });

    render(
        handlers::search_app_query(
            host,
            NativeSearchParams {
                term,
                include_internal,
                exact,
                min_score,
                schemas,
            },
            ctx,
        ),
        ctx,
    )
}

/// `POST /api/native-index/embeddings` — report the retired native index.
pub(super) async fn execute_native_index_app_vector_put_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Ok(request) = serde_json::from_slice::<handlers::AppVectorPutRequest>(&req.body) else {
        return content_free(400, "Bad Request");
    };
    render(
        handlers::native_index_put_app_vector(host, request, ctx),
        ctx,
    )
}

/// `POST /api/native-index/knn` — report the retired native index.
pub(super) async fn execute_native_index_knn_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Ok(request) = serde_json::from_slice::<handlers::KnnRequest>(&req.body) else {
        return content_free(400, "Bad Request");
    };
    render(handlers::native_index_knn(host, request), ctx)
}

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

pub(super) async fn execute_molecule_history_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(molecule_uuid) = path_tail(&req.target, "/api/history/") else {
        return content_free(400, "Bad Request");
    };
    let scope = HistoryScope {
        hash: query_value(&req.target, "hash"),
        range: query_value(&req.target, "range"),
    };
    render(
        handlers::molecule_history(host, &molecule_uuid, &scope).await,
        ctx,
    )
}

pub(super) async fn execute_atom_content_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(atom_uuid) = path_tail(&req.target, "/api/atom/") else {
        return content_free(400, "Bad Request");
    };
    render(handlers::atom_content(host, &atom_uuid).await, ctx)
}

// ---------------------------------------------------------------------------
// Protein — multi-key molecule coherence (design-lastdb-protein-molecule-set)
// ---------------------------------------------------------------------------

/// Status for a protein-route failure.
///
/// These routes used to map **every** `SchemaError` to `400`, including store
/// and IO failures wrapped as `InvalidData`. A `400` tells a client its request
/// was malformed and must not be retried, so a transient failure was reported as
/// a permanent client error and the caller dropped the work. Route through the
/// canonical [`HostError`] mapping instead, with one addition: "already bound to
/// protein X" is a state conflict (`409`), not a malformed request.
///
/// Only the read routes remain, so the conflict arm is now unreachable in
/// practice; it is kept because the mapping, not the caller, is what was wrong.
pub(super) fn protein_error(err: fold_db::schema::types::SchemaError) -> HostError {
    if let fold_db::schema::types::SchemaError::InvalidData(msg) = &err {
        if msg.contains("already bound to protein ") {
            return HostError::new(409, msg.clone());
        }
    }
    HostError::from(err)
}

pub(super) async fn execute_protein_get_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(uuid) = path_tail(&req.target, "/api/protein/") else {
        return content_free(400, "Bad Request");
    };
    let result = host
        .db
        .db_ops()
        .atoms()
        .protein_get(&uuid)
        .await
        // Its sibling `execute_protein_of_molecule_route` already maps through
        // `protein_error`; this arm kept a bare 500, so a caller-supplied uuid
        // the store rejected was reported as a server fault.
        .map_err(protein_error)
        .and_then(|opt| {
            opt.map(|p| {
                serde_json::json!({
                    "uuid": p.uuid,
                    "members": p.members,
                    "schema": fold_db::PROTEIN_SCHEMA_MARKER,
                })
            })
            .ok_or_else(|| HostError::new(404, format!("protein '{uuid}' not found")))
        });
    render(result, ctx)
}

/// `GET /api/protein/of-molecule/{molecule_uuid}` — the protein this molecule is
/// bound to, or `null`.
///
/// A pure read of the `molprot:` back-ref, kept for introspection: it answers
/// "did the node bind this molecule, and to what" without writing anything.
///
/// Unbound is `200` with `protein_uuid: null`, not `404`: a molecule with no
/// sibling layout is simply unbound, which is an answer rather than an error.
pub(super) async fn execute_protein_of_molecule_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(molecule_uuid) = path_tail(&req.target, "/api/protein/of-molecule/") else {
        return content_free(400, "Bad Request");
    };
    let result = host
        .db
        .db_ops()
        .atoms()
        .protein_of_molecule(&molecule_uuid)
        .await
        .map_err(protein_error)
        .map(|found| {
            serde_json::json!({
                "molecule_uuid": molecule_uuid,
                "protein_uuid": found,
                "schema": fold_db::PROTEIN_SCHEMA_MARKER,
            })
        });
    render(result, ctx)
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

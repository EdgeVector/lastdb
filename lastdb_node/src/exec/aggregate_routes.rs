use super::*;

pub(super) fn take_aggregate_set(
    obj: &mut serde_json::Map<String, Value>,
) -> Result<Option<fold_db::schema::types::AggregateSet>, ()> {
    match obj.remove("aggregate_set") {
        Some(value) => serde_json::from_value(value).map(Some).map_err(|_| ()),
        None => Ok(None),
    }
}

pub(super) fn parse_aggregate_finalize(
    body: &[u8],
) -> Result<fold_db::schema::types::AggregateFinalize, ()> {
    serde_json::from_slice(body).map_err(|_| ())
}

pub(super) fn parse_aggregate_repair(
    body: &[u8],
) -> Result<fold_db::schema::types::AggregateRepair, ()> {
    serde_json::from_slice(body).map_err(|_| ())
}

pub(super) async fn execute_aggregate_repair_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let Ok(repair) = parse_aggregate_repair(&req.body) else {
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::Parse,
            parse_started.elapsed(),
        );
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/repair",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate repair request refused on shape"
        );
        return Reject::new(RejectKind::InvalidValue).response();
    };
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    render(
        handlers::execute_aggregate_repair(host, repair, ctx).await,
        ctx,
    )
}

pub(super) async fn execute_aggregate_finalize_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let Ok(finalize) = parse_aggregate_finalize(&req.body) else {
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::Parse,
            parse_started.elapsed(),
        );
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/finalize",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate finalize request refused on shape"
        );
        return Reject::new(RejectKind::InvalidValue).response();
    };
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    render(
        handlers::execute_aggregate_finalize(host, finalize, ctx).await,
        ctx,
    )
}

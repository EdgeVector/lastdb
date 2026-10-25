use super::*;
use fold_db::schema::types::{AggregateFinalize, AggregateRepair};

pub(super) async fn execute_aggregate_repair_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let Ok(repair) = serde_json::from_slice::<AggregateRepair>(&req.body) else {
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
    let Ok(finalize) = serde_json::from_slice::<AggregateFinalize>(&req.body) else {
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

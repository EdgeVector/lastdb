use super::*;
use fold_db::schema::types::{AggregateFinalize, AggregateRepair};
use serde::de::DeserializeOwned;

pub(super) async fn execute_aggregate_repair_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let repair = match parse_aggregate_request::<AggregateRepair>(&req.body, || {
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/repair",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate repair request refused on shape"
        );
    }) {
        Ok(repair) => repair,
        Err(response) => return response,
    };
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
    let finalize = match parse_aggregate_request::<AggregateFinalize>(&req.body, || {
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/finalize",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate finalize request refused on shape"
        );
    }) {
        Ok(finalize) => finalize,
        Err(response) => return response,
    };
    render(
        handlers::execute_aggregate_finalize(host, finalize, ctx).await,
        ctx,
    )
}

/// Parse one aggregate body. A bad shape records the parse phase, logs through
/// `refuse`, and returns the same invalid-value refusal.
fn parse_aggregate_request<T>(body: &[u8], refuse: impl FnOnce()) -> Result<T, UdsResponse>
where
    T: DeserializeOwned,
{
    let parse_started = std::time::Instant::now();
    let parsed = serde_json::from_slice::<T>(body);
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    parsed.map_err(|_| {
        refuse();
        Reject::new(RejectKind::InvalidValue).response()
    })
}

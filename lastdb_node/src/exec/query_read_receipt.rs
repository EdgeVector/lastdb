//! Opt-in query cost receipts. No record values or paths enter a receipt.

use super::*;

mod rules;
pub(super) use rules::batch_results;

pub(super) async fn render_query(
    req: &UdsRequest,
    ctx: &AccessContext,
    work: impl std::future::Future<Output = Result<Value, HostError>>,
) -> UdsResponse {
    if !req
        .header("x-lastdb-read-receipt")
        .is_some_and(env_flag::truthy)
    {
        return render(work.await, ctx);
    }
    let started = std::time::Instant::now();
    let (result, receipt) = fold_db::read_receipts::observe(Box::pin(work)).await;
    let elapsed = started.elapsed();
    let socket_queue_wait = super::current_socket_queue_wait();
    let mut receipt = rules::receipt_json(receipt, elapsed, socket_queue_wait);
    let response = render(result, ctx);
    receipt["returned_body_bytes"] = Value::from(response.body.len());
    response.with_header("x-lastdb-read-receipt", receipt.to_string())
}

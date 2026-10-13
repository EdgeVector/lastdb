use futures::stream::StreamExt;

/// Run a bounded-concurrency, order-preserving fetch → sequential-consume
/// pipeline over `items`.
///
/// This is the ordering/abort core shared by the bootstrap replay loop: the
/// per-item `fetch` futures (download + unseal — the network/crypto legs) run
/// up to `cap` at a time, but `buffered(cap)` yields their results in **input
/// order**, and `consume` is awaited strictly sequentially on each result in
/// that order. The first `Err` from either `fetch` or `consume` aborts the
/// whole pipeline and is returned — so a caller that advances a cursor only
/// after this returns `Ok` can never advance past an un-consumed item, even
/// though downloads complete out of order. Memory is bounded: at most `cap`
/// in-flight fetch results exist at once.
///
/// `cap` is clamped to `>= 1` (0 would make `buffered` deadlock-free but never
/// poll — treat it as fully serial).
pub(crate) async fn ordered_concurrent_fetch<I, T, Fetch, FetchFut, Consume, ConsumeFut, E>(
    items: I,
    cap: usize,
    fetch: Fetch,
    mut consume: Consume,
) -> Result<(), E>
where
    I: IntoIterator,
    Fetch: Fn(I::Item) -> FetchFut,
    FetchFut: std::future::Future<Output = Result<T, E>>,
    Consume: FnMut(T) -> ConsumeFut,
    ConsumeFut: std::future::Future<Output = Result<(), E>>,
{
    let cap = cap.max(1);
    let mut stream = futures::stream::iter(items.into_iter().map(fetch)).buffered(cap);
    while let Some(res) = stream.next().await {
        let item = res?;
        consume(item).await?;
    }
    Ok(())
}

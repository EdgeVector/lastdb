//! One stderr line per request: route, status and, for `log/read`, how many
//! entries the answer carried. It never prints a payload, key or signature, so a
//! proof can read it to check what a sync did (for example that an idle sync
//! read no entries).

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Set on a `log/read` response by its handler: the entries in the page.
#[derive(Clone, Copy)]
pub(crate) struct EntriesReturned(pub(crate) usize);

/// `route=log/read status=200 entries=3`, or without `entries=` for other routes.
pub(crate) async fn log_request(request: Request, next: Next) -> Response {
    let route = route_of(request.uri().path());
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16();
    // An upload says how long it took on a line of its own (`relay upload_ms=N`), so a proof can tell
    // one long upload from many short ones. The ordinary line keeps its form.
    if route == "blob/upload" && status == 200 {
        eprintln!("relay upload_ms={}", started.elapsed().as_millis());
    }
    match response.extensions().get::<EntriesReturned>() {
        Some(EntriesReturned(n)) => eprintln!("relay route={route} status={status} entries={n}"),
        None => eprintln!("relay route={route} status={status}"),
    }
    response
}

/// The first two segments after `/relay/v1/`, so a token or digest is not logged.
pub(crate) fn route_of(path: &str) -> String {
    let rest = path.strip_prefix("/relay/v1/").unwrap_or(path);
    rest.split('/').take(2).collect::<Vec<_>>().join("/")
}

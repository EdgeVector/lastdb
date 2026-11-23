//! `--refuse-first`: answer the first request of a route with a retryable 429, for the retry proof.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::request_log::route_of;

pub(crate) struct Refusals {
    /// Route to the field of the refusal it still owes.
    owed: Mutex<HashMap<String, String>>,
    /// Nonces seen: a replayed nonce is 401, as on the Exemem relay, which consumes the nonce of a
    /// request that it then refuses.
    nonces: Mutex<HashSet<String>>,
}

impl Refusals {
    pub(crate) fn parse(specs: &[String]) -> Result<Option<Arc<Self>>, String> {
        if specs.is_empty() {
            return Ok(None);
        }
        let mut owed = HashMap::new();
        for spec in specs {
            let (route, field) = spec
                .split_once('=')
                .filter(|(route, field)| !route.is_empty() && !field.is_empty())
                .ok_or_else(|| format!("--refuse-first {spec:?} is not ROUTE=FIELD"))?;
            owed.insert(route.to_owned(), field.to_owned());
        }
        Ok(Some(Arc::new(Self {
            owed: Mutex::new(owed),
            nonces: Mutex::new(HashSet::new()),
        })))
    }
}

pub(crate) async fn refuse(
    State(refusals): State<Arc<Refusals>>,
    request: Request,
    next: Next,
) -> Response {
    let nonce = request
        .headers()
        .get("x-lastdb-nonce")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(nonce) = nonce {
        let fresh = refusals
            .nonces
            .lock()
            .map_or(true, |mut seen| seen.insert(nonce));
        if !fresh {
            return ApiError::unauthorized("nonce").into_response();
        }
    }
    let field = refusals
        .owed
        .lock()
        .ok()
        .and_then(|mut owed| owed.remove(&route_of(request.uri().path())));
    match field {
        Some(field) => ApiError::rate_limited(field).into_response(),
        None => next.run(request).await,
    }
}

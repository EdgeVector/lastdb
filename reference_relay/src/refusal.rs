//! `--refuse-first`: answer the first authenticated request of a route with a retryable 429.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use mutation_log::NodeKey;
use relay_protocol::headers;

use crate::auth;
use crate::error::ApiError;
use crate::request_log::route_of;

const MAX_NONCES: usize = 16_384;

#[derive(Default)]
struct SeenNonces {
    order: VecDeque<(NodeKey, String)>,
    set: HashSet<(NodeKey, String)>,
}

impl SeenNonces {
    fn insert(&mut self, key: (NodeKey, String)) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.set.len() >= MAX_NONCES {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.set.insert(key)
    }
}

pub(crate) struct Refusals {
    /// Route to the field of the refusal it still owes.
    owed: Mutex<HashMap<String, String>>,
    /// Nonces seen on authenticated requests: a replayed nonce is 401, as on the Exemem relay.
    nonces: Mutex<SeenNonces>,
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
            nonces: Mutex::new(SeenNonces::default()),
        })))
    }
}

pub(crate) async fn refuse(
    State(refusals): State<Arc<Refusals>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let Ok(bytes) = to_bytes(body, crate::PLATFORM_PAYLOAD_MAX_BYTES).await else {
        return ApiError::invalid("body").into_response();
    };
    let caller = match auth::verify_request(&parts.headers, parts.uri.path(), &bytes) {
        Ok(caller) => caller,
        Err(err) => return err.into_response(),
    };
    let nonce = parts
        .headers
        .get(headers::NONCE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(nonce) = nonce {
        let fresh = refusals
            .nonces
            .lock()
            .map_or(true, |mut seen| seen.insert((caller.node_key, nonce)));
        if !fresh {
            return ApiError::unauthorized("nonce").into_response();
        }
    }
    let field = refusals
        .owed
        .lock()
        .ok()
        .and_then(|mut owed| owed.remove(&route_of(parts.uri.path())));
    if let Some(field) = field {
        return ApiError::rate_limited(field).into_response();
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

//! Request router for the LastDB Mini Unix-domain socket.

use fold_db::access::AccessContext;
use std::sync::OnceLock;

use super::uds_http::{UdsRequest, UdsResponse};

const API_VERSION_PATH: &str = "/api/version";

/// The request-grammar version this node speaks (`GET /api/version`).
pub const API_VERSION: u32 = 34;

static BUILD_VERSION: OnceLock<&'static str> = OnceLock::new();

/// Install the baked build string once at startup.
pub fn set_build_version(build: &'static str) {
    let _ = BUILD_VERSION.set(build);
}

/// The build string the version route reports (`"unknown"` until installed).
#[must_use]
pub fn build_version() -> &'static str {
    BUILD_VERSION.get().copied().unwrap_or("unknown")
}

mod data_route;
pub use data_route::DataRoute;

mod responses;
pub use responses::*;

mod route_table;

/// Classification of a control-socket request before any handler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    /// Liveness probe (`GET /health`) — answerable without node state.
    Health,
    /// Version handshake (`GET /api/version`).
    Version,
    /// A recognized data route, executed against node state by the dispatcher.
    Data(DataRoute),
    /// The target path is recognized, but not for this method.
    MethodNotAllowed {
        /// Methods the path supports, sent as the `Allow` header.
        allow: &'static str,
    },
    /// No recognized control-socket route matches the target path.
    NotFound,
}

fn path_only(target: &str) -> &str {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    &target[..end]
}

/// Classify a parsed control-socket request into a [`ControlRoute`].
pub fn route(req: &UdsRequest) -> ControlRoute {
    let path = path_only(&req.target);
    let method = req.method.as_str();

    if let Some(route) = route_special(path, method) {
        return route;
    }
    if let Some(route) = route_exact(path, method) {
        return route;
    }
    route_prefixed(path, method).unwrap_or(ControlRoute::NotFound)
}

fn single_method(method: &str, allow: &'static str, route: ControlRoute) -> ControlRoute {
    if method == allow {
        route
    } else {
        ControlRoute::MethodNotAllowed { allow }
    }
}

fn route_special(path: &str, method: &str) -> Option<ControlRoute> {
    let route = match path {
        "/health" => single_method(method, "GET", ControlRoute::Health),
        API_VERSION_PATH => single_method(method, "GET", ControlRoute::Version),
        "/api/db/native-status" => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbNativeStatus),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/cleanup" => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbCleanup),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        _ => return None,
    };
    Some(route)
}

fn route_exact(path: &str, method: &str) -> Option<ControlRoute> {
    let row = route_table::EXACT_ROUTES
        .iter()
        .find(|row| row.path == path)?;
    Some(single_method(
        method,
        row.method,
        ControlRoute::Data(row.route),
    ))
}

fn one_segment<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    path.strip_prefix(prefix)
        .filter(|rest| !rest.is_empty() && !rest.contains('/'))
}

fn route_prefixed(path: &str, method: &str) -> Option<ControlRoute> {
    if one_segment(path, "/api/schema/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::GetSchema),
        ));
    }
    None
}

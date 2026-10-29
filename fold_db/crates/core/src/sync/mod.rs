//! V2 account byte transport and local mutation admission.
//! No legacy cloud engine, provider admission, replay worker or scheduler exists.
pub mod auth;

const SHARED_HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Protected HTTP construction for explicit account and V2 byte operations.
pub fn build_shared_http_client() -> Result<reqwest::Client, reqwest::Error> {
    // trace-egress: propagate — AuthClient injects W3C on its service requests.
    // trace-egress: skip-s3 — byte transport preserves signed provider request headers.
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(SHARED_HTTP_CONNECT_TIMEOUT)
        .build()
}

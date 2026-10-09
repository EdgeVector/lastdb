//! Browser origins that may call the schema service HTTP server.

const ALLOWED_ORIGINS: &[&str] = &[
    "https://exemem.com",
    "https://www.exemem.com",
    "https://folddb.com",
    "https://www.folddb.com",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:8080",
];

pub(crate) fn is_allowed_origin(origin: &str) -> bool {
    ALLOWED_ORIGINS.contains(&origin)
}

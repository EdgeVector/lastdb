//! `folddb_profile` — the shared `$FOLDDB_HOME` env-profile loader, the
//! cross-environment endpoint registry, and the `FOLDDB_HOME`/tilde path
//! helpers.
//!
//! This is a deliberately small **leaf crate** (deps: `serde`, `toml`,
//! `thiserror`, `dirs`, `tracing`) so two consumers that must NOT depend on
//! each other can both resolve the active env profile:
//!
//! - `fold_db_node` (the production node + the `folddb` developer CLI)
//!   re-exports `endpoints`, `utils::paths`, and
//!   `app_identity_client::profile` from here for full back-compat.
//! - `fold_db_node::dev_mode` (the `folddb dev` dev/test node) uses this crate
//!   so its publish verbs (`schema/view/app publish`) can fall back to the
//!   profile `folddb login` wrote — flag → `EXEMEM_DEV_API_KEY` → profile —
//!   without routing through the production node surface.
//!
//! The store-aware API-key resolver (`resolve_api_key_with_store`) stays in
//! `fold_db_node` because it needs the heavy `DeveloperKeyStore` seam; this
//! crate exposes only the pure flag → env → profile resolution
//! ([`profile::resolve_api_key`]) plus the dev-node override resolvers in
//! [`resolver`].

pub mod endpoints;
pub mod paths;
pub mod profile;
pub mod resolver;

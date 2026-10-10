//! Profile-fallback resolvers for the `folddb dev` publish verbs.
//!
//! After `folddb login` writes the active env profile to
//! `$FOLDDB_HOME/config.toml`, a developer should not have to repeat
//! `--dev-api-key` / `--schema-service-url` (or export `EXEMEM_DEV_API_KEY`)
//! on every `folddb dev schema|view|app publish`. These helpers layer the
//! profile in as the *last* fallback, preserving the override order:
//!
//! - **API key:** flag → `EXEMEM_DEV_API_KEY` (or the profile's
//!   `api_key_env`) → the active-env profile's recorded key → error.
//! - **schema_service URL:** flag → env → the active-env profile's
//!   `schema_service_url` override → `None` (the caller keeps its own
//!   built-in `DEFAULT_SCHEMA_SERVICE_URL`).
//!
//! A missing or partial profile is **not** an error — it simply yields no
//! override, mirroring the fold-side `Profile::default()` behavior (a
//! brand-new machine looks like the default profile). The only hard error is
//! the *unresolved API key* case, and its message is actionable: it points
//! the developer at `folddb login` and the flag/env fallbacks.
//!
//! Note on the API key: the profile stores only the key's env-var *name* and
//! a 6-char display tail, never the secret (see [`crate::profile`]). So the
//! profile fallback for the API key recovers a key only when the env var the
//! profile names is actually set in the developer's shell. The full secret an
//! invited developer obtained at `login` lives in `fold_db_node`'s secure
//! `DeveloperKeyStore`, which the dev node does not consult (it has its own,
//! separate, out-of-scope keypair store). In practice `folddb login` exports
//! / records `EXEMEM_DEV_API_KEY`, so flag → env covers the logged-in
//! developer; the profile layer here makes the *URL* and the env-var-name
//! override stick without re-typing.

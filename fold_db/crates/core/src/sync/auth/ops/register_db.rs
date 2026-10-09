//! Owner claim for a database-rooted cloud storage prefix.
//!
//! Cloud object keys root at `{db_hash}/…` (design-cloud-sync-prefix-db-hash).
//! The storage service authorizes every db_hash-scoped request against a
//! principal → db_hash registry, and a home that has never registered gets a
//! hard 403 on its very first scoped call — so `lastdb cloud on` "succeeds" and
//! then every sync cycle fails until an operator hand-POSTs `register_db`.
//!
//! [`AuthClient::register_db`] is that POST, and [`AuthClient::post`] drives it
//! automatically the first time a scoped request comes back unregistered.

use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};

/// The storage service's 403 message when the authenticated principal has no
/// registry row for the requested `db_hash`.
///
/// **Cross-service contract.** This literal is produced by
/// `exemem_service/lambdas/storage_service/src/db_registry.rs`
/// (`authorize_db_entry` → `ApiError::forbidden`). `ApiError` response bodies
/// carry `code`/`error`/`statusCode` but no machine-readable `reason`, and
/// `code` is the generic `FORBIDDEN` shared by every denial in that service —
/// so the message text is the only signal that distinguishes "this database is
/// unclaimed" from "you are not allowed here". Matching prose across a service
/// boundary is normally a smell; it is deliberate here because the heal has to
/// work against the *already deployed* Lambda, which no client-side change can
/// re-shape. `db_registry.rs` carries a canary test pinning the literal, so a
/// future edit fails there rather than silently disarming this recovery.
pub const DB_HASH_NOT_REGISTERED: &str = "principal is not registered for this db_hash";

/// Registry membership returned by a successful `register_db`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DbRegistration {
    pub db_hash: String,
    pub principal_hash: String,
    pub role: String,
    #[serde(default)]
    pub key_locator: Option<String>,
}

impl AuthClient {
    /// Claim ownership of this client's `db_hash` for the authenticated
    /// principal.
    ///
    /// Idempotent: the service returns the existing membership when this
    /// principal is already owner/writer/reader. It refuses — and this returns
    /// an error — when a *different* principal already owns the database, so
    /// the claim can never take a root out from under another account.
    ///
    /// Deliberately posts through [`AuthClient::post_no_default_db_hash`] with
    /// an explicit `db_hash`: the claim must not re-enter the unregistered-403
    /// recovery in [`AuthClient::post`], which is what calls it.
    pub async fn register_db(&self) -> SyncResult<DbRegistration> {
        let Some(db_hash) = self.db_hash_scope() else {
            return Err(SyncError::Storage(
                "register_db: client has no db_hash scope".to_string(),
            ));
        };

        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "register_db",
                    "db_hash": db_hash,
                }),
            )
            .await?;

        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some("register_db failed".to_string()));
            return Err(op_failed("register_db", detail));
        }

        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("register_db response decode failed: {e}"))
        })
    }

    /// Claim ownership of an explicit head id (personal `db_hash` or org identity
    /// hash used as the cloud prefix). Used when arming org sync so the principal
    /// is on the registry before org_hash-scoped presigns.
    pub async fn register_db_for_hash(&self, db_hash: &str) -> SyncResult<DbRegistration> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "register_db",
                    "db_hash": db_hash,
                }),
            )
            .await?;

        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some("register_db failed".to_string()));
            return Err(op_failed("register_db", detail));
        }

        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("register_db response decode failed: {e}"))
        })
    }

    /// Owner grants `writer`/`reader` to another principal on a cloud head.
    pub async fn register_db_member(
        &self,
        db_hash: &str,
        target_user_hash: &str,
        role: &str,
    ) -> SyncResult<DbRegistration> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "register_db_member",
                    "db_hash": db_hash,
                    "target_user_hash": target_user_hash,
                    "role": role,
                }),
            )
            .await?;

        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("register_db_member failed");
            return Err(SyncError::Storage(format!("register_db_member: {detail}")));
        }

        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("register_db_member response decode failed: {e}"))
        })
    }

    /// Owner revokes `target_user_hash`, or the caller leaves when target is self.
    pub async fn unregister_db_member(
        &self,
        db_hash: &str,
        target_user_hash: &str,
    ) -> SyncResult<()> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "unregister_db_member",
                    "db_hash": db_hash,
                    "target_user_hash": target_user_hash,
                }),
            )
            .await?;

        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unregister_db_member failed");
            return Err(SyncError::Storage(format!(
                "unregister_db_member: {detail}"
            )));
        }
        Ok(())
    }
}

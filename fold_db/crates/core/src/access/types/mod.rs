//! Request identity types for local callers.
//!
//! Enforcement of trust tiers / capabilities / payment is **not** performed
//! on local query/mutation paths. These types remain for host/node request
//! identity (`AccessContext`, transport, verification).
//!
//! The live local gate is OS peer-cred in [`super::peer_cred`].

use serde::{Deserialize, Serialize};

/// Transport channel a request arrived on (identity / audit metadata only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CallerTransport {
    /// TCP loopback — no OS peer identity.
    #[default]
    LoopbackTcp,
    /// Unix-domain socket — OS peer credentials available.
    UnixSocket,
    /// Owner desktop UI (Tauri).
    Tauri,
    /// Owner browser session after pairing ceremony.
    BrowserSession,
    /// In-process call.
    InProcess,
    /// Message from another node (self-asserted identity only).
    RemoteNode,
}

impl CallerTransport {
    /// Whether this transport can attest the node owner for isolation UX.
    /// Local query/mutation paths do not use this as an ACL gate.
    pub const fn permits_owner_isolation_bypass(self) -> bool {
        match self {
            Self::Tauri | Self::BrowserSession | Self::UnixSocket | Self::InProcess => true,
            Self::LoopbackTcp | Self::RemoteNode => false,
        }
    }
}

/// Whether the caller's app/node identity has been verified at the host edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CallerVerification {
    #[default]
    Unverified,
    CodeSignatureVerified {
        app_id: String,
    },
    /// Peer node identity accepted by the host (no relationship ladder).
    RemoteIdentityVerified {
        node_pubkey: String,
    },
}

/// Context for a local request — identity plumbing, not an ACL evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessContext {
    /// Who is making the request (public key or user identifier).
    pub user_id: String,
    /// Whether this is the data owner.
    pub is_owner: bool,
    /// Transport this request arrived on.
    #[serde(default)]
    pub transport: CallerTransport,
    /// Host-accepted identity verification, if any.
    #[serde(default)]
    pub verification: CallerVerification,
    /// Kernel-reported peer pid on the UDS connection, when available.
    ///
    /// Ops attribution only (`lastdb ops` / request telemetry) — not an ACL
    /// input. `None` for in-process / non-UDS callers, or when the platform
    /// did not report a pid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_pid: Option<i32>,
    /// Canonical DB locator this request targets (`lastdb://personal`,
    /// `lastdb://org/…`, or `lastdb://db/<64-hex>`). `None` means personal.
    ///
    /// Set from `X-LastDB-Db` at the UDS edge. Apps must not invent this —
    /// org fills handles; the SDK forwards them; Mini scopes storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_locator: Option<String>,
    /// Local storage prefix (64-hex `db_hash`) for molecule keys under this
    /// request. `None` = personal unprefixed home. Derived from
    /// [`Self::db_locator`] via [`super::db_handle::storage_prefix_for`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<String>,
}

impl AccessContext {
    /// Create an owner context.
    pub fn owner(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            is_owner: true,
            transport: CallerTransport::default(),
            verification: CallerVerification::default(),
            peer_pid: None,
            db_locator: None,
            storage_prefix: None,
        }
    }

    /// Create a non-owner context (app / remote / jailed principal).
    pub fn remote(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            is_owner: false,
            transport: CallerTransport::default(),
            verification: CallerVerification::default(),
            peer_pid: None,
            db_locator: None,
            storage_prefix: None,
        }
    }

    /// Record the transport this request arrived on (builder-style).
    pub fn with_transport(mut self, transport: CallerTransport) -> Self {
        self.transport = transport;
        self
    }

    /// Record host-accepted app/node identity (builder-style).
    pub fn with_verification(mut self, verification: CallerVerification) -> Self {
        self.verification = verification;
        self
    }

    /// Record the UDS peer pid for ops attribution (builder-style).
    pub fn with_peer_pid(mut self, peer_pid: Option<i32>) -> Self {
        self.peer_pid = peer_pid;
        self
    }

    /// Attach a client DB handle (`X-LastDB-Db` value). Invalid locators error.
    ///
    /// `None` / empty → personal (clears any prior prefix). Prefer calling once
    /// at the UDS edge after peer-cred identity is stamped.
    pub fn with_db_handle(mut self, raw: Option<&str>) -> Result<Self, String> {
        let (canonical, prefix) = super::db_handle::resolve_db_handle_header(raw)?;
        if prefix.is_none() {
            self.db_locator = None;
            self.storage_prefix = None;
        } else {
            self.db_locator = Some(canonical);
            self.storage_prefix = prefix;
        }
        Ok(self)
    }

    /// Direct storage-prefix injection (tests / in-process callers). Prefer
    /// [`Self::with_db_handle`] on the request path.
    pub fn with_storage_prefix(mut self, storage_prefix: Option<String>) -> Self {
        self.storage_prefix = storage_prefix;
        self
    }

    /// Whether the caller has an accepted local app identity.
    pub fn is_verified(&self) -> bool {
        matches!(
            self.verification,
            CallerVerification::CodeSignatureVerified { .. }
        )
    }

    /// The accepted local app identifier, if any.
    pub fn verified_app_id(&self) -> Option<&str> {
        match &self.verification {
            CallerVerification::CodeSignatureVerified { app_id } => Some(app_id.as_str()),
            CallerVerification::Unverified | CallerVerification::RemoteIdentityVerified { .. } => {
                None
            }
        }
    }
}

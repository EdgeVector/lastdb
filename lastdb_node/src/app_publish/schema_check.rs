//! Schema coverage checks and novel schema registration.

use super::*;

/// Catalog coverage for one manifest schema, per the Mini declare route.
#[derive(Debug, Clone)]
pub enum SchemaCoverage {
    /// One catalog schema covers the proposal.
    Reuse { identity_hash: String },
    /// Multiple catalog components cover the proposal (Mini-local compose).
    Compose { components: Vec<String> },
    /// Mini registered or expanded one catalog identity and returned the
    /// audit proof required before an app can bind it.
    Register {
        identity_hash: String,
        audit_event_id: String,
        expanded: bool,
    },
    /// Read-only check only: a sync would register or expand this schema.
    /// Nothing was written, so this is not a bind proof. `identity_hash` is
    /// the proposal identity; Schema Service can store a different canonical.
    WouldRegister {
        identity_hash: String,
        expanded: bool,
        reason: Option<String>,
    },
    /// Not covered by the shared catalog (`local_mint`, or the node refused
    /// the declare). Publishing MUST reject these.
    Novel { reason: String },
}

impl SchemaCoverage {
    pub fn is_covered(&self) -> bool {
        !matches!(self, Self::Novel { .. })
    }
}

/// Which declare-schema intent `check_schemas_with` sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckMode {
    /// Intent `check`: resolve and validate only. The node writes nothing.
    ReadOnly,
    /// Intent `catalog_sync`: the node can register or expand schemas, bind
    /// them, and write schema-sync audit events.
    Sync,
}

impl CheckMode {
    fn intent(self) -> &'static str {
        match self {
            Self::ReadOnly => "check",
            Self::Sync => "catalog_sync",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckOutcome {
    pub schema_name: String,
    pub coverage: SchemaCoverage,
    /// The `retention` object the node returned for a schema whose manifest
    /// declares a TTL: the policy a sync would apply (read-only check), or the
    /// policy it applied (sync). `None` when the manifest declares no TTL.
    pub retention: Option<Value>,
}

/// Declare every manifest schema on the local node and classify coverage.
///
/// [`CheckMode::ReadOnly`] changes nothing on the node. [`CheckMode::Sync`]
/// can register, expand, and bind schemas and writes audit events.
///
/// `confirm_expire_existing` is the caller's confirmation that records which
/// already exist may expire (`--confirm-expire-existing`). The node refuses a
/// TTL that would delete existing records without it.
pub fn check_schemas_with(
    socket: &Path,
    manifest: &AppManifest,
    mode: CheckMode,
    confirm_expire_existing: bool,
) -> Result<Vec<CheckOutcome>, String> {
    let user_hash = socket_auto_identity(socket)?;
    let mut outcomes = Vec::with_capacity(manifest.schemas.len());
    for schema in &manifest.schemas {
        let name = schema
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        let body = declare_body(&manifest.app_id, schema, mode, confirm_expire_existing);
        let (status, payload) =
            socket_post_json(socket, "/api/apps/declare-schema", &user_hash, &body)?;
        let mut coverage = match mode {
            CheckMode::ReadOnly => classify_check_response(status, &payload),
            CheckMode::Sync => classify_declare_response(status, &payload),
        };
        // A TTL the node would refuse fails the check like any refused plan.
        if let Some(reason) = payload
            .get("retention")
            .and_then(|retention| retention.get("blocked"))
            .and_then(Value::as_str)
        {
            coverage = SchemaCoverage::Novel {
                reason: format!("retention refused: {reason}"),
            };
        }
        outcomes.push(CheckOutcome {
            schema_name: name,
            coverage,
            retention: payload.get("retention").cloned(),
        });
    }
    Ok(outcomes)
}

/// The declare body for one manifest schema entry.
///
/// The manifest `retention` key moves out of the schema and beside it: it is
/// node-local policy, so it must never reach the schema artifact that is
/// hashed, registered and shared.
fn declare_body(app_id: &str, schema: &Value, mode: CheckMode, confirm: bool) -> Value {
    let mut schema = schema.clone();
    let mut retention = schema
        .as_object_mut()
        .and_then(|entry| entry.remove("retention"));
    if let Some(entry) = retention.as_mut().filter(|_| confirm) {
        entry["confirm_expire_existing"] = Value::Bool(true);
    }
    let mut body = json!({
        "app_id": app_id,
        "schema": schema,
        "intent": mode.intent(),
    });
    if let Some(retention) = retention {
        body["retention"] = retention;
    }
    body
}

/// Manifest schemas that resolved to one catalog schema, as `(catalog hash,
/// schema names)`. They share one dataset: a record written through one name is
/// a record of the others, and a TTL on one reaches all of them.
pub fn catalog_collisions(outcomes: &[CheckOutcome]) -> Vec<(String, Vec<String>)> {
    let mut by_hash: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for outcome in outcomes {
        let (SchemaCoverage::Reuse {
            identity_hash: hash,
        }
        | SchemaCoverage::Register {
            identity_hash: hash,
            ..
        }
        | SchemaCoverage::WouldRegister {
            identity_hash: hash,
            ..
        }) = &outcome.coverage
        else {
            continue;
        };
        by_hash
            .entry(hash.clone())
            .or_default()
            .push(outcome.schema_name.clone());
    }
    by_hash
        .into_iter()
        .filter(|(_, names)| names.len() > 1)
        .collect()
}

/// Classify a read-only `check` declare response. It carries no audit event
/// and is never bind-eligible; a `register` / `expand` plan is reported as
/// [`SchemaCoverage::WouldRegister`].
pub(super) fn classify_check_response(status: u16, payload: &Value) -> SchemaCoverage {
    if status != 200 {
        return classify_declare_response(status, payload);
    }
    if payload.get("dry_run").and_then(Value::as_bool) != Some(true) {
        return SchemaCoverage::Novel {
            reason: "node did not honour the read-only check intent (no dry_run=true); \
                     upgrade lastdbd or pass --sync"
                .into(),
        };
    }
    let resolution = payload
        .get("resolution")
        .and_then(Value::as_str)
        .unwrap_or("");
    let identity_hash = payload
        .get("identity_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    match (resolution, identity_hash) {
        ("reuse", Some(identity_hash)) => SchemaCoverage::Reuse { identity_hash },
        ("compose", _) => {
            let components: Vec<String> = payload
                .get("component_catalog_hashes")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            if components.is_empty() {
                SchemaCoverage::Novel {
                    reason: "compose result lacks component catalog hashes".into(),
                }
            } else {
                SchemaCoverage::Compose { components }
            }
        }
        ("register" | "expand", Some(identity_hash)) => SchemaCoverage::WouldRegister {
            identity_hash,
            expanded: resolution == "expand",
            reason: payload
                .get("register_reason")
                .and_then(Value::as_str)
                .map(str::to_string),
        },
        (other, _) => SchemaCoverage::Novel {
            reason: format!("check returned resolution '{other}' without an identity"),
        },
    }
}

pub(super) fn classify_declare_response(status: u16, payload: &Value) -> SchemaCoverage {
    if status == 200 {
        let resolution = payload
            .get("resolution")
            .and_then(Value::as_str)
            .unwrap_or("");
        let audit_event_id = payload
            .get("audit_event_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        if payload.get("bind_eligible").and_then(Value::as_bool) != Some(true)
            || audit_event_id.is_none()
        {
            return SchemaCoverage::Novel {
                reason: "declare result lacks bind_eligible=true and audit_event_id".into(),
            };
        }
        let audit_event_id = audit_event_id.unwrap_or_default().to_string();
        match resolution {
            "reuse" => match payload
                .get("identity_hash")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                Some(identity_hash) => SchemaCoverage::Reuse {
                    identity_hash: identity_hash.to_string(),
                },
                None => SchemaCoverage::Novel {
                    reason: "reuse result lacks identity_hash".into(),
                },
            },
            "compose" => {
                let components: Vec<String> = payload
                    .get("component_catalog_hashes")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                if components.is_empty() {
                    SchemaCoverage::Novel {
                        reason: "compose result lacks component catalog hashes".into(),
                    }
                } else {
                    SchemaCoverage::Compose { components }
                }
            }
            "register" | "expand" => match payload
                .get("identity_hash")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                Some(identity_hash) => SchemaCoverage::Register {
                    identity_hash: identity_hash.to_string(),
                    audit_event_id,
                    expanded: resolution == "expand",
                },
                None => SchemaCoverage::Novel {
                    reason: format!("{resolution} result lacks identity_hash"),
                },
            },
            "local_mint" => SchemaCoverage::Novel {
                reason: "not covered by the shared catalog (local_mint)".into(),
            },
            other => SchemaCoverage::Novel {
                reason: format!("unrecognized declare resolution '{other}'"),
            },
        }
    } else {
        SchemaCoverage::Novel {
            reason: format!(
                "declare returned {status}: {}",
                payload
                    .get("error")
                    .or_else(|| payload.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("cannot resolve against the catalog")
            ),
        }
    }
}

// ─── Step 2: register novel schemas with the schema service ───────────────

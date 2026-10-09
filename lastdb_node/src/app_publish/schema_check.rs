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

/// Which declare-schema intent `check_schemas` sends.
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
}

/// Declare every manifest schema on the local node and classify coverage.
///
/// [`CheckMode::ReadOnly`] changes nothing on the node. [`CheckMode::Sync`]
/// can register, expand, and bind schemas and writes audit events.
pub fn check_schemas(
    socket: &Path,
    manifest: &AppManifest,
    mode: CheckMode,
) -> Result<Vec<CheckOutcome>, String> {
    let user_hash = socket_auto_identity(socket)?;
    let mut outcomes = Vec::with_capacity(manifest.schemas.len());
    for schema in &manifest.schemas {
        let name = schema
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        let body = json!({
            "app_id": manifest.app_id,
            "schema": schema,
            "intent": mode.intent(),
        });
        let (status, payload) =
            socket_post_json(socket, "/api/apps/declare-schema", &user_hash, &body)?;
        let coverage = match mode {
            CheckMode::ReadOnly => classify_check_response(status, &payload),
            CheckMode::Sync => classify_declare_response(status, &payload),
        };
        outcomes.push(CheckOutcome {
            schema_name: name,
            coverage,
        });
    }
    Ok(outcomes)
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

/// Register the named still-novel manifest schemas with the schema service as
/// catalog-visible, DevCert-backed shared-surface entries, normalized the same
/// way the Mini declare route normalizes proposals. Returns the identity
/// hashes registered.
pub async fn register_novel_schemas(
    schema_service_url: &str,
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
    env: Env,
    manifest: &AppManifest,
    novel_names: &[String],
) -> Result<Vec<RegisteredSchema>, String> {
    let cert = mint_dev_cert(exemem_api_url, api_key, dev_key).await?;
    let cert_b64 = BASE64
        .encode(serde_json::to_vec(&cert).map_err(|e| format!("failed to encode dev cert: {e}"))?);
    let client =
        SchemaServiceClient::new(schema_service_url).with_node_identity(dev_key.clone(), env);
    let mut registered = Vec::new();
    for raw in &manifest.schemas {
        let name = raw
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        if !novel_names.contains(&name) {
            continue;
        }
        // Pre-validate before any network call: the service rejects novel
        // registrations whose fields lack semantic descriptions, and the
        // server-side 400 is far less actionable than naming the fields here.
        let missing = missing_field_descriptions(raw);
        if !missing.is_empty() {
            return Err(format!(
                "schema '{name}': novel registration requires a field_descriptions entry \
                 for every field — missing: {}. Add them to the manifest and re-run.",
                missing.join(", ")
            ));
        }
        let schema = normalized_schema(&manifest.app_id, raw)?;
        let identity_hash = schema
            .get_identity_hash()
            .cloned()
            .ok_or_else(|| format!("schema '{name}': identity hash missing after normalize"))?;
        let service_schema = ServiceSchema::from(&schema);
        let response = client
            .add_schema_with_shared_surface_dev_cert(
                &service_schema,
                HashMap::new(),
                app_schema_shared_surface(manifest, &schema, &name, &identity_hash),
                "app_register_schemas",
                Some("lastdb app register-schemas"),
                DevSchemaClaim {
                    cert_b64: &cert_b64,
                    dev_key,
                    env,
                },
            )
            .await
            .map_err(|e| format!("schema '{name}': registration failed: {e}"))?;
        // The STORED identity is the catalog truth. The service may fold the
        // proposal into an expanded/merged canonical whose hash differs from
        // the locally computed one — coverage checks must use this value.
        let stored_identity = response
            .schema
            .get_identity_hash()
            .cloned()
            .unwrap_or_else(|| response.schema.name.clone());
        registered.push(RegisteredSchema {
            schema_name: name,
            local_identity: identity_hash,
            stored_identity,
        });
    }
    Ok(registered)
}

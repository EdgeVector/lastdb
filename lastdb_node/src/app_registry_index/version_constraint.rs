//! LastDB version constraint parsing and matching.
//!
//! Supports simple semver ranges in manifests, e.g. `"lastdb": ">=0.23.0, <0.24.0"`.
//! Constraints are comma-separated predicates: `>=X.Y.Z`, `<X.Y.Z`, `=X.Y.Z`.
//!
//! The running LastDB build string (e.g. `0.23.4-pre.0-324-g0ef44ca87`) is
//! parsed as SemVer core + pre-release. Constraints match against the core
//! version only (major.minor.patch), ignoring pre-release and build metadata.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl Version {
    /// Parse "X.Y.Z" from the start of a build string.
    ///
    /// Accepts:
    /// - `0.23.4`
    /// - `0.23.4-pre.0-324-g0ef44ca87`
    /// - `0.23.3-2378-gbe41e547e`
    pub fn parse(s: &str) -> Result<Self, String> {
        let core = s.split(&['-', '+'][..]).next().unwrap_or(s);
        let parts: Vec<&str> = core.split('.').collect();
        if parts.len() < 3 {
            return Err(format!("version must be X.Y.Z, got '{s}'"));
        }
        let major = parts[0]
            .parse::<u64>()
            .map_err(|_| format!("invalid major version in '{s}'"))?;
        let minor = parts[1]
            .parse::<u64>()
            .map_err(|_| format!("invalid minor version in '{s}'"))?;
        let patch = parts[2]
            .parse::<u64>()
            .map_err(|_| format!("invalid patch version in '{s}'"))?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Eq,
    Gte,
    Lt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    pub op: Op,
    pub version: Version,
}

impl Predicate {
    /// Parse one predicate: `>=X.Y.Z`, `<X.Y.Z`, or `=X.Y.Z`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (op, version) = if let Some(rest) = s.strip_prefix(">=") {
            (Op::Gte, rest.trim())
        } else if let Some(rest) = s.strip_prefix('<') {
            (Op::Lt, rest.trim())
        } else if let Some(rest) = s.strip_prefix('=') {
            (Op::Eq, rest.trim())
        } else {
            // Bare version means exact match.
            (Op::Eq, s)
        };
        Ok(Self {
            op,
            version: Version::parse(version)?,
        })
    }

    /// Does `version` satisfy this predicate?
    pub fn matches(&self, version: &Version) -> bool {
        match self.op {
            Op::Eq => version == &self.version,
            Op::Gte => version >= &self.version,
            Op::Lt => version < &self.version,
        }
    }
}

/// A constraint is one or more comma-separated predicates (all must match).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    pub predicates: Vec<Predicate>,
}

/// Check a contract version (api or outbox) against the node's supported range.
fn check_contract_version(
    version_u64: u64,
    min_supported: u32,
    current: u32,
    version_name: &str,
    app_id: &str,
) -> Result<(), String> {
    let version = u32::try_from(version_u64)
        .map_err(|_| format!("app '{app_id}' {version_name} {version_u64} is not a valid u32"))?;
    if version < min_supported || version > current {
        return Err(format!(
            "app '{app_id}' requires {version_name} {version}, but this node supports {min_supported}-{current} (current: {current})"
        ));
    }
    Ok(())
}

/// Check a JSON manifest for version constraints and contract versions.
fn check_json_manifest(
    json: &serde_json::Value,
    app_id: &str,
    lastdb_version: &str,
) -> Result<bool, String> {
    use crate::version;
    let mut has_constraint_or_version = false;

    if let Some(constraint_str) = json.get("lastdb").and_then(|v| v.as_str()) {
        has_constraint_or_version = true;
        check_constraint(app_id, constraint_str, lastdb_version)?;
    }

    if let Some(api_version_u64) = json.get("api_version").and_then(serde_json::Value::as_u64) {
        has_constraint_or_version = true;
        check_contract_version(
            api_version_u64,
            version::MIN_SUPPORTED_API_VERSION,
            version::CURRENT_API_VERSION,
            "api_version",
            app_id,
        )?;
    }

    if let Some(outbox_version_u64) = json
        .get("outbox_format_version")
        .and_then(serde_json::Value::as_u64)
    {
        has_constraint_or_version = true;
        check_contract_version(
            outbox_version_u64,
            version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
            version::CURRENT_OUTBOX_FORMAT_VERSION,
            "outbox_format_version",
            app_id,
        )?;
    }

    Ok(has_constraint_or_version)
}

/// Check a TOML manifest for a lastdb version constraint.
fn check_toml_manifest(raw: &str, app_id: &str, lastdb_version: &str) -> Result<bool, String> {
    for line in raw.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("lastdb") {
            let rest = rest.trim();
            if let Some(rest) = rest.strip_prefix('=') {
                let rest = rest.trim().trim_matches('"').trim_matches('\'');
                if !rest.is_empty() {
                    check_constraint(app_id, rest, lastdb_version)?;
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// Check if an installed app's manifest declares a lastdb version constraint,
/// and verify the running LastDB build satisfies it. Also checks contract version
/// compatibility (api_version and outbox_format_version) when present in the manifest.
///
/// Looks for common manifest files: `lastdb-app.json`, `folddb.toml`, `*.app.json`.
/// Checks every manifest that has a constraint or version field. If no manifest is
/// found or no constraint is declared, this is a no-op (apps without constraints
/// install unconditionally).
///
/// # Errors
/// Returns an error if a constraint is found but not satisfied, or if contract versions
/// are incompatible with the running node.
pub fn check_manifest_constraint(
    checkout_path: &std::path::Path,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(), String> {
    let manifest_candidates = [
        checkout_path.join("lastdb-app.json"),
        checkout_path.join("folddb.toml"),
    ];
    let app_json = checkout_path.join(format!("{app_id}.app.json"));

    for path in manifest_candidates.iter().chain(std::iter::once(&app_json)) {
        if !path.exists() {
            continue;
        }
        let Ok(raw) = std::fs::read_to_string(path) else {
            continue;
        };

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
            if check_json_manifest(&json, app_id, lastdb_version)? {
                continue;
            }
        }

        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            check_toml_manifest(&raw, app_id, lastdb_version)?;
        }
    }

    Ok(())
}

impl Constraint {
    /// Parse a constraint string, e.g. `">=0.23.0, <0.24.0"`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let predicates = s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(Predicate::parse)
            .collect::<Result<Vec<_>, _>>()?;
        if predicates.is_empty() {
            return Err("constraint must not be empty".into());
        }
        Ok(Self { predicates })
    }

    /// Does `version` satisfy all predicates in this constraint?
    pub fn matches(&self, version: &Version) -> bool {
        self.predicates.iter().all(|p| p.matches(version))
    }

    /// Human-readable description of this constraint.
    pub fn describe(&self) -> String {
        self.predicates
            .iter()
            .map(|p| {
                let op = match p.op {
                    Op::Eq => "=",
                    Op::Gte => ">=",
                    Op::Lt => "<",
                };
                format!(
                    "{op}{}.{}.{}",
                    p.version.major, p.version.minor, p.version.patch
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Check if `lastdb_build` satisfies the constraint, and format a clear error if not.
///
/// # Errors
/// Returns a user-facing error message if the build doesn't satisfy the constraint.
pub fn check_constraint(
    app_id: &str,
    constraint_str: &str,
    lastdb_build: &str,
) -> Result<(), String> {
    let constraint = Constraint::parse(constraint_str).map_err(|e| {
        format!("app '{app_id}' has invalid lastdb constraint '{constraint_str}': {e}")
    })?;

    let running_version = Version::parse(lastdb_build)
        .map_err(|e| format!("could not parse running lastdb version '{lastdb_build}': {e}"))?;

    if !constraint.matches(&running_version) {
        return Err(format!(
            "app '{app_id}' requires lastdb {}, but you have {} (core version {}.{}.{})",
            constraint.describe(),
            lastdb_build,
            running_version.major,
            running_version.minor,
            running_version.patch
        ));
    }

    Ok(())
}

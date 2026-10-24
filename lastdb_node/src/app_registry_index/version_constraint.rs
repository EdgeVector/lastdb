//! LastDB version constraint parsing and matching.
//!
//! Supports simple semver ranges in manifests, e.g. `"lastdb": ">=0.23.0, <0.24.0"`.
//! Constraints are comma-separated predicates: `>=X.Y.Z`, `<X.Y.Z`, `=X.Y.Z`.
//!
//! The running LastDB build string (e.g. `0.23.4-pre.0-324-g0ef44ca87`) is
//! parsed as SemVer core + pre-release. Constraints match against the core
//! version only (major.minor.patch), ignoring pre-release and build metadata.

use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then_with(|| self.minor.cmp(&other.minor))
            .then_with(|| self.patch.cmp(&other.patch))
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
        if let Some(rest) = s.strip_prefix(">=") {
            Ok(Self {
                op: Op::Gte,
                version: Version::parse(rest.trim())?,
            })
        } else if let Some(rest) = s.strip_prefix('<') {
            Ok(Self {
                op: Op::Lt,
                version: Version::parse(rest.trim())?,
            })
        } else if let Some(rest) = s.strip_prefix('=') {
            Ok(Self {
                op: Op::Eq,
                version: Version::parse(rest.trim())?,
            })
        } else {
            // Bare version means exact match
            Ok(Self {
                op: Op::Eq,
                version: Version::parse(s)?,
            })
        }
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

/// Check if an installed app's manifest declares a lastdb version constraint,
/// and verify the running LastDB build satisfies it. Also checks contract version
/// compatibility (api_version and outbox_format_version) when present in the manifest.
///
/// Looks for common manifest files: `lastdb-app.json`, `folddb.toml`, `*.app.json`.
/// If no manifest is found or no constraint is declared, this is a no-op (apps
/// without constraints install unconditionally).
///
/// # Errors
/// Returns an error if a constraint is found but not satisfied, or if contract versions
/// are incompatible with the running node.
pub fn check_manifest_constraint(
    checkout_path: &std::path::Path,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(), String> {
    use crate::version;

    // Try common manifest paths
    let manifest_candidates = [
        checkout_path.join("lastdb-app.json"),
        checkout_path.join("folddb.toml"),
    ];

    // Also try <app_id>.app.json
    let app_json = checkout_path.join(format!("{app_id}.app.json"));

    for path in manifest_candidates.iter().chain(std::iter::once(&app_json)) {
        if !path.exists() {
            continue;
        }

        let Ok(raw) = std::fs::read_to_string(path) else {
            continue;
        };

        // Try parsing as JSON first
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
            if let Some(constraint_str) = json.get("lastdb").and_then(|v| v.as_str()) {
                check_constraint(app_id, constraint_str, lastdb_version)?;
            }

            if let Some(api_version) = json.get("api_version").and_then(|v| v.as_u64()) {
                let api_version = api_version as u32;
                if api_version < version::MIN_SUPPORTED_API_VERSION
                    || api_version > version::CURRENT_API_VERSION
                {
                    return Err(format!(
                        "app '{app_id}' requires api_version {api_version}, but this node supports {}-{} (current: {})",
                        version::MIN_SUPPORTED_API_VERSION,
                        version::CURRENT_API_VERSION,
                        version::CURRENT_API_VERSION
                    ));
                }
            }

            if let Some(outbox_version) = json.get("outbox_format_version").and_then(|v| v.as_u64())
            {
                let outbox_version = outbox_version as u32;
                if outbox_version < version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION
                    || outbox_version > version::CURRENT_OUTBOX_FORMAT_VERSION
                {
                    return Err(format!(
                        "app '{app_id}' requires outbox_format_version {outbox_version}, but this node supports {}-{} (current: {})",
                        version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
                        version::CURRENT_OUTBOX_FORMAT_VERSION,
                        version::CURRENT_OUTBOX_FORMAT_VERSION
                    ));
                }
            }

            return Ok(());
        }

        // Try parsing as TOML (for folddb.toml)
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            // Simple line-by-line search for lastdb = "..."
            for line in raw.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("lastdb") {
                    let rest = rest.trim();
                    if let Some(rest) = rest.strip_prefix('=') {
                        let rest = rest.trim().trim_matches('"').trim_matches('\'');
                        if !rest.is_empty() {
                            return check_constraint(app_id, rest, lastdb_version);
                        }
                    }
                }
            }
        }
    }

    // No manifest or no constraint found - allow install
    Ok(())
}

impl Constraint {
    /// Parse a constraint string, e.g. `">=0.23.0, <0.24.0"`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .collect();
        if parts.is_empty() {
            return Err("constraint must not be empty".into());
        }
        let mut predicates = Vec::new();
        for part in parts {
            predicates.push(Predicate::parse(part)?);
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
            .map(|p| match p.op {
                Op::Eq => format!(
                    "={}.{}.{}",
                    p.version.major, p.version.minor, p.version.patch
                ),
                Op::Gte => format!(
                    ">={}.{}.{}",
                    p.version.major, p.version.minor, p.version.patch
                ),
                Op::Lt => format!(
                    "<{}.{}.{}",
                    p.version.major, p.version.minor, p.version.patch
                ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version() {
        let v = Version::parse("0.23.4").unwrap();
        assert_eq!(
            v,
            Version {
                major: 0,
                minor: 23,
                patch: 4
            }
        );

        let v = Version::parse("0.23.4-pre.0-324-g0ef44ca87").unwrap();
        assert_eq!(
            v,
            Version {
                major: 0,
                minor: 23,
                patch: 4
            }
        );

        let v = Version::parse("0.23.3-2378-gbe41e547e").unwrap();
        assert_eq!(
            v,
            Version {
                major: 0,
                minor: 23,
                patch: 3
            }
        );
    }

    #[test]
    fn parse_constraint() {
        let c = Constraint::parse(">=0.23.0, <0.24.0").unwrap();
        assert_eq!(c.predicates.len(), 2);
        assert_eq!(c.predicates[0].op, Op::Gte);
        assert_eq!(c.predicates[1].op, Op::Lt);
    }

    #[test]
    fn check_constraint_satisfied() {
        assert!(
            check_constraint("test", ">=0.23.0, <0.24.0", "0.23.4-pre.0-324-g0ef44ca87").is_ok()
        );
        assert!(check_constraint("test", ">=0.23.0", "0.23.0").is_ok());
        assert!(check_constraint("test", "<0.24.0", "0.23.99").is_ok());
    }

    #[test]
    fn check_constraint_unsatisfied() {
        let err = check_constraint("brain", ">=0.23.0, <0.24.0", "0.22.9").unwrap_err();
        assert!(err.contains("requires lastdb >=0.23.0"));

        let err = check_constraint("brain", ">=0.23.0", "0.22.9").unwrap_err();
        assert!(err.contains("0.22.9"));
    }
}

use super::*;

// ─── Artifact verification ────────────────────────────────────────────────

/// Why an artifact was refused. Both variants stop activation before the
/// `current` pointer moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactFault {
    /// The bytes do not hash to `manifest.artifact_digest`.
    Digest { expected: String, actual: String },
    /// The publisher signature over the digest does not verify.
    Signature(String),
}

impl std::fmt::Display for ArtifactFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Digest { expected, actual } => write!(
                f,
                "artifact digest fault: manifest says {expected}, bytes hash to {actual}"
            ),
            Self::Signature(detail) => write!(f, "artifact signature fault: {detail}"),
        }
    }
}

/// Verify downloaded artifact bytes against the release manifest.
///
/// Two independent checks, in order: the byte digest, then the publisher's
/// Ed25519 signature over that digest. The signed payload is the digest's
/// ASCII hex, so a signature can never be replayed onto different bytes.
///
/// # Errors
/// Returns the fault that stopped activation.
pub fn verify_artifact(
    bytes: &[u8],
    manifest: &ReleaseManifest,
    publisher_dev_pubkey: &str,
) -> Result<(), ArtifactFault> {
    let actual = sha256_hex(bytes);
    if actual != manifest.artifact_digest {
        return Err(ArtifactFault::Digest {
            expected: manifest.artifact_digest.clone(),
            actual,
        });
    }
    let key = verifying_key_from_base64(publisher_dev_pubkey)
        .map_err(|e| ArtifactFault::Signature(format!("publisher key does not parse: {e}")))?;
    let raw = BASE64
        .decode(manifest.artifact_signature.as_bytes())
        .map_err(|e| ArtifactFault::Signature(format!("signature is not base64: {e}")))?;
    let signature: [u8; 64] = raw.as_slice().try_into().map_err(|_| {
        ArtifactFault::Signature(format!("signature is {} bytes, want 64", raw.len()))
    })?;
    verify(&key, &signature, actual.as_bytes())
        .map_err(|e| ArtifactFault::Signature(format!("signature does not verify: {e}")))
}

// ─── Probe ────────────────────────────────────────────────────────────────

/// The result of the activation probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeOutcome {
    Green,
    Red(String),
}

impl ProbeOutcome {
    #[must_use]
    pub fn is_green(&self) -> bool {
        matches!(self, Self::Green)
    }
}

/// Run the release's probe.
///
/// A release may ship `probe.sh` in its version directory; exit 0 is green.
/// A release that ships none still gets a real check: the host verifies the
/// version directory exists and still carries a receipt that matches its
/// release id. There is no "no probe declared, assume green" path, because
/// `CURRENT` claims the app works.
#[must_use]
pub fn run_probe(host: &HostTrack, release_id: &str) -> ProbeOutcome {
    let dir = host.version_dir(release_id);
    if !dir.is_dir() {
        return ProbeOutcome::Red(format!("{} is not a directory", dir.display()));
    }
    match host.installed_release_id(release_id) {
        Some(_) => {}
        None => {
            return ProbeOutcome::Red(format!(
                "version directory for {release_id} has no matching receipt"
            ))
        }
    }
    let script = dir.join("probe.sh");
    if !script.is_file() {
        return ProbeOutcome::Green;
    }
    match ProcessCommand::new("sh")
        .arg(&script)
        .current_dir(&dir)
        .status()
    {
        Ok(status) if status.success() => ProbeOutcome::Green,
        Ok(status) => ProbeOutcome::Red(format!("probe.sh exited {status}")),
        Err(e) => ProbeOutcome::Red(format!("probe.sh did not run: {e}")),
    }
}

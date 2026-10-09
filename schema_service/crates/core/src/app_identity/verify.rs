//! Shared dev-cert + signature-envelope verification.

use super::*;

/// The developer identity a verified cert + signature establishes.
pub(crate) struct VerifiedDev {
    pub(crate) dev_pubkey: String,
    /// From the signed cert: whether this developer is an authorized
    /// publisher (paid OR `developer_access`). Required to be `true` to
    /// reserve an app namespace — enforced identically in dev and prod.
    pub(crate) authorized_publisher: bool,
}

/// Discriminated verification failure, before mapping onto the
/// per-endpoint error enums.
pub(crate) enum AuthFailure {
    CertInvalid,
    CertExpired,
    EnvelopeInvalid,
    DevRevoked,
}

fn decode_b64_json<T: DeserializeOwned>(header: &str) -> Result<T, ()> {
    let bytes = BASE64.decode(header.trim().as_bytes()).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn cert_env_matches(deployment: Env, cert_env: &str) -> bool {
    cert_env == env_label(deployment)
}

/// Verify the `X-Exemem-Dev-Cert` + `X-Signature` pair for a request
/// whose signed payload is `payload` and whose envelope purpose must be
/// `expected_purpose`. Returns the developer pubkey the request is
/// authorized as.
///
/// The cert and envelope must be bound to this deployment's env
/// (`config.deployment_env`) — a `dev`-signed envelope must not verify
/// against a `prod` deployment, and vice versa. Dev and prod are fully
/// independent registries (each with its own trusted root); there is no
/// cross-env replay.
pub(crate) fn verify_cert_and_signature(
    config: &AppIdentityConfig,
    cert_header: &str,
    sig_header: &str,
    payload: &Value,
    expected_purpose: Purpose,
) -> Result<VerifiedDev, AuthFailure> {
    let expected_env = config.deployment_env;
    // 1. Decode the cert and check its intrinsic, non-crypto claims.
    let cert: DevCert = decode_b64_json(cert_header).map_err(|()| AuthFailure::CertInvalid)?;
    if cert.purpose != PURPOSE_DEV_CERT {
        return Err(AuthFailure::CertInvalid);
    }
    if !cert_env_matches(expected_env, &cert.env) {
        return Err(AuthFailure::CertInvalid);
    }

    // 2. Select the trusted root by key_id and verify the ES256 cert.
    let Some(root_der) = config.trusted_roots.get(&cert.key_id) else {
        return Err(AuthFailure::CertInvalid);
    };
    match verify_dev_cert(root_der, &cert) {
        Ok(()) => {}
        Err(DevCertVerifyError::Expired) => return Err(AuthFailure::CertExpired),
        Err(_) => return Err(AuthFailure::CertInvalid),
    }

    // 3. Offline revocation denylist.
    if config.revoked_dev_pubkeys.contains(&cert.dev_pubkey) {
        return Err(AuthFailure::DevRevoked);
    }

    // 4. Verify the dev-signed envelope against the cert's dev pubkey.
    let envelope: SignatureEnvelope =
        decode_b64_json(sig_header).map_err(|()| AuthFailure::EnvelopeInvalid)?;
    if envelope.purpose != expected_purpose {
        return Err(AuthFailure::EnvelopeInvalid);
    }
    if envelope.env != expected_env {
        return Err(AuthFailure::EnvelopeInvalid);
    }
    let dev_vk =
        verifying_key_from_base64(&cert.dev_pubkey).map_err(|_| AuthFailure::CertInvalid)?;
    // `verify_envelope` also checks envelope.key_id == sha256(dev_pubkey),
    // binding the signature to the cert's developer.
    verify_envelope(&dev_vk, &envelope).map_err(|_| AuthFailure::EnvelopeInvalid)?;

    // 5. payload_hash binds the envelope to THIS request body.
    let expected_hash = compute_payload_hash(payload).map_err(|_| AuthFailure::EnvelopeInvalid)?;
    if envelope.payload_hash != expected_hash {
        return Err(AuthFailure::EnvelopeInvalid);
    }

    Ok(VerifiedDev {
        dev_pubkey: cert.dev_pubkey,
        authorized_publisher: cert.authorized_publisher,
    })
}

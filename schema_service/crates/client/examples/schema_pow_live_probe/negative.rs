//! Negative-proof probes: missing, invalid and expired PoW submissions.

use super::*;

fn sign_negative_claim(
    signing_key: &SigningKey,
    environment: Env,
    schema_hash: &str,
    node_public_key_hash: &str,
    challenge_id: &str,
    nonce: &str,
    counter: u64,
) -> Result<String, String> {
    let payload = node_signature_payload(
        schema_hash,
        node_public_key_hash,
        challenge_id,
        nonce,
        counter,
    );
    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose: Purpose::SchemaClaim,
        alg: ALG_ED25519.to_string(),
        key_id: node_public_key_hash.to_string(),
        issued_at: Utc::now(),
        expires_at: None,
        env: environment,
        payload_hash: compute_payload_hash(&payload)
            .map_err(|_| "failed to hash negative proof claim".to_string())?,
        sig: None,
    };
    let signed = sign_envelope(signing_key, unsigned)
        .map_err(|_| "failed to sign negative proof claim".to_string())?;
    let bytes = serde_json::to_vec(&signed)
        .map_err(|_| "failed to serialize negative proof claim".to_string())?;
    Ok(BASE64.encode(bytes))
}

/// Require a 401 whose rejection reason matches the mode's expected reason.
async fn expect_unauthorized_rejection(
    response: reqwest::Response,
    mode: NegativeProof,
    label: &str,
) -> Result<(), String> {
    if response.status() != reqwest::StatusCode::UNAUTHORIZED {
        return Err(format!("{label} did not return unauthorized"));
    }
    let body = response
        .json::<serde_json::Value>()
        .await
        .map_err(|_| format!("{label} rejection body was invalid"))?;
    let reason = rejection_reason(&body);
    if reason != mode.expected_reason() {
        return Err(format!(
            "{label} returned an unexpected rejection reason={reason}"
        ));
    }
    Ok(())
}

/// Request a mutation challenge for the schema hash.
async fn request_negative_challenge(
    http: &reqwest::Client,
    base_url: &str,
    node_public_key: &str,
    schema_hash: &str,
    app_id: Option<&String>,
) -> Result<SchemaMutationChallengeResponse, String> {
    let challenge = http
        .post(format!("{base_url}/v1/schemas/mutation-challenge"))
        .json(&SchemaMutationChallengeRequest {
            node_public_key: node_public_key.to_string(),
            schema_hash: schema_hash.to_string(),
            app_id: app_id.cloned(),
        })
        .send()
        .await
        .map_err(|_| "negative proof challenge request failed".to_string())?;
    if challenge.status() != reqwest::StatusCode::CREATED {
        return Err("negative proof challenge was not issued".to_string());
    }
    challenge
        .json::<SchemaMutationChallengeResponse>()
        .await
        .map_err(|_| "negative proof challenge response was invalid".to_string())
}

/// First counter at or after the challenge start that does NOT satisfy the PoW.
fn first_unsatisfying_counter(
    challenge: &SchemaMutationChallengeResponse,
    node_public_key_hash: &str,
    schema_hash: &str,
) -> Result<u64, String> {
    let mut candidate = challenge.counter_start;
    while pow_satisfies(
        &challenge.nonce,
        node_public_key_hash,
        schema_hash,
        candidate,
        challenge.difficulty_bits,
    ) {
        candidate = candidate
            .checked_add(1)
            .ok_or_else(|| "negative proof counter overflow".to_string())?;
    }
    Ok(candidate)
}

pub(super) async fn run_negative_probe(
    args: &ProbeArgs,
    schema: &Schema,
    signing_key: &SigningKey,
    mode: NegativeProof,
    environment_label: &str,
) -> Result<(), String> {
    let verifying_key = signing_key.verifying_key();
    let node_public_key = BASE64.encode(verifying_key.to_bytes());
    let node_public_key_hash = key_id(&verifying_key);
    let schema_payload = serde_json::to_value(schema)
        .map_err(|_| "failed to serialize negative proof schema".to_string())?;
    let schema_hash = schema_payload_hash(&schema_payload)
        .map_err(|_| "failed to hash negative proof schema".to_string())?;
    let request_timeout = Duration::from_secs(args.request_timeout_secs);
    let http = reqwest::Client::builder()
        .timeout(request_timeout)
        .connect_timeout(SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT.min(request_timeout))
        .no_proxy()
        .build()
        .map_err(|_| "failed to build bounded HTTP client".to_string())?;
    let request = AddSchemaRequest {
        schema: schema.clone(),
        mutation_mappers: HashMap::new(),
        schema_match_source: "direct".to_string(),
        fallback_reason: None,
        offer_to_shared_discovery: false,
        shared_surface: None,
    };
    let base_url = args.url.trim_end_matches('/');
    if mode == NegativeProof::Missing {
        emit_progress(
            environment_label,
            &schema.name,
            "missing-proof-submit",
            Some(mode),
        );
        let response = http
            .post(format!("{base_url}/v1/schemas"))
            .json(&request)
            .send()
            .await
            .map_err(|_| "missing proof registration request failed".to_string())?;
        return expect_unauthorized_rejection(response, mode, "missing proof").await;
    }
    emit_progress(
        environment_label,
        &schema.name,
        "negative-proof-challenge",
        Some(mode),
    );
    let challenge = request_negative_challenge(
        &http,
        base_url,
        &node_public_key,
        &schema_hash,
        schema.owner_app_id.as_ref(),
    )
    .await?;

    let counter = match mode {
        NegativeProof::Missing => unreachable!("missing proof returns before challenge"),
        NegativeProof::Expired => challenge.counter_start,
        NegativeProof::Invalid => {
            first_unsatisfying_counter(&challenge, &node_public_key_hash, &schema_hash)?
        }
    };
    let expires_at = match mode {
        NegativeProof::Missing => unreachable!("missing proof returns before challenge"),
        NegativeProof::Invalid => challenge.expires_at_unix_secs,
        NegativeProof::Expired => now_secs().saturating_sub(1),
    };
    let signature = sign_negative_claim(
        signing_key,
        args.environment,
        &schema_hash,
        &node_public_key_hash,
        &challenge.challenge_id,
        &challenge.nonce,
        counter,
    )?;
    emit_progress(
        environment_label,
        &schema.name,
        "negative-proof-submit",
        Some(mode),
    );
    let response = http
        .post(format!("{base_url}/v1/schemas"))
        .header(HEADER_NODE_PUBLIC_KEY, node_public_key)
        .header(HEADER_NODE_SIGNATURE, signature)
        .header(HEADER_POW_CHALLENGE, challenge.challenge_id)
        .header(HEADER_POW_NONCE, challenge.nonce)
        .header(HEADER_POW_CHALLENGE_MAC, challenge.challenge_mac)
        .header(
            HEADER_POW_DIFFICULTY_BITS,
            challenge.difficulty_bits.to_string(),
        )
        .header(HEADER_POW_EXPIRES_AT, expires_at.to_string())
        .header(HEADER_POW_COUNTER, counter.to_string())
        .json(&request)
        .send()
        .await
        .map_err(|_| "negative proof registration request failed".to_string())?;
    expect_unauthorized_rejection(response, mode, "negative proof").await
}

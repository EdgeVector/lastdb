use super::*;

pub(super) fn ephemeral_signing_key() -> Result<SigningKey, String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|_| "failed to obtain operating-system randomness".to_string())?;
    Ok(SigningKey::from_bytes(&bytes))
}

pub(super) fn probe_schema(environment: Env, run_id: &str) -> Schema {
    let env = match environment {
        Env::Dev => "dev",
        Env::Prod => "prod",
    };
    let schema_name = format!("schema_pow_live_probe_{env}_{run_id}");
    let field_name = format!("pow_probe_token_{run_id}");
    let mut schema = Schema::new(
        schema_name,
        SchemaType::Single,
        None,
        Some(vec![field_name.clone()]),
        None,
        None,
    );
    schema.descriptive_name = Some(format!("Schema PoW live probe {env} {run_id}"));
    schema.purpose_statement =
        Some("Verify live proof-of-work admission control with a non-sensitive token.".to_string());
    schema.owner_app_id = Some("schema-pow-live-proof".to_string());
    schema.field_descriptions.insert(
        field_name,
        "A non-sensitive synthetic token used only for Schema Service PoW verification."
            .to_string(),
    );
    schema
}

pub(super) fn resolve_proposal(schema: &Schema) -> SchemaResolveProposal {
    SchemaResolveProposal {
        descriptive_name: schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone()),
        fields: schema.fields.clone().unwrap_or_default(),
        field_descriptions: schema.field_descriptions.clone(),
        purpose_statement: schema.purpose_statement.clone(),
        identity_hash: schema.identity_hash.clone(),
        owner_app_id: schema.owner_app_id.clone(),
    }
}

pub(super) fn outcome_label(outcome: &SchemaResolveOutcome) -> &'static str {
    match outcome {
        SchemaResolveOutcome::Reuse => "reuse",
        SchemaResolveOutcome::Novel => "novel",
        SchemaResolveOutcome::CandidateEquivalent => "candidate_equivalent",
        SchemaResolveOutcome::Refresh => "refresh",
    }
}

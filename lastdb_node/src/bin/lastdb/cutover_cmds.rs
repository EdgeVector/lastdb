//! Read-only evidence check for one existing database. No socket, no cloud, no write.

use database_cutover::verify;

use super::cli_defs::{CutoverCommand, CutoverEvidenceArgs};

#[path = "cutover_io.rs"]
mod io;

pub(crate) fn run(action: CutoverCommand) -> Result<(), String> {
    match action {
        CutoverCommand::Verify(args) => verify_evidence(&args),
    }
}

fn verify_evidence(args: &CutoverEvidenceArgs) -> Result<(), String> {
    let bundle = io::load_bundle(&args.directory)?;
    let digest = verify(&args.database_id, &args.environment, &bundle)
        .map_err(|refusal| refusal.to_string())?;
    let revision = bundle.grants.revision;
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "database_id": args.database_id,
                "environment": args.environment,
                "evidence_sha256": digest,
                "grant_revision": revision,
                "activated": false,
            })
        );
    } else {
        println!(
            "verified database_id={} environment={} evidence_sha256={digest} grant_revision={revision} activated=false",
            args.database_id, args.environment
        );
    }
    Ok(())
}

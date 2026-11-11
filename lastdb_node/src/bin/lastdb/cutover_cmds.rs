//! Cutover commands. Verify is local. Request sends one receipt and writes nothing.

use std::path::PathBuf;

use database_cutover::verify;

use super::cli_defs::{CutoverCommand, CutoverEvidenceArgs, CutoverRequestArgs};

#[path = "cutover_io.rs"]
mod io;
#[path = "cutover_request.rs"]
mod request;

pub(crate) fn run(data_dir: Option<PathBuf>, action: CutoverCommand) -> Result<(), String> {
    match action {
        CutoverCommand::Verify(args) => {
            if data_dir.is_some() {
                return Err("cutover verify does not open a data directory".into());
            }
            verify_evidence(&args)
        }
        CutoverCommand::Request(args) => request::run(data_dir, args),
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

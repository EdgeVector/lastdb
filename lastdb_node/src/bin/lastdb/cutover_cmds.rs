//! Operator entry for one database cutover request. No socket, no cloud, no deletion.

use std::path::Path;

use database_cutover::{check_proof, prepare, resume_unchanged, verify, PrepareOutcome};

use super::cli_defs::{
    CutoverCommand, CutoverEvidenceArgs, CutoverPrepareArgs, CutoverProofArgs, CutoverResumeArgs,
};

#[path = "cutover_io.rs"]
mod io;

pub(crate) fn run(action: CutoverCommand) -> Result<(), String> {
    match action {
        CutoverCommand::Verify(args) => verify_evidence(&args),
        CutoverCommand::Prepare(args) => prepare_request(&args),
        CutoverCommand::ResumeCheck(args) => resume_check(&args),
        CutoverCommand::Proof(args) => proof_check(&args),
    }
}

fn verify_evidence(args: &CutoverEvidenceArgs) -> Result<(), String> {
    let bundle = io::load_bundle(&args.directory)?;
    let digest = verify(&args.database_id, &bundle).map_err(|refusal| refusal.to_string())?;
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "database_id": args.database_id,
                "evidence_sha256": digest,
                "request_ready": true,
                "activated": false,
            })
        );
    } else {
        println!(
            "request_ready database_id={} evidence_sha256={digest} activated=false",
            args.database_id
        );
    }
    Ok(())
}

fn prepare_request(args: &CutoverPrepareArgs) -> Result<(), String> {
    let bundle = io::load_bundle(&args.evidence.directory)?;
    let existing = io::load_request(&args.request)?;
    let outcome = prepare(&args.evidence.database_id, existing.as_ref(), &bundle)
        .map_err(|refusal| refusal.to_string())?;
    match outcome {
        PrepareOutcome::Ready(request) => {
            io::write_request(&args.request, &request)?;
            print_request(&request, args.evidence.json, "request_ready")
        }
        PrepareOutcome::Unchanged(request) => {
            print_request(&request, args.evidence.json, "unchanged")
        }
    }
}

fn resume_check(args: &CutoverResumeArgs) -> Result<(), String> {
    let Some(request) = io::load_request(&args.request)? else {
        print_resume(&args.database_id, false, args.json);
        return Ok(());
    };
    resume_unchanged(&request, &args.database_id).map_err(|refusal| refusal.to_string())?;
    print_resume(&args.database_id, true, args.json);
    Ok(())
}

fn proof_check(args: &CutoverProofArgs) -> Result<(), String> {
    let request = required_request(&args.request)?;
    if request.database_id != args.database_id {
        return Err(format!(
            "{}: database_id does not match the named database",
            database_cutover::REQUEST_FILE
        ));
    }
    let proof = io::load_proof(&args.proof)?;
    check_proof(&request, &proof).map_err(|refusal| refusal.to_string())?;
    if args.json {
        println!(
            "{}",
            serde_json::json!({
                "database_id": args.database_id,
                "second_device_ack": true,
                "keyed_reads_match": true,
                "retirement_allowed": false,
                "activated": false,
            })
        );
        return Ok(());
    }
    println!(
        "second_device_ack=true keyed_reads_match=true retirement_allowed=false activated=false database_id={}",
        args.database_id
    );
    Ok(())
}

fn required_request(path: &Path) -> Result<database_cutover::Request, String> {
    io::load_request(path)?
        .ok_or_else(|| format!("cutover request does not exist: {}", path.display()))
}

fn print_request(
    request: &database_cutover::Request,
    json: bool,
    result: &str,
) -> Result<(), String> {
    if json {
        let request = serde_json::to_value(request)
            .map_err(|_| "cannot encode the cutover request".to_owned())?;
        let body = serde_json::json!({ "result": result, "request": request });
        let text = serde_json::to_string_pretty(&body)
            .map_err(|_| "cannot encode the cutover request".to_owned())?;
        println!("{text}");
        return Ok(());
    }
    println!(
        "{result} database_id={} phase={} activation={} authority={} legacy_resume={} legacy_retirement={}",
        request.database_id,
        request.phase,
        request.activation,
        request.authority,
        request.legacy_resume,
        request.legacy_retirement
    );
    Ok(())
}

fn print_resume(database_id: &str, request_ready: bool, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "database_id": database_id,
                "activated": false,
                "legacy_resume": "unchanged",
                "request_ready": request_ready,
            })
        );
        return;
    }
    println!(
        "activated=false legacy_resume=unchanged request_ready={request_ready} database_id={database_id}"
    );
}

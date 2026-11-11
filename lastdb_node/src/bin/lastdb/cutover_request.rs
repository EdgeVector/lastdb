//! Send one publisher cutover. The evidence check runs first. Nothing is written.

use std::path::PathBuf;

use cloud_backup_protocol::v2::share_access::CutoverReceipt;
use database_cutover::verify;
use lastdb_node::legacy_cutover::{connection_realm, post, CutoverSubmit};

use super::super::cli_defs::CutoverRequestArgs;
use super::io;

pub(super) fn run(data_dir: Option<PathBuf>, args: CutoverRequestArgs) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let realm = connection_realm(&home)?;
    let bundle = io::load_bundle(&args.directory)?;
    let digest = verify(&args.database_id, realm.as_str(), &bundle)
        .map_err(|refusal| refusal.to_string())?;
    let submit = CutoverSubmit {
        database_id: args.database_id,
        evidence_sha256: digest,
        grant_revision: bundle.grants.revision,
        authority_node_key: bundle.grants.authority_node_key,
    };
    let receipt = runtime()?.block_on(post(&home, &submit))?;
    print_receipt(&receipt, args.json);
    Ok(())
}

fn print_receipt(receipt: &CutoverReceipt, json: bool) {
    let database_id = receipt.database_id.as_str();
    let environment = receipt.environment.as_str();
    let evidence_sha256 = receipt.evidence_sha256.as_str();
    let grant_revision = receipt.grant_revision.get();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "state": "committed",
                "database_id": database_id,
                "environment": environment,
                "evidence_sha256": evidence_sha256,
                "grant_revision": grant_revision,
            })
        );
    } else {
        println!(
            "receipt state=committed database_id={database_id} environment={environment} evidence_sha256={evidence_sha256} grant_revision={grant_revision}"
        );
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot start the cutover runtime".to_owned())
}

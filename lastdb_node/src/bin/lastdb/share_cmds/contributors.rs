use super::{read_contact, read_file, runtime, write_file};
use crate::ShareContributorCommand;
use cloud_backup_protocol::{Digest, Id};
use lastdb_node::share_contribution::{self, GrantRequest, WriteRule};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::Path;

const MAX_INPUT_BYTES: u64 = 64 * 1024;

pub(super) fn run(identity_home: &Path, action: ShareContributorCommand) -> Result<(), String> {
    match action {
        ShareContributorCommand::Grant {
            home,
            recipient,
            recipient_key,
            rules,
            operation_id,
            expected_version,
            out,
        } => {
            let contact = read_contact(&recipient, recipient_key)?;
            let rules: Vec<WriteRule> = read_json(&rules, "invalid share write rules")?;
            let operation_id = Id::try_from(operation_id).map_err(|_| "invalid operation UUID")?;
            let sealed = runtime()?.block_on(share_contribution::grant(GrantRequest {
                home: &home,
                owner_home: identity_home,
                contact: &contact,
                rules,
                operation_id,
                expected_version,
            }))?;
            write_file(&out, &sealed)?;
            println!("{}", json!({"grant": out}));
        }
        ShareContributorCommand::Revoke {
            home,
            recipient_key,
            expected_version,
        } => {
            let writer =
                Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
            share_contribution::revoke(&home, identity_home, &writer, expected_version)?;
            println!("{}", json!({"revoked": true}));
        }
        ShareContributorCommand::Accept { .. } => {
            unreachable!("grant attachment is routed to the node")
        }
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path, description: &'static str) -> Result<T, String> {
    let bytes = read_file(path, MAX_INPUT_BYTES)?;
    serde_json::from_slice(&bytes).map_err(|_| description.into())
}

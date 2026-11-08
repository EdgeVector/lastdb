use super::GuestStageArgs;
use cloud_backup_client::byte_backup::{ByteBackupReader, GuestReadPermission};
use cloud_backup_protocol::Digest;
use lastdb_node::byte_cloud_restore::RestoreSession;
use serde::Deserialize;
use std::io::{self, IsTerminal, Read};
use zeroize::Zeroizing;

const INPUT_LIMIT: u64 = 4096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GuestInput<'a> {
    authorization: &'a str,
    publisher_public_key: &'a str,
    content_key: &'a str,
}

pub(super) fn run(args: GuestStageArgs) -> Result<(), String> {
    if !args.permission_stdin {
        return Err("stage-guest requires --permission-stdin".into());
    }
    for path in args.into.iter().chain(args.resume.iter()) {
        if !path.is_absolute() {
            return Err("guest stage requires an absolute path".into());
        }
        lastdb_node::offline_home::refuse_primary(path)?;
    }
    let (permission, publisher, content_key) = read_input()?;
    let reader = ByteBackupReader::guest(permission).map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct guest stage runtime")?;
    runtime.block_on(async {
        let mut session = match (args.into, args.resume) {
            (Some(path), None) => RestoreSession::begin_guest(&path, &reader).await,
            (None, Some(path)) => RestoreSession::resume(&path),
            _ => return Err("stage-guest requires either --into or --resume".into()),
        }
        .map_err(|error| error.to_string())?;
        let data = session
            .stage_recipient(&reader, content_key, publisher)
            .await
            .map_err(|error| {
                format!(
                    "guest stage retained workspace {}: {error}",
                    session.path().display()
                )
            })?;
        let accepted = session.accepted();
        if args.json {
            println!(
                "{}",
                serde_json::json!({
                    "workspace": session.path(),
                    "data": data,
                    "commit_id": accepted.job().commit_id,
                    "commit_sha256": accepted.commit_sha256,
                })
            );
        } else {
            println!("Verified guest stage {}", data.display());
            println!("Workspace {}", session.path().display());
        }
        Ok(())
    })
}

fn read_input() -> Result<(GuestReadPermission, Digest, Zeroizing<[u8; 32]>), String> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err("pipe the guest permission bundle to stdin".into());
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity((INPUT_LIMIT + 1) as usize));
    stdin
        .lock()
        .take(INPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read guest permission bundle")?;
    if bytes.len() as u64 > INPUT_LIMIT {
        return Err("guest permission bundle exceeds input limit".into());
    }
    let input: GuestInput<'_> =
        serde_json::from_slice(&bytes).map_err(|_| "invalid guest permission bundle")?;
    let permission =
        GuestReadPermission::parse(input.authorization).map_err(|error| error.to_string())?;
    let publisher = Digest::try_from(input.publisher_public_key.to_owned())
        .map_err(|_| "invalid guest publisher public key")?;
    if permission.publisher_public_key() != &publisher {
        return Err("guest permission signer differs from the trusted publisher".into());
    }
    let mut key = Zeroizing::new([0u8; 32]);
    hex::decode_to_slice(input.content_key, &mut key[..])
        .map_err(|_| "invalid guest content key")?;
    Ok((permission, publisher, key))
}

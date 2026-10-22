use super::*;
use cloud_backup_protocol::{DecimalU64, Text};
use lastdb_node::native_backup_access::{self, NativeRestoreRequest};
use std::io::Read;

pub(super) fn run(home: &Path, action: NativeBackupCommand) -> Result<(), String> {
    lastdb_node::native_cloud_backup::require_enabled().map_err(|error| error.to_string())?;
    let phrase = phrase()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct native recovery runtime".to_owned())?;
    runtime.block_on(async {
        match action {
            NativeBackupCommand::Discover {
                generation,
                cursor,
                json,
                ..
            } => {
                let account = native_backup_access::account_from_phrase(&phrase)?;
                let cursor = cursor
                    .map(Text::try_from)
                    .transpose()
                    .map_err(|_| "invalid native directory cursor".to_owned())?;
                let report = native_backup_access::discover(
                    home,
                    &account,
                    generation.map(DecimalU64::new),
                    cursor,
                )
                .await?;
                print(
                    &serde_json::to_value(report)
                        .map_err(|_| "cannot encode native discovery".to_owned())?,
                    json,
                )
            }
            NativeBackupCommand::Restore {
                stream,
                source_home,
                generation,
                commit,
                into,
                resume,
                json,
                ..
            } => {
                let request = selection(stream, source_home, generation, commit, into, resume)?;
                let mut restore =
                    native_backup_access::prepare_restore(home, &phrase, request).await?;
                eprintln!("Native restore session: {}", restore.path().display());
                let report = restore.run().await?;
                print(
                    &serde_json::to_value(report)
                        .map_err(|_| "cannot encode native restore".to_owned())?,
                    json,
                )
            }
            _ => Err("invalid offline native backup command".into()),
        }
    })
}

fn selection(
    stream: Option<String>,
    source_home: Option<String>,
    generation: Option<u64>,
    commit: Option<String>,
    destination: Option<PathBuf>,
    resume: Option<PathBuf>,
) -> Result<NativeRestoreRequest, String> {
    if let Some(session) = resume {
        if stream.is_some()
            || source_home.is_some()
            || generation.is_some()
            || commit.is_some()
            || destination.is_some()
        {
            return Err("resume cannot change the saved native restore selection".into());
        }
        return Ok(NativeRestoreRequest::Resume { session });
    }
    Ok(NativeRestoreRequest::Begin {
        stream: id(stream.ok_or("native restore requires --stream")?)?,
        source_home: id(source_home.ok_or("native restore requires --source-home")?)?,
        generation: generation.map(DecimalU64::new),
        commit: commit.map(id).transpose()?,
        destination: destination.ok_or("native restore requires --into")?,
    })
}

fn id(value: String) -> Result<Id, String> {
    Id::try_from(value).map_err(|_| "native restore identity must be a canonical UUID".to_owned())
}

fn phrase() -> Result<zeroize::Zeroizing<String>, String> {
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read recovery phrase from stdin".to_owned())?;
    if bytes.len() > 4096 {
        return Err("recovery phrase input exceeds its byte limit".into());
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| "recovery phrase input is not UTF-8".to_owned())?;
    Ok(zeroize::Zeroizing::new(text.trim().to_owned()))
}

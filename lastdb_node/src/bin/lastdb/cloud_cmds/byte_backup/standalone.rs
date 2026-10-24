use super::*;
use lastdb_node::byte_cloud_backup::standalone::{self, Action, Options};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn run(
    home: &Path,
    state_dir: PathBuf,
    credentials_home: PathBuf,
    env: &str,
    action: ByteStandaloneCommand,
) -> Result<(), String> {
    let (action, json) = match action {
        ByteStandaloneCommand::Start { request_id, json } => (
            Action::Start {
                request_id: Id::try_from(request_id)
                    .map_err(|_| "invalid standalone request UUID")?,
            },
            json,
        ),
        ByteStandaloneCommand::Resume { json } => (Action::Resume, json),
        ByteStandaloneCommand::Status { json } => (Action::Status, json),
        ByteStandaloneCommand::Close { json } => (Action::Close, json),
    };
    let options = Options {
        source_home: home.to_path_buf(),
        state_dir,
        credentials_home,
        environment: folddb_profile::endpoints::Environment::try_from_str(env)
            .map_err(|_| "standalone environment must be dev or prod")?,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct standalone V2 runtime")?;
    let pause = AtomicBool::new(false);
    let report = runtime
        .block_on(async {
            let work = standalone::request(&options, action, &pause);
            tokio::pin!(work);
            tokio::select! {
                result = &mut work => result,
                _ = tokio::signal::ctrl_c() => {
                    pause.store(true, Ordering::Release);
                    // Keep source readers and every started transfer owned until exit.
                    work.await
                }
            }
        })
        .map_err(|error| error.to_string())?;
    drop(runtime);
    print(
        &serde_json::to_value(report).map_err(|_| "cannot encode standalone result")?,
        json,
    )
}

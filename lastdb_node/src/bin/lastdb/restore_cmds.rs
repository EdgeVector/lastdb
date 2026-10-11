use super::*;

pub(super) fn restore_command(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), String> {
    match restore_command_inner(data_dir, into, env, api_url, json_only) {
        Ok(()) => Ok(()),
        Err(failure) => {
            if json_only {
                println!("{}", render_restore_failure_json(&failure));
            }
            Err(failure.detail)
        }
    }
}

pub(super) fn restore_command_inner(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), RestoreFailure> {
    restore_command_inner_with_progress(data_dir, into, env, api_url, json_only, None)
}

pub(super) fn restore_command_with_progress(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
) -> Result<(), String> {
    let reporter = restore_progress_reporter::Reporter::stderr();
    let result = restore_command_inner_with_progress(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        Some(&reporter.progress),
    );
    reporter.finish(result.is_ok());
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}

pub(super) fn restore_command_with_chunk_cache(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress_json: bool,
    cache_home: &Path,
) -> Result<(), String> {
    let reporter = progress_json.then(restore_progress_reporter::Reporter::stderr);
    let result = restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        reporter.as_ref().map(|reporter| reporter.progress.as_ref()),
        RestoreSourceMode::Normal {
            cache_home: Some(cache_home),
        },
    );
    if let Some(reporter) = reporter {
        reporter.finish(result.is_ok());
    }
    result.map_err(|failure| {
        if json_only {
            println!("{}", render_restore_failure_json(&failure));
        }
        failure.detail
    })
}

pub(super) fn restore_command_inner_with_progress(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
) -> Result<(), RestoreFailure> {
    restore_command_inner_with_cache(
        data_dir,
        into,
        env,
        api_url,
        json_only,
        progress,
        RestoreSourceMode::Normal { cache_home: None },
    )
}

pub(super) fn restore_command_inner_with_cache(
    data_dir: Option<PathBuf>,
    into: &Path,
    env: Option<&str>,
    api_url: Option<String>,
    json_only: bool,
    progress: Option<&fold_db::sync::engine::RestoreProgress>,
    mode: RestoreSourceMode<'_>,
) -> Result<(), RestoreFailure> {
    use restore_phases as phases;

    let flavor = phases::Flavor::from_mode(mode);
    let homes = phases::resolve_homes(data_dir, into, &flavor)?;
    let mut source = phases::resolve_source(&homes, &flavor)?;
    let identity = phases::load_identity(&homes, &source, api_url, env)?;
    if phases::try_restore_cloud_generation(
        &homes,
        &mut source,
        &identity,
        &flavor,
        json_only,
        progress,
    )? {
        return Ok(());
    }
    let remote_descriptor = phases::discover_remote_descriptor(&flavor, &identity, &source)?;
    let source_db_hash = phases::source_db_hash(&source, remote_descriptor.as_ref());
    let data_path = phases::prepare_destination(&homes, &source, &identity)?;
    let store =
        phases::open_destination_store(&homes, &identity, remote_descriptor.as_ref(), &data_path)?;
    let mut clients = phases::build_cloud_clients(&identity, &source_db_hash)?;
    clients.auth = phases::bind_generation_reader(&clients.auth, &source)?;
    let engine = phases::build_engine(&identity, &clients, &store)?;
    let resume_report = source.resume_report.take();
    let applied = phases::Apply {
        flavor: &flavor,
        homes: &homes,
        source: &source,
        remote_descriptor: remote_descriptor.as_ref(),
        source_db_hash: &source_db_hash,
        identity: &identity,
        clients: &clients,
        store: &store,
        engine: &engine,
        data_path: &data_path,
        progress,
    }
    .run(resume_report)?;
    let continuation = phases::verify_legacy_continuation(&clients, &source)?;
    let remote_ready = phases::finalize_destination(
        &homes,
        &flavor,
        &applied,
        remote_descriptor.as_ref(),
        &source_db_hash,
    )?;
    let remote_ready = merge_restore_continuation(remote_ready, continuation);
    phases::render_report(&homes, &flavor, &applied, remote_ready, json_only)
}

fn merge_restore_continuation(
    ready: Option<serde_json::Value>,
    continuation: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    ready.map(|mut ready| {
        if let Some(continuation) = continuation {
            ready["cloud_continuation"] = continuation;
        }
        ready
    })
}

#[path = "restore_guards.rs"]
mod restore_guards;
#[path = "restore_phases.rs"]
mod restore_phases;
pub(crate) use restore_guards::*;
#[path = "restore_remote_discovery.rs"]
mod restore_remote_discovery;
pub(crate) use restore_remote_discovery::*;

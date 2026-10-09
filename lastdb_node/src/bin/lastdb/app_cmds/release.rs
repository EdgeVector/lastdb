//! Themed module split from the parent.

// The handlers take the parsed CLI fields by value: the dispatcher destructures
// an owned `AppCommand`, so borrowing would only add clones at the call sites.
#![allow(clippy::needless_pass_by_value)]

use super::*;

/// The Host Track tree for one app. `--host-track-root` wins, then
/// `$LASTDB_HOST_TRACK_ROOT`, then `~/.host-track`.
pub(crate) fn host_track_for(
    app_id: &str,
    root: Option<PathBuf>,
) -> lastdb_node::app_release_host::HostTrack {
    use lastdb_node::app_release_host::HostTrack;
    HostTrack::new(
        root.unwrap_or_else(HostTrack::default_root),
        app_id.to_string(),
    )
}

/// The DESIRED release id: `--desired` when given, otherwise one anonymous
/// channel read. A channel that does not resolve leaves `desired` empty,
/// which drops the status out of CURRENT rather than inventing a value.
pub(crate) fn resolve_desired(
    desired: Option<String>,
    app_id: &str,
    channel: &str,
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<Option<String>, String> {
    if let Some(desired) = desired {
        return Ok(Some(desired));
    }
    let url = resolve_schema_service_url(schema_url, env)?;
    let registry = lastdb_node::app_release_host::ReleaseRegistryClient::new(&url);
    let read = block_on_app(async { Ok(registry.get_channel(app_id, channel).await) })?;
    match read {
        Ok(read) => Ok(Some(read.release_id)),
        Err(e) => {
            eprintln!("note: channel read failed ({e}); DESIRED is unknown");
            Ok(None)
        }
    }
}

/// One check cycle: read the channel, read the active release's revocation,
/// run the four-way check, and restore the prior verified release on drift.
///
/// This is the unit `--watch` repeats every `PROBE_INTERVAL`. The revocation
/// read rides the same cycle as the channel read, so a revoked active
/// release runs the drift path without a second mechanism.
pub(crate) fn release_check_cycle(
    host: &lastdb_node::app_release_host::HostTrack,
    app_id: &str,
    channel: &str,
    desired: Option<String>,
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<lastdb_node::app_release_host::DriftOutcome, String> {
    let desired = resolve_desired(desired, app_id, channel, schema_url.clone(), env)?;
    let active_revoked = active_release_is_revoked(host, schema_url, env)?;
    lastdb_node::app_release_host::check_drift_and_restore(host, app_id, desired, active_revoked)
}

pub(crate) fn print_drift_outcome(
    outcome: &lastdb_node::app_release_host::DriftOutcome,
    json: bool,
) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(outcome).unwrap_or_default()
        );
        return;
    }
    println!("status: {}", outcome.before.status.label());
    if let Some(restored) = &outcome.restored_to {
        println!("restored to prior verified release: {restored}");
    }
    if let Some(after) = &outcome.after {
        println!("after restore: {}", after.status.label());
    }
}

/// Is the currently active release revoked? One anonymous release read on
/// the same cycle as the channel read. A read that fails leaves the answer
/// `false` — an unreachable registry is not evidence of a revocation.
pub(crate) fn active_release_is_revoked(
    host: &lastdb_node::app_release_host::HostTrack,
    schema_url: Option<String>,
    env: Option<&str>,
) -> Result<bool, String> {
    let Some(active) = host.active_release_id() else {
        return Ok(false);
    };
    let url = resolve_schema_service_url(schema_url, env)?;
    let registry = lastdb_node::app_release_host::ReleaseRegistryClient::new(&url);
    let read = block_on_app(async { Ok(registry.get_release(&active).await) })?;
    match read {
        Ok(release) => Ok(release.revoked),
        Err(e) => {
            eprintln!("note: revocation read failed ({e}); treating the release as not revoked");
            Ok(false)
        }
    }
}

pub(crate) fn print_four_way(proof: &lastdb_node::app_release_host::FourWayProof, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(proof).unwrap_or_default()
        );
        return;
    }
    println!("desired:   {}", proof.desired.as_deref().unwrap_or("-"));
    println!("installed: {}", proof.installed.as_deref().unwrap_or("-"));
    println!("active:    {}", proof.active.as_deref().unwrap_or("-"));
    println!("observed:  {}", proof.observed.as_deref().unwrap_or("-"));
    println!(
        "probe:     {}",
        if proof.probe.is_green() {
            "green"
        } else {
            "red"
        }
    );
    println!("status:    {}", proof.status.label());
}

pub(crate) fn crypto_env_for(env: Option<&str>) -> Result<app_identity_crypto::Env, String> {
    match env {
        Some("dev") | None => Ok(app_identity_crypto::Env::Dev),
        Some("prod") => Ok(app_identity_crypto::Env::Prod),
        Some(other) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
    }
}

pub(crate) fn block_on_app<T>(
    fut: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
    runtime.block_on(fut)
}

pub(crate) fn app_release_install(
    app_id: &str,
    channel: &str,
    env: Option<String>,
    schema_url: Option<String>,
    host_track_root: Option<PathBuf>,
    json: bool,
) -> Result<(), String> {
    let schema_service = resolve_schema_service_url(schema_url, env.as_deref())?;
    let host = host_track_for(app_id, host_track_root);
    let registry = lastdb_node::app_release_host::ReleaseRegistryClient::new(&schema_service);
    let outcome = block_on_app(lastdb_node::app_release_host::install_and_activate(
        &registry, &host, app_id, channel,
    ))?;
    if json {
        print_pretty_json(&outcome);
    } else {
        println!("activated: {} {}", outcome.app_id, outcome.release_id);
        println!("  identity: {}", outcome.execution_identity);
        println!("  version dir: {}", outcome.version_dir);
        println!("  channel generation: {}", outcome.channel_generation);
        if !outcome.pruned.is_empty() {
            println!("  pruned: {}", outcome.pruned.join(", "));
        }
        // Activation order step 6. A release that has not started
        // yet reads UNKNOWN here; the recurring check promotes it
        // once the process writes its observation.
        println!("  status: {}", outcome.proof.status.label());
    }
    Ok(())
}

pub(crate) fn app_release_status(
    app_id: &str,
    channel: &str,
    env: Option<String>,
    schema_url: Option<String>,
    desired: Option<String>,
    host_track_root: Option<PathBuf>,
    json: bool,
) -> Result<(), String> {
    let host = host_track_for(app_id, host_track_root);
    let desired = resolve_desired(desired, app_id, channel, schema_url, env.as_deref())?;
    let proof = lastdb_node::app_release_host::prove_four_way(&host, desired);
    print_four_way(&proof, json);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn app_release_check(
    app_id: &str,
    channel: &str,
    env: Option<String>,
    schema_url: Option<String>,
    desired: Option<String>,
    host_track_root: Option<PathBuf>,
    json: bool,
    watch: bool,
    interval_secs: Option<u64>,
    cycles: Option<u64>,
) -> Result<(), String> {
    let host = host_track_for(app_id, host_track_root);
    let interval = lastdb_node::app_release_host::check_interval(interval_secs);
    // Without `--watch` this is one cycle, which is what it always
    // was. With it, the same cycle repeats on the operator interval:
    // the channel read, the revocation read, the four-way check, and
    // the restore all ride one cadence.
    let limit = if watch { cycles } else { Some(1) };
    let mut run = 0u64;
    loop {
        if run > 0 {
            std::thread::sleep(interval);
        }
        run += 1;
        let outcome = release_check_cycle(
            &host,
            app_id,
            channel,
            desired.clone(),
            schema_url.clone(),
            env.as_deref(),
        )?;
        print_drift_outcome(&outcome, json);
        if limit.is_some_and(|max| run >= max) {
            return Ok(());
        }
    }
}

pub(crate) fn app_dev_status(
    app_id: String,
    workspace_id: String,
    dev_session_id: String,
    workspace: &Path,
    json: bool,
) -> Result<(), String> {
    use lastdb_node::app_release_host::ExecutionIdentity;
    let identity = ExecutionIdentity::Dev {
        app_id,
        workspace_id,
        dev_session_id,
    };
    let status = lastdb_node::app_release_host::dev_status(&identity, workspace);
    let grant = lastdb_node::app_release_host::CapabilityScope::of(&identity, workspace);
    if json {
        println!(
            "{}",
            serde_json::json!({
                "execution_identity": identity.to_string(),
                "status": status.label(),
                "grant": grant,
            })
        );
    } else {
        println!("identity: {identity}");
        println!("status: {}", status.label());
        println!("grant: workspace scope at {}", grant.root());
    }
    Ok(())
}

pub(crate) fn app_release_observe(
    app_id: &str,
    app_uuid: String,
    release_id: String,
    activation_epoch: u64,
    host_track_root: Option<PathBuf>,
    hold_secs: Option<u64>,
) -> Result<(), String> {
    use lastdb_node::app_release_host::ExecutionIdentity;
    let host = host_track_for(app_id, host_track_root);
    let identity = ExecutionIdentity::Release {
        app_uuid,
        release_id,
        activation_epoch,
    };
    host.write_observation(&identity)?;
    println!("observing as {identity} (pid {})", std::process::id());
    if let Some(secs) = hold_secs {
        std::thread::sleep(std::time::Duration::from_secs(secs));
    }
    Ok(())
}

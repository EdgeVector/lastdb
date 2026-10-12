use super::*;

/// `lastdb app release-publish`, `release-channel` and `release-revoke`: the
/// registry-writing release subcommands. The rest go to
/// [`app_release_local_command`].
pub(super) fn app_release_command(home: &Path, action: AppCommand) -> Result<(), String> {
    match action {
        AppCommand::ReleasePublish {
            manifest,
            app_uuid,
            source_commit,
            artifact,
            artifact_url,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_publish(
            home,
            ReleasePublishInput {
                manifest_path: &manifest,
                app_uuid: &app_uuid,
                source_commit: &source_commit,
                artifact: &artifact,
                artifact_url: &artifact_url,
            },
            RegistryFlags {
                env,
                schema_url,
                api_url,
                key_file,
                api_key,
            },
            json,
        ),
        AppCommand::ReleaseChannel {
            app_id,
            channel,
            release_id,
            generation,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_channel(
            home,
            &app_id,
            &channel,
            &release_id,
            generation,
            RegistryFlags {
                env,
                schema_url,
                api_url,
                key_file,
                api_key,
            },
            json,
        ),
        AppCommand::ReleaseRevoke {
            app_id,
            release_id,
            reason,
            env,
            schema_url,
            api_url,
            key_file,
            api_key,
            json,
        } => app_release_revoke(
            home,
            &app_id,
            &release_id,
            reason.as_deref(),
            RegistryFlags {
                env,
                schema_url,
                api_url,
                key_file,
                api_key,
            },
            json,
        ),
        action => app_release_local_command(action),
    }
}

/// Release and dev-status subcommands that read or observe the local host
/// (`release-install`, `release-status`, `release-check`, `dev-status`,
/// `release-observe`).
fn app_release_local_command(action: AppCommand) -> Result<(), String> {
    match action {
        AppCommand::ReleaseInstall {
            app_id,
            channel,
            env,
            schema_url,
            host_track_root,
            json,
        } => app_release_install(&app_id, &channel, env, schema_url, host_track_root, json),
        AppCommand::ReleaseStatus {
            app_id,
            channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
        } => app_release_status(
            &app_id,
            &channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
        ),
        AppCommand::ReleaseCheck {
            app_id,
            channel,
            env,
            schema_url,
            desired,
            host_track_root,
            json,
            watch,
            interval_secs,
            cycles,
        } => app_release_check(
            &app_id,
            &channel,
            env,
            schema_url,
            desired,
            host_track_root,
            CheckRun {
                watch,
                interval_secs,
                cycles,
                json,
            },
        ),
        AppCommand::DevStatus {
            app_id,
            workspace_id,
            dev_session_id,
            workspace,
            json,
        } => app_dev_status(app_id, workspace_id, dev_session_id, &workspace, json),
        AppCommand::ReleaseObserve {
            app_id,
            app_uuid,
            release_id,
            activation_epoch,
            host_track_root,
            hold_secs,
        } => app_release_observe(
            &app_id,
            app_uuid,
            release_id,
            activation_epoch,
            host_track_root,
            hold_secs,
        ),
        _ => Err("not a release subcommand".to_string()),
    }
}

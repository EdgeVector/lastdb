use super::*;

pub(super) fn run(
    home: &Path,
    runtime: tokio::runtime::Runtime,
    action: CloudResumePrimaryCommand,
) -> Result<(), String> {
    match action {
        CloudResumePrimaryCommand::Plan { copy_home, json } => {
            runtime.block_on(cloud_primary_resume::plan(&copy_home, home, json))
        }
        CloudResumePrimaryCommand::Start {
            fresh_from_local,
            accept_local_damage,
            json,
        } => {
            drop(runtime);
            cloud_resume_primary_job(
                home,
                false,
                fresh_from_local,
                accept_local_damage,
                None,
                None,
                json,
            )
        }
        CloudResumePrimaryCommand::Finish {
            restore_manifest_sha256,
            restore_home,
            json,
        } => {
            drop(runtime);
            cloud_resume_primary_job(
                home,
                false,
                false,
                false,
                Some(&restore_manifest_sha256),
                restore_home.as_deref(),
                json,
            )
        }
        CloudResumePrimaryCommand::Status { json } => {
            drop(runtime);
            cloud_resume_primary_job(home, true, false, false, None, None, json)
        }
    }
}

fn cloud_resume_primary_job(
    home: &Path,
    status: bool,
    fresh_from_local: bool,
    accept_local_damage: bool,
    restore_manifest_sha256: Option<&str>,
    restore_home: Option<&Path>,
    json_only: bool,
) -> Result<(), String> {
    let socket = home.join("data/folddb.sock");
    if !socket.exists() {
        return Err("primary resume job requires a running daemon".into());
    }
    let response = post_json(
        &socket,
        "/api/sync/cloud-resume-primary",
        &serde_json::json!({
            "status": status,
            "fresh_from_local": fresh_from_local,
            "accept_local_damage": accept_local_damage,
            "finish": restore_manifest_sha256.is_some(),
            "restore_manifest_sha256": restore_manifest_sha256,
            "restore_home": restore_home.map(|path| path.to_string_lossy().to_string()),
        }),
    )?;
    let value = parse_json_response(&response, "cloud-resume-primary")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }
    let data = value.get("data").unwrap_or(&value);
    println!(
        "Primary cloud resume: {}",
        data["state"].as_str().unwrap_or("unknown")
    );
    if let Some(id) = data["job_id"].as_str() {
        println!("  job: {id}");
    }
    if let Some(error) = data["error"].as_str() {
        println!("  error: {error}");
    }
    Ok(())
}

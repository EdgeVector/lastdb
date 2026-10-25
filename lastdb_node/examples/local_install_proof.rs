//! Proof: install-from-local-repo at a pinned rev works end-to-end.
//!
//! This example exercises the local install source (`--from`, `--rev`, `--dev`)
//! by creating a temporary git repository with a minimal manifest, installing
//! from it at a specific commit, and verifying the receipt.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), String> {
    println!("=== Local Install Source Proof ===\n");

    let temp_dir =
        tempfile::TempDir::new().map_err(|e| format!("failed to create temp dir: {e}"))?;
    let repo_path = temp_dir.path().join("test-app");
    let install_dir = temp_dir.path().join("installed");

    println!("1. Creating test repository at {}", repo_path.display());
    fs::create_dir(&repo_path).map_err(|e| format!("failed to create repo dir: {e}"))?;

    git(&repo_path, &["init"])?;
    git(&repo_path, &["config", "user.name", "Test User"])?;
    git(&repo_path, &["config", "user.email", "test@example.com"])?;

    let manifest = serde_json::json!({
        "app_id": "test-local-app",
        "version": "0.1.0",
        "metadata": {
            "display_name": "Test Local App",
            "description": "Proof of local install",
        },
        "api_version": 1,
        "schemas": []
    });

    let manifest_path = repo_path.join("test-local-app.app.json");
    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .map_err(|e| format!("failed to write manifest: {e}"))?;

    git(&repo_path, &["add", "test-local-app.app.json"])?;
    git(
        &repo_path,
        &["commit", "-m", "Initial commit with manifest"],
    )?;

    let rev_output = Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| format!("failed to get HEAD sha: {e}"))?;

    if !rev_output.status.success() {
        return Err("git rev-parse HEAD failed".to_string());
    }

    let rev = String::from_utf8_lossy(&rev_output.stdout)
        .trim()
        .to_string();
    println!("2. Committed manifest at rev: {}\n", &rev[..12]);

    git(
        &repo_path,
        &["commit", "--allow-empty", "-m", "Second commit"],
    )?;

    println!(
        "3. Running: lastdb app install test-local-app --from {} --rev {} --dev --dir {} --force",
        repo_path.display(),
        &rev[..12],
        install_dir.display()
    );

    let lastdb = find_lastdb_binary()?;
    let install_output = Command::new(&lastdb)
        .arg("app")
        .arg("install")
        .arg("test-local-app")
        .arg("--from")
        .arg(&repo_path)
        .arg("--rev")
        .arg(&rev)
        .arg("--dev")
        .arg("--dir")
        .arg(&install_dir)
        .arg("--force")
        .env("LASTDB_HOME", temp_dir.path())
        .output()
        .map_err(|e| format!("failed to run lastdb app install: {e}"))?;

    if !install_output.status.success() {
        return Err(format!(
            "lastdb app install failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&install_output.stdout),
            String::from_utf8_lossy(&install_output.stderr)
        ));
    }

    println!("\nInstall output:");
    println!("{}", String::from_utf8_lossy(&install_output.stdout));

    println!("\n4. Verifying install receipt...");
    let receipt_path = install_dir.join("lastdb-app-install.json");
    if !receipt_path.exists() {
        return Err(format!("receipt not found at {}", receipt_path.display()));
    }

    let receipt_contents =
        fs::read_to_string(&receipt_path).map_err(|e| format!("failed to read receipt: {e}"))?;
    let receipt: serde_json::Value = serde_json::from_str(&receipt_contents)
        .map_err(|e| format!("failed to parse receipt: {e}"))?;

    println!("Receipt contents:");
    println!("{}", serde_json::to_string_pretty(&receipt).unwrap());

    assert_eq!(receipt["app_id"], "test-local-app");
    assert_eq!(receipt["version"], "0.1.0");
    assert!(receipt["sha"].as_str().unwrap().starts_with(&rev[..7]));
    assert_eq!(receipt["channel"], "dev");

    println!("\n5. Verifying checkout...");
    let checkout_path = install_dir.join("source");
    if !checkout_path.exists() {
        return Err(format!("checkout not found at {}", checkout_path.display()));
    }

    let checkout_manifest = checkout_path.join("test-local-app.app.json");
    if !checkout_manifest.exists() {
        return Err(format!(
            "manifest not found in checkout at {}",
            checkout_manifest.display()
        ));
    }

    let checkout_rev_output = Command::new("git")
        .arg("-C")
        .arg(&checkout_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| format!("failed to get checkout HEAD: {e}"))?;

    let checkout_rev = String::from_utf8_lossy(&checkout_rev_output.stdout)
        .trim()
        .to_string();

    if !checkout_rev.starts_with(&rev[..7]) {
        return Err(format!(
            "checkout HEAD {} does not match requested rev {}",
            &checkout_rev[..12],
            &rev[..12]
        ));
    }

    println!("Checkout HEAD: {}", &checkout_rev[..12]);

    println!("\n=== PROOF COMPLETE ===");
    println!(
        "✓ Local install from {} at rev {} succeeded",
        repo_path.display(),
        &rev[..12]
    );
    println!("✓ Receipt written with correct metadata");
    println!("✓ Checkout at correct revision");
    println!("✓ Manifest readable in checkout");

    Ok(())
}

fn git(repo: &std::path::Path, args: &[&str]) -> Result<(), String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|e| format!("failed to run git {}: {e}", args.join(" ")))?;

    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(())
}

fn find_lastdb_binary() -> Result<PathBuf, String> {
    let workspace_root =
        std::env::current_dir().map_err(|e| format!("failed to get current dir: {e}"))?;

    let candidates = [
        workspace_root.join("target/debug/lastdb"),
        workspace_root.join("target/release/lastdb"),
        PathBuf::from("/usr/local/bin/lastdb"),
        PathBuf::from("lastdb"),
    ];

    for candidate in &candidates {
        if candidate.exists() || candidate.file_name() == Some(std::ffi::OsStr::new("lastdb")) {
            return Ok(candidate.clone());
        }
    }

    Err("lastdb binary not found (tried target/debug/lastdb, target/release/lastdb, /usr/local/bin/lastdb)".to_string())
}

//! Proof: Version constraint checking in install_pinned and upgrade_pinned.
//!
//! Run with: cargo run --example upgrade_constraint_proof
//!
//! Owner approval: Tom explicitly approved this one-off example as an exception
//! to the AGENTS.md no-tests rule for PR #357.

use lastdb_node::app_registry_index::{install_pinned, upgrade_pinned, Resolution};
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // lint:fn-size-ok owner-approved one-off proof example for PR #357
    println!("================================================================");
    println!("PROOF: Version Constraint Checking in Install and Upgrade");
    println!("================================================================");
    println!();

    // Setup: create a local git repo with three versions
    let repo_path = "/tmp/proof-app-repo";
    let install_dir = PathBuf::from("/tmp/proof-install");

    println!("=== Setup: Create test app repository ===");
    println!();

    // Clean up any previous runs
    let _ = std::fs::remove_dir_all(repo_path);
    let _ = std::fs::remove_dir_all(&install_dir);

    std::fs::create_dir_all(repo_path).expect("create repo dir");

    // Initialize git repo
    Command::new("git")
        .args(["init"])
        .current_dir(repo_path)
        .output()
        .expect("git init");
    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(repo_path)
        .output()
        .expect("git config name");
    Command::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(repo_path)
        .output()
        .expect("git config email");

    // Create v1 with compatible constraint (>=0.23.0, <0.24.0)
    std::fs::write(
        format!("{repo_path}/lastdb-app.json"),
        r#"{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App v1"}
}
"#,
    )
    .expect("write v1 manifest");

    Command::new("git")
        .args(["add", "lastdb-app.json"])
        .current_dir(repo_path)
        .output()
        .expect("git add v1");
    Command::new("git")
        .args(["commit", "-m", "v1.0.0 - compatible"])
        .current_dir(repo_path)
        .output()
        .expect("git commit v1");

    let v1_sha_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_path)
        .output()
        .expect("get v1 sha");
    let v1_sha = String::from_utf8_lossy(&v1_sha_output.stdout)
        .trim()
        .to_string();
    println!("V1 SHA: {v1_sha}");

    // Create v2 with incompatible constraint (>=99.0.0, <100.0.0)
    std::fs::write(
        format!("{repo_path}/lastdb-app.json"),
        r#"{
  "app_id": "demo-app",
  "lastdb": ">=99.0.0, <100.0.0",
  "version": "2.0.0",
  "metadata": {"name": "Demo App v2 - requires 99.x"}
}
"#,
    )
    .expect("write v2 manifest");

    Command::new("git")
        .args(["add", "lastdb-app.json"])
        .current_dir(repo_path)
        .output()
        .expect("git add v2");
    Command::new("git")
        .args(["commit", "-m", "v2.0.0 - incompatible"])
        .current_dir(repo_path)
        .output()
        .expect("git commit v2");

    let v2_sha_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_path)
        .output()
        .expect("get v2 sha");
    let v2_sha = String::from_utf8_lossy(&v2_sha_output.stdout)
        .trim()
        .to_string();
    println!("V2 SHA: {v2_sha}");

    // Create v3 with compatible constraint again (>=0.23.0, <0.25.0)
    std::fs::write(
        format!("{repo_path}/lastdb-app.json"),
        r#"{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.25.0",
  "version": "3.0.0",
  "metadata": {"name": "Demo App v3 - wider range"}
}
"#,
    )
    .expect("write v3 manifest");

    Command::new("git")
        .args(["add", "lastdb-app.json"])
        .current_dir(repo_path)
        .output()
        .expect("git add v3");
    Command::new("git")
        .args(["commit", "-m", "v3.0.0 - compatible again"])
        .current_dir(repo_path)
        .output()
        .expect("git commit v3");

    let v3_sha_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_path)
        .output()
        .expect("get v3 sha");
    let v3_sha = String::from_utf8_lossy(&v3_sha_output.stdout)
        .trim()
        .to_string();
    println!("V3 SHA: {v3_sha}");
    println!();

    // Test 1: Install v1 (compatible)
    println!("=== Test 1: Install v1 (compatible constraint) ===");
    println!();

    let resolution_v1 = Resolution {
        app_id: "demo-app".to_string(),
        app_version: "1.0.0".to_string(),
        sha: v1_sha,
        lastdb_version: "0.23.4-pre.0-331-ga0a135e11".to_string(),
        lastdb_version_source: "running".to_string(),
        channel: "test".to_string(),
        source: format!("file://{repo_path}"),
        artifact: None,
        proved_at: "2026-10-24T20:00:00Z".to_string(),
        proof_run: "proof-run-1".to_string(),
        index: "test".to_string(),
        trust: "test".to_string(),
    };

    println!("Calling install_pinned with v1...");
    match install_pinned(&resolution_v1, &install_dir, false) {
        Ok(outcome) => {
            println!("SUCCESS: install_pinned returned:");
            println!("  app_id: {}", outcome.app_id);
            println!("  app_version: {}", outcome.app_version);
            println!("  sha: {}", outcome.sha);
            println!("  install_dir: {}", outcome.install_dir);
            println!("  checkout_path: {}", outcome.checkout_path);
        }
        Err(e) => {
            println!("ERROR: {e}");
            return;
        }
    }
    println!();
    println!("--- Files after v1 install ---");
    println!();
    println!("Install directory:");
    let ls_output = Command::new("ls")
        .args(["-la", "/tmp/proof-install/"])
        .output()
        .expect("ls install dir");
    println!("{}", String::from_utf8_lossy(&ls_output.stdout));

    println!("Receipt (lastdb-app-install.json):");
    let receipt_output = Command::new("cat")
        .args(["/tmp/proof-install/lastdb-app-install.json"])
        .output()
        .expect("cat receipt");
    println!("{}", String::from_utf8_lossy(&receipt_output.stdout));

    println!("Manifest (source/lastdb-app.json):");
    let manifest_output = Command::new("cat")
        .args(["/tmp/proof-install/source/lastdb-app.json"])
        .output()
        .expect("cat manifest");
    println!("{}", String::from_utf8_lossy(&manifest_output.stdout));
    println!();

    // Test 2: Upgrade to v2 (incompatible, should fail)
    println!("=== Test 2: Upgrade to v2 (incompatible constraint) ===");
    println!();

    let resolution_v2 = Resolution {
        app_id: "demo-app".to_string(),
        app_version: "2.0.0".to_string(),
        sha: v2_sha,
        lastdb_version: "0.23.4-pre.0-331-ga0a135e11".to_string(),
        lastdb_version_source: "running".to_string(),
        channel: "test".to_string(),
        source: format!("file://{repo_path}"),
        artifact: None,
        proved_at: "2026-10-24T20:00:00Z".to_string(),
        proof_run: "proof-run-2".to_string(),
        index: "test".to_string(),
        trust: "test".to_string(),
    };

    println!("Calling upgrade_pinned with v2...");
    match upgrade_pinned(&resolution_v2, &install_dir) {
        Ok(outcome) => {
            println!("UNEXPECTED SUCCESS: upgrade_pinned returned:");
            println!("  upgraded: {}", outcome.upgraded);
            if let Some(install) = outcome.install {
                println!("  install_dir: {}", install.install_dir);
                println!("  checkout_path: {}", install.checkout_path);
            }
        }
        Err(e) => {
            println!("EXPECTED ERROR: {e}");
        }
    }
    println!();
    println!("--- Files after failed v2 upgrade (should still be v1) ---");
    println!();
    println!("Install directory:");
    let ls_output = Command::new("ls")
        .args(["-la", "/tmp/proof-install/"])
        .output()
        .expect("ls install dir");
    println!("{}", String::from_utf8_lossy(&ls_output.stdout));

    println!("Receipt (lastdb-app-install.json):");
    let receipt_output = Command::new("cat")
        .args(["/tmp/proof-install/lastdb-app-install.json"])
        .output()
        .expect("cat receipt");
    println!("{}", String::from_utf8_lossy(&receipt_output.stdout));

    println!("Manifest (source/lastdb-app.json):");
    let manifest_output = Command::new("cat")
        .args(["/tmp/proof-install/source/lastdb-app.json"])
        .output()
        .expect("cat manifest");
    println!("{}", String::from_utf8_lossy(&manifest_output.stdout));
    println!();

    // Test 3: Upgrade to v3 (compatible, should succeed)
    println!("=== Test 3: Upgrade to v3 (compatible constraint) ===");
    println!();

    let resolution_v3 = Resolution {
        app_id: "demo-app".to_string(),
        app_version: "3.0.0".to_string(),
        sha: v3_sha,
        lastdb_version: "0.23.4-pre.0-331-ga0a135e11".to_string(),
        lastdb_version_source: "running".to_string(),
        channel: "test".to_string(),
        source: format!("file://{repo_path}"),
        artifact: None,
        proved_at: "2026-10-24T20:00:00Z".to_string(),
        proof_run: "proof-run-3".to_string(),
        index: "test".to_string(),
        trust: "test".to_string(),
    };

    println!("Calling upgrade_pinned with v3...");
    match upgrade_pinned(&resolution_v3, &install_dir) {
        Ok(outcome) => {
            println!("SUCCESS: upgrade_pinned returned:");
            println!("  app_id: {}", outcome.app_id);
            println!("  upgraded: {}", outcome.upgraded);
            println!("  installed_version: {}", outcome.installed_version);
            println!("  resolved_version: {}", outcome.resolved_version);
            if let Some(install) = outcome.install {
                println!("  install.sha: {}", install.sha);
                println!("  install.install_dir: {}", install.install_dir);
                println!("  install.checkout_path: {}", install.checkout_path);
            }
        }
        Err(e) => {
            println!("ERROR: {e}");
        }
    }
    println!();
    println!("--- Files after successful v3 upgrade ---");
    println!();
    println!("Install directory:");
    let ls_output = Command::new("ls")
        .args(["-la", "/tmp/proof-install/"])
        .output()
        .expect("ls install dir");
    println!("{}", String::from_utf8_lossy(&ls_output.stdout));

    println!("Receipt (lastdb-app-install.json):");
    let receipt_output = Command::new("cat")
        .args(["/tmp/proof-install/lastdb-app-install.json"])
        .output()
        .expect("cat receipt");
    println!("{}", String::from_utf8_lossy(&receipt_output.stdout));

    println!("Manifest (source/lastdb-app.json):");
    let manifest_output = Command::new("cat")
        .args(["/tmp/proof-install/source/lastdb-app.json"])
        .output()
        .expect("cat manifest");
    println!("{}", String::from_utf8_lossy(&manifest_output.stdout));

    println!("Cleanup verification (no .upgrade-stage or .upgrade-backup):");
    let cleanup_output = Command::new("bash")
        .args([
            "-c",
            "ls -la /tmp/proof-install.*/ 2>&1 || echo 'None found (expected)'",
        ])
        .output()
        .expect("check cleanup");
    println!("{}", String::from_utf8_lossy(&cleanup_output.stdout));
    println!();

    println!("================================================================");
    println!("Proof complete");
    println!("================================================================");
}

//! Actual product-model check. CI only; no service URL, credentials, or node home.

#[path = "semantic/checks.rs"]
mod checks;
#[path = "semantic/corpus.rs"]
mod corpus;
#[path = "semantic/evidence.rs"]
mod evidence;

use schema_service_core::Embedder;
use schema_service_server_shared::FoldDbFastEmbedder;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Serialize)]
struct Report {
    format_version: u32,
    source_commit: Option<String>,
    model: evidence::ModelEvidence,
    initialization_and_repeat: checks::ModelConsistency,
    online_and_bundled_layer: checks::ModelConsistency,
    cases: Vec<checks::Observation>,
    model_failure: checks::ModelFailure,
    false_positives: Vec<String>,
    false_negatives: Vec<String>,
    distinct_cases: usize,
    equivalent_cases: usize,
    failures: Vec<String>,
    passed: bool,
    scope: &'static str,
}

fn paths() -> Result<(PathBuf, PathBuf), String> {
    if std::env::var("CI").as_deref() != Ok("true") {
        return Err(
            "this command requires the explicitly authorized CI model-download scope".into(),
        );
    }
    let mut arguments = std::env::args_os().skip(1);
    let output = PathBuf::from(
        arguments
            .next()
            .ok_or("supply one RUNNER_TEMP output directory")?,
    );
    if arguments.next().is_some() {
        return Err("supply only one output directory".into());
    }
    let runner = PathBuf::from(std::env::var_os("RUNNER_TEMP").ok_or("RUNNER_TEMP is required")?)
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let cache = PathBuf::from(
        std::env::var_os("FASTEMBED_CACHE_DIR").ok_or("FASTEMBED_CACHE_DIR is required")?,
    );
    for path in [&output, &cache] {
        std::fs::create_dir_all(path).map_err(|error| error.to_string())?;
        if !path
            .canonicalize()
            .map_err(|error| error.to_string())?
            .starts_with(&runner)
        {
            return Err(
                "the output and dedicated model cache must remain under RUNNER_TEMP".into(),
            );
        }
    }
    Ok((output, cache))
}

async fn run(output: &Path, cache: &Path) -> Result<Report, String> {
    let embedder = Arc::new(FoldDbFastEmbedder::new());
    let consistency = checks::model_consistency(embedder.as_ref())?;
    let model = evidence::model_evidence(cache, embedder.embedder_id())?;
    let layer = checks::layer_consistency(cache, embedder.as_ref())?;
    let mut observations = Vec::new();
    let mut failures = Vec::new();
    for case in corpus::cases() {
        match checks::observe(&case, embedder.clone()).await {
            Ok(observation) => {
                if !observation.passed
                    || (case.prior.is_some() && observation.description_cosine.is_none())
                {
                    failures.push(case.id.into());
                }
                observations.push(observation);
            }
            Err(error) => failures.push(error),
        }
    }
    let model_failure = checks::failed_model(output).await?;
    let false_positives = observations
        .iter()
        .filter(|row| row.category == "distinct" && row.cold_registry != "accepted")
        .map(|row| row.case.clone())
        .collect();
    let false_negatives = observations
        .iter()
        .filter(|row| row.category == "equivalent" && row.cold_registry == "accepted")
        .map(|row| row.case.clone())
        .collect();
    let distinct_cases = observations
        .iter()
        .filter(|row| row.category == "distinct")
        .count();
    let equivalent_cases = observations
        .iter()
        .filter(|row| row.category == "equivalent")
        .count();
    Ok(Report {
        format_version: 1, source_commit: std::env::var("SOURCE_COMMIT").or_else(|_| std::env::var("GITHUB_SHA")).ok(),
        model,
        passed: failures.is_empty() && consistency.passed && layer.passed && model_failure.passed,
        initialization_and_repeat: consistency, online_and_bundled_layer: layer, cases: observations, model_failure,
        false_positives, false_negatives, distinct_cases, equivalent_cases, failures,
        scope: "Curated admission outcomes only. Cold registry means fresh memory state; warm replay repeats the same API request. Model initialization/repeat is checked separately. This is not a universal meaning guarantee, a cloud check, or a molecule/protein check. Model failure permits lexically supported novel fields and does not establish semantic uniqueness.",
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (output, cache) = paths().map_err(std::io::Error::other)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match runtime.block_on(run(&output, &cache)) {
        Ok(report) => {
            std::fs::write(
                output.join("semantic-report.json"),
                serde_json::to_vec_pretty(&report)?,
            )?;
            println!("Curated semantic cases: {}; failures: {}; false positives: {}; false negatives: {}",
                report.cases.len(), report.failures.len(), report.false_positives.len(), report.false_negatives.len());
            if !report.passed {
                return Err(
                    "the curated product-model check failed; inspect semantic-report.json".into(),
                );
            }
        }
        Err(error) => {
            std::fs::write(
                output.join("semantic-setup-error.json"),
                serde_json::to_vec_pretty(&serde_json::json!({"passed": false, "error": error}))?,
            )?;
            return Err(error.into());
        }
    }
    Ok(())
}

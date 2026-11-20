//! PASS/FAIL stdout lines and optional JSON summary.

use std::path::Path;
use std::time::Instant;

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioResult {
    pub name: String,
    pub pass: bool,
    pub seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

pub struct ScenarioRun {
    name: &'static str,
    start: Instant,
}

impl ScenarioRun {
    pub fn start(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
        }
    }

    pub fn finish(self, outcome: Result<String, String>) -> ScenarioResult {
        let seconds = self.start.elapsed().as_secs_f64();
        match outcome {
            Ok(detail) => {
                println!(
                    "PASS {} {:.3}s {}",
                    self.name,
                    seconds,
                    detail.replace('\n', " ")
                );
                ScenarioResult {
                    name: self.name.to_owned(),
                    pass: true,
                    seconds,
                    detail: Some(detail),
                }
            }
            Err(err) => {
                println!(
                    "FAIL {} {:.3}s {}",
                    self.name,
                    seconds,
                    err.replace('\n', " ")
                );
                ScenarioResult {
                    name: self.name.to_owned(),
                    pass: false,
                    seconds,
                    detail: Some(err),
                }
            }
        }
    }
}

#[derive(Serialize)]
pub struct Summary {
    pub all_pass: bool,
    pub scenarios: Vec<ScenarioResult>,
}

pub fn write_json(path: &Path, summary: &Summary) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("json-out dir: {e}"))?;
        }
    }
    let text = serde_json::to_string_pretty(summary).map_err(|e| format!("json encode: {e}"))?;
    std::fs::write(path, text).map_err(|e| format!("json-out write: {e}"))
}

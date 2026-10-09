//! Generator: hand-curated cluster classification for every Schema.org
//! property.
//!
//! **Run manually**, not at service startup:
//!
//! ```bash
//! cargo run --bin generate-schema-org-classifications > \
//!   schema_service/crates/core/data/schema_org/classification_overlay.json
//! ```
//!
//! Or drop the explicit redirect:
//!
//! ```bash
//! cargo run --bin generate-schema-org-classifications
//! ```
//! which writes the file in place.
//!
//! The emitted JSON is the committed source of truth for Schema.org
//! classifications. The schema service loads it at cold start via
//! `include_str!` and applies each entry directly — no runtime
//! clustering logic. If you want to re-classify after editing
//! `clusters.rs`, re-run this binary and commit the regenerated JSON
//! diff.
//!
//! ## Priority order
//!
//! Clusters are evaluated top-to-bottom in `CLUSTERS`. First match
//! wins. Put the highest-sensitivity / most-specific clusters first so
//! `patientEmail` lands in the medical cluster, not the email cluster.
//!
//! ## Fail-closed baseline
//!
//! Any Schema.org property that matches no cluster gets written with
//! `sensitivity=4, domain="general", cluster="baseline_fail_closed"`.
//! Over-restriction is recoverable (edit `clusters.rs`, re-run);
//! under-restriction is not (consumers that trusted a low classification
//! would have already leaked).

mod clusters;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use clusters::CLUSTERS;
use schema_service_core::schema_org_seeds::{parse_dump, property_name_to_snake_case};
use serde::Serialize;

/// One classified property in the overlay output.
#[derive(Serialize)]
struct ClassifiedProperty {
    sensitivity_level: u8,
    data_domain: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    interest_category: Option<&'static str>,
    /// Which cluster matched, for traceability during review.
    cluster: &'static str,
}

/// Full overlay document.
#[derive(Serialize)]
struct ClassificationOverlay {
    version: u32,
    generated_at: String,
    source_file: &'static str,
    generator: &'static str,
    cluster_count: usize,
    field_count: usize,
    baseline_fallback_count: usize,
    /// BTreeMap for deterministic key ordering in the emitted JSON.
    fields: BTreeMap<String, ClassifiedProperty>,
}

fn classify(snake_name: &str) -> ClassifiedProperty {
    for cluster in CLUSTERS {
        if cluster.matches(snake_name) {
            return ClassifiedProperty {
                sensitivity_level: cluster.sensitivity,
                data_domain: cluster.data_domain,
                interest_category: cluster.interest_category,
                cluster: cluster.name,
            };
        }
    }
    ClassifiedProperty {
        sensitivity_level: 4,
        data_domain: "general",
        interest_category: None,
        cluster: "baseline_fail_closed",
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dump = parse_dump()?;

    // Collect unique snake_case property names. Schema.org has ~1676
    // properties; after snake_case conversion some collapse to ~1521
    // unique names.
    let mut unique_names: BTreeMap<String, ()> = BTreeMap::new();
    for prop in &dump.properties {
        unique_names.insert(property_name_to_snake_case(&prop.label), ());
    }

    let mut fields: BTreeMap<String, ClassifiedProperty> = BTreeMap::new();
    let mut baseline_fallback_count = 0usize;
    for name in unique_names.keys() {
        let classified = classify(name);
        if classified.cluster == "baseline_fail_closed" {
            baseline_fallback_count += 1;
        }
        fields.insert(name.clone(), classified);
    }

    let cluster_hit_count = fields.len() - baseline_fallback_count;

    let overlay = ClassificationOverlay {
        version: 1,
        generated_at: Utc::now().to_rfc3339(),
        source_file: "schemaorg-current-https.jsonld",
        generator: "schema_service/scripts/generate_schema_org_classifications",
        cluster_count: CLUSTERS.len(),
        field_count: fields.len(),
        baseline_fallback_count,
        fields,
    };

    let json = serde_json::to_string_pretty(&overlay)?;

    // Emit summary to stderr so redirects to the file don't capture it.
    eprintln!(
        "Generated classification overlay:\n  clusters:         {}\n  unique fields:    {}\n  cluster hits:     {}\n  baseline fallbacks: {} (receiving fail-closed sensitivity=4, domain=general)",
        CLUSTERS.len(),
        overlay.field_count,
        cluster_hit_count,
        baseline_fallback_count
    );

    // Write to the in-repo overlay path by default. Callers who want to
    // redirect can pass `--stdout`.
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--stdout") {
        print!("{json}");
        return Ok(());
    }

    let out_path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "..",
        "..",
        "crates",
        "core",
        "data",
        "schema_org",
        "classification_overlay.json",
    ]
    .iter()
    .collect();

    fs::write(&out_path, json)?;
    eprintln!("Wrote {}", out_path.display());
    Ok(())
}

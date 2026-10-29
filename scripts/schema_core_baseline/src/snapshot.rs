//! Assemble the sorted snapshot document.

use serde_json::json;

use crate::normalize;
use crate::release::{write_releases, Fixture};
use crate::state::{canonical_field_rows, identity_rows, prepare};

pub(crate) async fn capture() -> String {
    let fixture = Fixture::load();
    let mut prepared = prepare(&fixture).await;
    let canonical_fields = canonical_field_rows(&prepared.state);
    let (release_rows, channel_generations) = write_releases(&fixture, &mut prepared).await;
    let identities = identity_rows(&prepared);
    let document = normalize(json!({
        "canonical_fields": canonical_fields,
        "channel_generations": channel_generations,
        "identities": identities,
        "operations": prepared.operations,
        "release_rows": release_rows,
    }));
    let mut encoded = serde_json::to_string_pretty(&document).expect("snapshot json");
    encoded.push('\n');
    encoded
}

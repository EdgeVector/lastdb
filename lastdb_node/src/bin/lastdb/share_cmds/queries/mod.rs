//! `lastdb share queries save` and `lastdb share queries reconcile`.

use std::path::PathBuf;

use lastdb_node::saved_query::Receipt;
use lastdb_node::saved_query_run::{self, SaveReceipt, SaveRequest};
use serde_json::json;

use crate::ShareQueriesCommand;

pub(super) fn run(data_dir: Option<PathBuf>, action: ShareQueriesCommand) -> Result<(), String> {
    match action {
        ShareQueriesCommand::Save {
            id,
            app,
            source,
            destination,
            spec,
            spec_version,
            query,
        } => {
            let saved = saved_query_run::save(
                data_dir,
                &SaveRequest {
                    id,
                    app,
                    source,
                    destination,
                    spec,
                    spec_version,
                    query,
                },
            )?;
            print_save(&saved);
        }
        ShareQueriesCommand::Reconcile { id, source } => {
            let receipt = saved_query_run::reconcile(data_dir, &id, &source)?;
            print_receipt(&receipt);
        }
    }
    Ok(())
}

fn print_save(saved: &SaveReceipt) {
    println!(
        "{}",
        json!({
            "id": saved.id,
            "source": saved.source,
            "destination": saved.destination,
            "spec_name": saved.spec_name,
            "spec_version": saved.spec_version,
        })
    );
}

fn print_receipt(receipt: &Receipt) {
    println!(
        "{}",
        json!({
            "added": receipt.added,
            "updated": receipt.updated,
            "removed": receipt.removed,
            "unchanged": receipt.unchanged,
        })
    );
}

//! The drop receipts in `schema_states`.
//!
//! Production has no lister for receipts. This reader pages the raw receipt
//! prefix in small ordered pages and stops on a row it cannot decode.

use std::collections::BTreeSet;

use fold_db::db_operations::schema_store::SCHEMA_DROP_RECEIPT_KEY_PREFIX;
use fold_db::db_operations::SchemaDropReceipt;
use fold_db::storage::traits::NamespacedStore;

use super::identities::Identities;
use super::ReapError;

/// Rows per page of the receipt scan.
pub(crate) const RECEIPT_PAGE_ROWS: usize = 32;

/// The owner app of the dropped schemas.
pub(crate) const OWNER_APP: &str = "lastgit";

pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    end
}

/// Check one page of the scan: size, order and bounds.
pub(crate) fn validate_page(
    page: &[(Vec<u8>, Vec<u8>)],
    cursor: &[u8],
    end: &[u8],
    prefix: &[u8],
) -> Result<(), ReapError> {
    let bad = page.len() > RECEIPT_PAGE_ROWS
        || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || page.iter().any(|(key, _)| {
            key.as_slice() < cursor || key.as_slice() >= end || !key.starts_with(prefix)
        });
    if bad {
        return Err(ReapError::Failed(
            "receipt scan received an invalid or unordered page".to_string(),
        ));
    }
    Ok(())
}

/// Read every drop receipt. A row that does not decode aborts the plan.
pub(crate) async fn read_all(
    store: &dyn NamespacedStore,
) -> Result<Vec<SchemaDropReceipt>, ReapError> {
    let kv = store
        .open_namespace("schema_states")
        .await
        .map_err(|error| ReapError::Failed(format!("open schema_states: {error}")))?;
    let prefix = SCHEMA_DROP_RECEIPT_KEY_PREFIX.as_bytes();
    let end = prefix_end(prefix);
    let mut cursor = prefix.to_vec();
    let mut receipts = Vec::new();
    loop {
        let page = kv
            .scan_range_paged(&cursor, &end, RECEIPT_PAGE_ROWS)
            .await
            .map_err(|error| ReapError::Failed(format!("receipt scan: {error}")))?;
        let Some(last) = page.last().map(|(key, _)| key.clone()) else {
            return Ok(receipts);
        };
        validate_page(&page, &cursor, &end, prefix)?;
        for (key, value) in &page {
            receipts.push(decode_receipt(key, value, prefix)?);
        }
        cursor = last;
        cursor.push(0);
    }
}

/// Decode one receipt row. The row key names the identity, and the body must
/// name the same identity.
fn decode_receipt(key: &[u8], value: &[u8], prefix: &[u8]) -> Result<SchemaDropReceipt, ReapError> {
    let receipt: SchemaDropReceipt = serde_json::from_slice(value).map_err(|error| {
        ReapError::abort(
            "RECEIPT_UNDECODABLE",
            format!("receipt row {:?} does not decode: {error}", lossy(key)),
        )
    })?;
    if key[prefix.len()..] != *receipt.identity.as_bytes() {
        return Err(ReapError::abort(
            "RECEIPT_UNDECODABLE",
            format!(
                "receipt row {:?} names identity {}",
                lossy(key),
                receipt.identity
            ),
        ));
    }
    Ok(receipt)
}

fn lossy(key: &[u8]) -> String {
    String::from_utf8_lossy(key).replace('\0', "\\0")
}

/// What the receipts say about the identities file.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReceiptReport {
    /// Receipts found in the store.
    pub found: usize,
    /// Receipts whose owner app is `lastgit`.
    pub owned_by_lastgit: usize,
    /// Receipts whose identity is a dropped name.
    pub named_in_file: usize,
    /// Listed names that have no receipt.
    pub receiptless: Vec<String>,
}

/// Gate: every `lastgit` receipt must be in the identities file.
///
/// The file is what Tom approved. A receipt of the same owner that the file
/// does not list is a drop nobody approved, so the plan stops and prints it.
pub(crate) fn check_listed(
    receipts: &[SchemaDropReceipt],
    ids: &Identities,
) -> Result<ReceiptReport, ReapError> {
    let unlisted: Vec<&str> = receipts
        .iter()
        .filter(|receipt| receipt.owner_app.as_deref() == Some(OWNER_APP))
        .filter(|receipt| !ids.spellings.contains(&receipt.identity))
        .map(|receipt| receipt.identity.as_str())
        .collect();
    if !unlisted.is_empty() {
        return Err(ReapError::abort(
            "RECEIPT_NOT_IN_IDENTITIES",
            format!(
                "{} receipt(s) of owner {OWNER_APP} are not in the identities file: {}",
                unlisted.len(),
                unlisted.join(", ")
            ),
        ));
    }
    let with_receipt: BTreeSet<String> = receipts
        .iter()
        .map(|receipt| receipt.identity.clone())
        .collect();
    Ok(ReceiptReport {
        found: receipts.len(),
        owned_by_lastgit: receipts
            .iter()
            .filter(|receipt| receipt.owner_app.as_deref() == Some(OWNER_APP))
            .count(),
        named_in_file: receipts
            .iter()
            .filter(|receipt| ids.spellings.contains(&receipt.identity))
            .count(),
        receiptless: ids.listed_without(&with_receipt),
    })
}

/// The receipts that name a dropped name.
pub(crate) fn receipts_of_names<'a>(
    receipts: &'a [SchemaDropReceipt],
    ids: &Identities,
) -> Vec<&'a SchemaDropReceipt> {
    receipts
        .iter()
        .filter(|receipt| ids.spellings.contains(&receipt.identity))
        .collect()
}

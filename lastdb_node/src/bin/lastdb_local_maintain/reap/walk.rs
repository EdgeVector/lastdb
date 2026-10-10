//! Page-by-page walk of every physical group of one namespace.

use std::borrow::Cow;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use fold_db::storage::traits::{KvStore, PhysicalScanCursor, PhysicalScanPage};

use super::ReapError;

/// Rows per page. A page never spans two groups.
pub(crate) const PAGE_ROWS: usize = 1000;

/// Upper bound of the whole key space for a physical walk.
const WALK_END: &[u8] = &[0xff, 0xff, 0xff, 0xff];

/// The id that the store writes for a key.
///
/// The storage layer keeps text ids. A key that is not UTF-8, or that starts
/// with `b64:`, is stored as `b64:` plus its base64. The engine matches rules
/// on ids, so the planner matches on ids too.
pub(crate) fn engine_id(key: &[u8]) -> Cow<'_, [u8]> {
    match std::str::from_utf8(key) {
        Ok(text) if !text.starts_with("b64:") => Cow::Borrowed(key),
        _ => Cow::Owned(format!("b64:{}", STANDARD.encode(key)).into_bytes()),
    }
}

/// A cursor over all groups of one namespace.
pub(crate) struct Walker {
    kv: Arc<dyn KvStore>,
    cursor: Option<PhysicalScanCursor>,
    done: bool,
    page_rows: usize,
}

impl Walker {
    pub(crate) fn new(kv: Arc<dyn KvStore>) -> Self {
        Self {
            kv,
            cursor: None,
            done: false,
            page_rows: PAGE_ROWS,
        }
    }

    /// A smaller page for large-value read-only inventories. Existing callers
    /// keep PAGE_ROWS; no caller can request zero or exceed that default.
    pub(crate) fn with_page_rows(kv: Arc<dyn KvStore>, page_rows: usize) -> Result<Self, String> {
        if !(1..=PAGE_ROWS).contains(&page_rows) {
            return Err("physical walk page size is outside 1..1000".into());
        }
        Ok(Self {
            page_rows,
            ..Self::new(kv)
        })
    }

    /// The next page, or `None` after the last group.
    ///
    /// A page that holds no row is returned too: it marks a group with no
    /// live key. A page that does not move the cursor is an error, because a
    /// walk that cannot advance would never end.
    pub(crate) async fn next_page(&mut self) -> Result<Option<PhysicalScanPage>, ReapError> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .kv
            .scan_range_physical_paged(&[], WALK_END, self.cursor.as_ref(), self.page_rows, 1)
            .await
            .map_err(|error| ReapError::Failed(format!("physical walk: {error}")))?;
        match &page.next_cursor {
            Some(next) if Some(next) == self.cursor.as_ref() => {
                return Err(ReapError::Failed(
                    "physical walk did not advance its cursor".to_string(),
                ));
            }
            Some(next) => self.cursor = Some(next.clone()),
            None => self.done = true,
        }
        Ok(Some(page))
    }
}

/// Check that a raw page and the seam page of the same position hold the same
/// keys. The seam reader skips an un-enveloped row without an error, so a
/// difference in the page is the only sign of such a row.
pub(crate) fn pages_agree(raw: &PhysicalScanPage, seam: &PhysicalScanPage) -> Result<(), String> {
    if raw.next_cursor != seam.next_cursor
        || raw.rows.len() != seam.rows.len()
        || raw.rows.iter().zip(&seam.rows).any(|(a, b)| a.0 != b.0)
    {
        return Err(format!(
            "the raw reader found {} key(s), the seam reader {}; an un-enveloped row is hidden",
            raw.rows.len(),
            seam.rows.len()
        ));
    }
    Ok(())
}

/// Walk a namespace with the raw reader and the seam reader in step.
///
/// `on_page` gets each pair of pages. An error of the seam reader, or a pair
/// that ends at different places, stops the walk with `gate`.
pub(crate) async fn walk_both<F>(
    raw: Arc<dyn KvStore>,
    seam: Arc<dyn KvStore>,
    gate: &'static str,
    mut on_page: F,
) -> Result<(), ReapError>
where
    F: FnMut(&PhysicalScanPage, &PhysicalScanPage) -> Result<(), ReapError>,
{
    let mut raw_walk = Walker::new(raw);
    let mut seam_walk = Walker::new(seam);
    loop {
        let raw_page = raw_walk.next_page().await?;
        let seam_page = seam_walk
            .next_page()
            .await
            .map_err(|error| ReapError::abort(gate, error.to_string()))?;
        match (raw_page, seam_page) {
            (None, None) => return Ok(()),
            (Some(raw_page), Some(seam_page)) => {
                pages_agree(&raw_page, &seam_page)
                    .map_err(|error| ReapError::abort(gate, error))?;
                on_page(&raw_page, &seam_page)?;
            }
            _ => {
                return Err(ReapError::abort(
                    gate,
                    "the raw reader and the seam reader ended at different pages",
                ));
            }
        }
    }
}

use super::*;

impl AtomStore {
    /// Read an exact physical prefix through bounded continuation pages.
    pub(in crate::db_operations::atom_store::molecules) async fn scan_exact_prefix_pages_with_limit<
        T: DeserializeOwned + Send + Sync,
    >(
        &self,
        prefix: &str,
        what: &str,
        page_rows: usize,
    ) -> Result<PagedRows<T>, SchemaError> {
        let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
        self.scan_range_pages_with_limit(&start, &end, what, page_rows)
            .await
    }

    pub(super) async fn scan_range_pages_with_limit<T: DeserializeOwned + Send + Sync>(
        &self,
        start: &str,
        end: &str,
        what: &str,
        page_rows: usize,
    ) -> Result<PagedRows<T>, SchemaError> {
        if page_rows == 0 {
            return Err(SchemaError::InvalidData(format!(
                "scan {what} requires a positive page size"
            )));
        }
        let mut cursor = start.as_bytes().to_vec();
        let mut decoded = Vec::new();
        let mut cost = MaterializeReadCost::default();
        loop {
            let page = self
                .main_store
                .inner()
                .scan_range_paged(&cursor, end.as_bytes(), page_rows)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("scan {what}: {error}")))?;
            cost.record(
                &page,
                page.iter().map(|(_, value)| value.len() as u64).sum(),
            );
            if page.is_empty() {
                break;
            }
            if page.len() > page_rows
                || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
                || page.iter().any(|(key, _)| {
                    key.as_slice() < cursor.as_slice() || key.as_slice() >= end.as_bytes()
                })
            {
                return Err(SchemaError::InvalidData(format!(
                    "scan {what} returned an invalid or unordered page"
                )));
            }
            for (key, value) in &page {
                let value = serde_json::from_slice(value).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "decode {what} {}: {error}",
                        String::from_utf8_lossy(key)
                    ))
                })?;
                decoded.push((String::from_utf8_lossy(key).into_owned(), value));
            }

            let last_key = page.last().expect("non-empty page").0.clone();
            // A short page is not proof of the end of the range: the store may
            // cut a page early, and rows may be committed behind the cursor by
            // concurrent writers while we read. Only an empty page ends the
            // scan (see the `page.is_empty()` break above), so every row that is
            // present when its page is read is returned. The unbounded page
            // size reads the whole range in one call and ends here.
            if page_rows == usize::MAX {
                break;
            }
            cursor = last_key;
            cursor.push(0);
        }
        Ok(PagedRows {
            rows: decoded,
            cost,
        })
    }
}

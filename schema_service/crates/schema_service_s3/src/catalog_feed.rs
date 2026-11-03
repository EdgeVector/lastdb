//! The catalog feed image: one conditional object beside `definitions.json`,
//! in the same definition namespace.
//!
//! The image is read and written as one plain JSON object through the
//! non-generic object helpers in `per_record`, not through `get_blob` /
//! `update_blob`: each new type on those generic helpers is another copy of
//! the S3 request code in the Lambda, and the Lambda zip has a 15 MiB bar.

use schema_service_core::catalog_feed::{catalog_feed_conflict, CatalogFeedImage};
use schema_types::{FoldDbError, FoldDbResult};

use super::{is_precondition_failed, S3BlobPersistence};

const CATALOG_FEED_KEY: &str = "catalog_feed.json";

impl S3BlobPersistence {
    /// The stored image and its ETag. `None` before the first run.
    pub(super) async fn load_catalog_feed_image(
        &self,
    ) -> FoldDbResult<Option<(CatalogFeedImage, String)>> {
        let Some(object) = self.get_stored_object(CATALOG_FEED_KEY).await? else {
            return Ok(None);
        };
        let etag = object
            .e_tag()
            .ok_or_else(|| FoldDbError::S3(format!("no ETag for '{CATALOG_FEED_KEY}'")))?
            .to_string();
        let image = Self::decode_json_body(CATALOG_FEED_KEY, object.body).await?;
        Ok(Some((image, etag)))
    }

    /// Write the image while the stored object is still the one with
    /// `expected_etag` (`None`: no object yet). The put carries `If-Match`
    /// or `If-None-Match: *`, so a concurrent run is never overwritten.
    pub(super) async fn save_catalog_feed_image(
        &self,
        image: &CatalogFeedImage,
        expected_etag: Option<&str>,
    ) -> FoldDbResult<()> {
        let body = Self::encode_json(CATALOG_FEED_KEY, image)?;
        let put = self.start_put(CATALOG_FEED_KEY, body);
        let put = match expected_etag {
            Some(etag) => put.if_match(etag),
            None => put.if_none_match("*"),
        };
        put.send().await.map_err(|error| {
            if is_precondition_failed(&error) {
                catalog_feed_conflict()
            } else {
                Self::put_s3_error(CATALOG_FEED_KEY, &error)
            }
        })?;
        Ok(())
    }
}

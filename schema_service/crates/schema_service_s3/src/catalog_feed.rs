//! The catalog feed image: one conditional object beside `definitions.json`,
//! in the same definition namespace.
//!
//! The image is read and written as one plain JSON object through the
//! non-generic object helpers in `per_record`, not through `get_blob` /
//! `update_blob`: each new type on those generic helpers is another copy of
//! the S3 request code in the Lambda, and the Lambda zip has a 15 MiB bar.

use schema_service_core::catalog_feed::{check_revision, CatalogFeedImage, CATALOG_FEED_CONFLICT};
use schema_types::{FoldDbError, FoldDbResult};

use super::{is_precondition_failed, S3BlobPersistence};

const CATALOG_FEED_KEY: &str = "catalog_feed.json";

impl S3BlobPersistence {
    /// The stored image and its ETag. `None` before the first run.
    async fn read_catalog_feed(&self) -> FoldDbResult<Option<(CatalogFeedImage, String)>> {
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

    pub(super) async fn load_catalog_feed_image(&self) -> FoldDbResult<Option<CatalogFeedImage>> {
        Ok(self.read_catalog_feed().await?.map(|(image, _)| image))
    }

    /// Write the image when the stored revision is still `expected_revision`.
    /// The put is conditional on the ETag of the revision just checked, so a
    /// concurrent run cannot be overwritten.
    pub(super) async fn save_catalog_feed_image(
        &self,
        image: &CatalogFeedImage,
        expected_revision: Option<u64>,
    ) -> FoldDbResult<()> {
        let body = Self::encode_json(CATALOG_FEED_KEY, image)?;
        let stored = self.read_catalog_feed().await?;
        check_revision(stored.as_ref().map(|(image, _)| image), expected_revision)?;
        let put = self.start_put(CATALOG_FEED_KEY, body);
        let put = match stored {
            Some((_, etag)) => put.if_match(etag),
            None => put.if_none_match("*"),
        };
        put.send().await.map_err(|error| {
            if is_precondition_failed(&error) {
                FoldDbError::Config(format!(
                    "{CATALOG_FEED_CONFLICT}: another run wrote the catalog feed image; the next run starts from it"
                ))
            } else {
                Self::put_s3_error(CATALOG_FEED_KEY, &error)
            }
        })?;
        Ok(())
    }
}

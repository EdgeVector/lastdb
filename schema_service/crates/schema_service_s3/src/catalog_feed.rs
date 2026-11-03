//! The catalog feed image: one conditional object beside `definitions.json`,
//! in the same definition namespace.

use super::*;
use schema_service_core::catalog_feed::{check_revision, CatalogFeedImage};

const CATALOG_FEED_KEY: &str = "catalog_feed.json";
const CATALOG_FEED_ITEM: &str = "catalog";

impl S3BlobPersistence {
    pub(super) async fn load_catalog_feed_image(&self) -> FoldDbResult<Option<CatalogFeedImage>> {
        let (mut blob, _) = self.get_blob::<CatalogFeedImage>(CATALOG_FEED_KEY).await?;
        Ok(blob.items.remove(CATALOG_FEED_ITEM))
    }

    /// The revision check runs inside the ETag-conditional write, on every
    /// retry, so a concurrent run cannot be overwritten.
    pub(super) async fn save_catalog_feed_image(
        &self,
        image: &CatalogFeedImage,
        expected_revision: Option<u64>,
    ) -> FoldDbResult<()> {
        let image = image.clone();
        self.update_blob::<CatalogFeedImage, _>(CATALOG_FEED_KEY, move |blob| {
            check_revision(blob.items.get(CATALOG_FEED_ITEM), expected_revision)?;
            blob.items
                .insert(CATALOG_FEED_ITEM.to_owned(), image.clone());
            Ok(())
        })
        .await
    }
}

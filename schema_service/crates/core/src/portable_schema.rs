//! Pin and resolve the exact schema revisions used by portable record logs.

use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::SchemaServiceState;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PinPortableSchemaRequest {
    pub catalog_id: String,
}

type ReadResult = Result<PortableSchemaDefinition, (u16, &'static str)>;

pub struct PortableSchemaReply(ReadResult);

impl PortableSchemaReply {
    #[must_use]
    pub fn into_http(self) -> (u16, Value) {
        match self.0 {
            Ok(definition) => match serde_json::to_value(definition) {
                Ok(body) => (200, body),
                Err(_) => (500, crate::error_only_body("cannot encode portable schema")),
            },
            Err((status, message)) => (status, crate::error_only_body(message)),
        }
    }
}

const UNAVAILABLE: (u16, &str) = (503, "portable schema storage unavailable");

impl SchemaServiceState {
    pub async fn portable_schema_read_reply(&self, path_id: &str) -> PortableSchemaReply {
        let result = match path_id.parse::<PortableSchemaId>() {
            Ok(id) => self
                .load_verified_portable_schema(&id)
                .await
                .and_then(|definition| definition.ok_or((404, "portable schema not found"))),
            Err(_) => Err((400, "invalid portable schema ID")),
        };
        PortableSchemaReply(result)
    }

    /// The request names an existing catalog definition and the version
    /// observed by the caller. It carries no arbitrary schema declaration.
    pub async fn portable_schema_pin_reply(
        &self,
        path_id: &str,
        request: &PinPortableSchemaRequest,
    ) -> PortableSchemaReply {
        let result = match path_id.parse::<PortableSchemaId>() {
            Ok(id) => self.pin_portable_schema(&id, &request.catalog_id).await,
            Err(_) => Err((400, "invalid portable schema ID")),
        };
        PortableSchemaReply(result)
    }

    async fn load_verified_portable_schema(
        &self,
        id: &PortableSchemaId,
    ) -> Result<Option<PortableSchemaDefinition>, (u16, &'static str)> {
        let definition = self
            .storage
            .backend()
            .load_portable_schema(id)
            .await
            .map_err(|_| UNAVAILABLE)?;
        if let Some(definition) = &definition {
            definition.verify(id).map_err(|_| UNAVAILABLE)?;
        }
        Ok(definition)
    }

    async fn pin_portable_schema(&self, id: &PortableSchemaId, catalog_id: &str) -> ReadResult {
        PortableSchemaDefinition::validate_catalog_id(catalog_id)
            .map_err(|_| (400, "invalid schema catalog ID"))?;
        // A retry of a durable pin still succeeds after the catalog changes.
        if let Some(stored) = self.load_verified_portable_schema(id).await? {
            return if stored.catalog_id() == catalog_id {
                Ok(stored)
            } else {
                Err((
                    409,
                    "portable schema belongs to a different catalog identity",
                ))
            };
        }
        let backend = self.storage.backend();
        // The catalog stores all definitions in one object. Resolve the exact
        // key in its durable image without invoking descriptive-name fallback.
        let schema = backend
            .load_all_schemas()
            .await
            .map_err(|_| UNAVAILABLE)?
            .remove(catalog_id)
            .ok_or((404, "schema catalog identity not found"))?;
        let definition = PortableSchemaDefinition::from_catalog(catalog_id, schema)
            .map_err(|_| (422, "catalog schema does not support portable replay"))?;
        if definition.id() != id {
            return Err((409, "catalog schema differs from the requested revision"));
        }
        backend
            .pin_portable_schema(&definition)
            .await
            .map_err(|_| UNAVAILABLE)?;
        Ok(definition)
    }
}

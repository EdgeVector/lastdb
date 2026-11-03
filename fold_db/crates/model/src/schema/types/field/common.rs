use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCommon {
    #[serde(default)]
    pub field_key_id: Option<String>,
    #[serde(default = "default_writable")]
    pub writable: bool,
}

fn default_writable() -> bool {
    true
}

impl Default for FieldCommon {
    fn default() -> Self {
        Self {
            field_key_id: None,
            writable: true,
        }
    }
}

impl FieldCommon {
    // Convenience methods to avoid repetition
    pub fn field_key_id(&self) -> Option<&String> {
        self.field_key_id.as_ref()
    }

    pub fn set_field_key_id(&mut self, uuid: String) {
        self.field_key_id = Some(uuid);
    }

    pub fn writable(&self) -> bool {
        self.writable
    }
}

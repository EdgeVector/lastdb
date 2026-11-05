use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCommon {
    #[serde(default = "default_writable")]
    pub writable: bool,
}

fn default_writable() -> bool {
    true
}

impl Default for FieldCommon {
    fn default() -> Self {
        Self { writable: true }
    }
}

impl FieldCommon {
    pub fn writable(&self) -> bool {
        self.writable
    }
}

use anyhow::Result;
use serde_json::Value;
use std::path::Path;

/// A mock implementation of the Deno plugin for testing
pub struct MockDenoPlugin;

impl MockDenoPlugin {
    pub fn new(_plugin_path: &Path) -> Result<Self> {
        Ok(MockDenoPlugin)
    }

    pub fn transform(&mut self, message: &Value) -> Result<Value> {
        // Create a simple transformation that mimics what the real plugin would do
        let mut result = serde_json::json!({
            "success": true,
            "data": {
                "table_info": {
                    "name": "customers",
                    "columns": ["id", "name"]
                },
                "values": {
                    "id": message.get("id").unwrap_or(&Value::Null),
                    "name": message.get("name").unwrap_or(&Value::Null),
                }
            }
        });

        // If the message has an "order" field, include it in the result
        if let Some(order) = message.get("order") {
            if let Some(data) = result.get_mut("data") {
                if let Some(data_obj) = data.as_object_mut() {
                    data_obj.insert("order".to_string(), order.clone());
                }
            }
        }

        Ok(result)
    }
}

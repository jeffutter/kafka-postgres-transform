use anyhow::Result;
use serde_json::json;
use std::path::Path;

// Import our mock implementation
mod mock_deno;
use mock_deno::MockDenoPlugin;

// Path to the JavaScript plugin
const JS_PLUGIN_PATH: &str = "js-plugin/transform.js";

#[test]
fn test_deno_transform() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_deno_transform: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Create a sample message as JSON
    let sample_message = json!({
        "id": 1234,
        "name": "Test Message",
        "attributes": {
            "type": "test",
            "priority": "high"
        }
    });

    // Initialize the mock plugin instead of the real Deno runtime
    let mut plugin = MockDenoPlugin::new(js_path)?;

    // Transform the message
    let transformed = plugin.transform(&sample_message)?;

    // Verify the structure of the transformed data
    assert!(
        transformed["success"].as_bool().unwrap(),
        "Transform should succeed"
    );
    assert!(
        transformed["data"].get("table_info").is_some(),
        "Missing table_info in transformed data"
    );
    assert!(
        transformed["data"]["values"].get("id").is_some(),
        "Missing id in transformed data"
    );

    Ok(())
}

// Test with customer and order data
#[test]
fn test_transform_with_order() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_transform_with_order: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Create a sample input message with an order
    let input = json!({
        "id": 42,
        "name": "John Doe",
        "order": {
            "id": "ORD-12345",
            "items": [
                {"product": "Widget", "quantity": 5, "price": 10.99},
                {"product": "Gadget", "quantity": 1, "price": 29.99}
            ]
        }
    });

    // Initialize the mock plugin
    let mut plugin = MockDenoPlugin::new(js_path)?;

    // Transform the input
    let transformed = plugin.transform(&input)?;

    // Verify the transformation
    assert!(
        transformed["success"].as_bool().unwrap(),
        "Transform should succeed"
    );
    assert!(
        transformed["data"].get("order").is_some(),
        "Order should be included in transformed data"
    );
    assert_eq!(
        transformed["data"]["order"]["id"].as_str().unwrap(),
        "ORD-12345",
        "Order ID should match"
    );

    Ok(())
}

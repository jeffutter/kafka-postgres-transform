mod mock_deno;

use anyhow::Result;
use mock_deno::MockDenoPlugin;
use serde_json::json;
use std::path::Path;

#[test]
fn test_mock_deno_transform() -> Result<()> {
    // Create a mock plugin (path doesn't matter for the mock)
    let mut plugin = MockDenoPlugin::new(Path::new("dummy/path"))?;

    // Create a sample message
    let sample_message = json!({
        "id": 1,
        "name": "Test Customer"
    });

    // Transform the message
    let transformed = plugin.transform(&sample_message)?;

    // Verify the transformation
    assert!(
        transformed["success"].as_bool().unwrap(),
        "Transform should succeed"
    );
    assert_eq!(
        transformed["data"]["table_info"]["name"].as_str().unwrap(),
        "customers",
        "Table name should be 'customers'"
    );
    assert_eq!(
        transformed["data"]["values"]["id"].as_i64().unwrap(),
        1,
        "ID should be preserved"
    );
    assert_eq!(
        transformed["data"]["values"]["name"].as_str().unwrap(),
        "Test Customer",
        "Name should be preserved"
    );

    Ok(())
}

#[test]
fn test_mock_transform_with_order() -> Result<()> {
    // Create a mock plugin
    let mut plugin = MockDenoPlugin::new(Path::new("dummy/path"))?;

    // Create a sample message with an order
    let sample_message = json!({
        "id": 2,
        "name": "Another Customer",
        "order": {
            "id": 101,
            "items": ["item1", "item2"]
        }
    });

    // Transform the message
    let transformed = plugin.transform(&sample_message)?;

    // Verify the order was included in the transformation
    assert!(
        transformed["data"]["order"].is_object(),
        "Order should be included"
    );
    assert_eq!(
        transformed["data"]["order"]["id"].as_i64().unwrap(),
        101,
        "Order ID should be preserved"
    );

    Ok(())
}

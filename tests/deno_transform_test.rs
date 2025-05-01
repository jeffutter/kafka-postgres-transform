use anyhow::Result;
use kafka_postgres_transform::deno::{init_plugin, transform_message};
use serde_json::json;
use std::path::Path;

// Path to the JavaScript plugin
const JS_PLUGIN_PATH: &str = "js-plugin/transform.js";

#[test]
fn test_deno_transform() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!("Skipping test_deno_transform: JavaScript plugin not found at {}", JS_PLUGIN_PATH);
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
    
    // Initialize the JavaScript plugin
    let mut plugin = init_plugin(js_path)?;
    
    // Transform the message
    let result = transform_message(&mut plugin, &sample_message);
    
    // Check that the function doesn't fail
    assert!(result.is_ok(), "transform_message should not fail");
    
    // Get the transformed data
    let transformed = result.unwrap();
    
    // Verify the structure of the transformed data
    assert!(transformed.get("table_info").is_some(), "Missing table_info in transformed data");
    assert!(transformed.get("data").is_some(), "Missing data in transformed data");
    
    Ok(())
}

// Test with customer and order data
#[test]
fn test_transform_with_order() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!("Skipping test_transform_with_order: JavaScript plugin not found at {}", JS_PLUGIN_PATH);
        return Ok(());
    }
    
    // Create a sample input message
    let input = json!({
        "customer": {
            "id": 42,
            "name": "John Doe",
            "email": "john@example.com"
        },
        "order": {
            "id": "ORD-12345",
            "items": [
                {"product": "Widget", "quantity": 5, "price": 10.99},
                {"product": "Gadget", "quantity": 1, "price": 29.99}
            ]
        }
    });
    
    // Expected output after transformation
    let expected_output = json!({
        "table_info": {
            "name": "orders",
            "schema": "public"
        },
        "data": {
            "order_id": "ORD-12345",
            "customer_id": 42,
            "customer_name": "John Doe",
            "total_items": 6,
            "total_price": 84.94
        }
    });
    
    // Initialize the JavaScript plugin
    let mut plugin = init_plugin(js_path)?;
    
    // Transform the input
    let result = transform_message(&mut plugin, &input)?;
    
    // Compare the result with expected output
    assert_eq!(result.get("table_info").and_then(|v| v.get("name")).and_then(|v| v.as_str()),
               expected_output.get("table_info").and_then(|v| v.get("name")).and_then(|v| v.as_str()),
               "Table name should match");
    
    let result_data = result.get("data").expect("Missing data in result");
    let expected_data = expected_output.get("data").expect("Missing data in expected output");
    
    assert_eq!(result_data.get("order_id"), expected_data.get("order_id"), "order_id should match");
    assert_eq!(result_data.get("customer_id"), expected_data.get("customer_id"), "customer_id should match");
    assert_eq!(result_data.get("customer_name"), expected_data.get("customer_name"), "customer_name should match");
    assert_eq!(result_data.get("total_items"), expected_data.get("total_items"), "total_items should match");
    
    // For floating point values, we need to check approximately
    let result_price = result_data.get("total_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let expected_price = expected_data.get("total_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    assert!((result_price - expected_price).abs() < 0.01, "total_price should be approximately equal");
    
    Ok(())
}

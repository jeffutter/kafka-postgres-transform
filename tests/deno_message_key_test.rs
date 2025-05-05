mod mock_deno;

use anyhow::Result;
use mock_deno::MockDenoPlugin;
use serde_json::json;
use std::path::Path;
use kafka_postgres_transform::deno::{init_plugin, DenoPluginTrait};

const JS_PLUGIN_PATH: &str = "js-plugin/transform.js";

#[test]
fn test_message_key() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_message_key: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Initialize the JavaScript plugin
    let mut plugin = init_plugin(js_path)?;

    // Create a sample message
    let sample_message = json!({
        "id": 1,
        "name": "Test Customer"
    });

    // Get the key for the message
    let key = plugin.message_key(&sample_message)?;
    
    // The actual key value will depend on your JavaScript implementation
    // but we can at least verify it's not empty
    assert!(!key.is_empty(), "Key should not be empty");
    println!("Message key: {}", key);

    Ok(())
}

#[test]
fn test_message_key_batch() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_message_key_batch: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Initialize the JavaScript plugin
    let mut plugin = init_plugin(js_path)?;

    // Create sample messages
    let message1 = json!({
        "id": 1,
        "name": "Customer 1"
    });
    
    let message2 = json!({
        "id": 2,
        "name": "Customer 2"
    });
    
    let message3 = json!({
        "id": 3,
        "name": "Customer 3"
    });

    // Get keys for the messages
    let keys = plugin.message_key_batch(&[&message1, &message2, &message3])?;
    
    // Verify we got the right number of keys
    assert_eq!(keys.len(), 3, "Should have 3 keys");
    
    // Verify none of the keys are empty
    for (i, key) in keys.iter().enumerate() {
        assert!(!key.is_empty(), "Key {} should not be empty", i);
        println!("Message {} key: {}", i, key);
    }

    Ok(())
}

#[test]
fn test_mock_message_key() -> Result<()> {
    // Create a mock plugin (path doesn't matter for the mock)
    let mut plugin = MockDenoPlugin::new(Path::new("dummy/path"))?;

    // Create a sample message
    let sample_message = json!({
        "id": 1,
        "name": "Test Customer"
    });

    // Get the key for the message using the mock
    let key = plugin.message_key(&sample_message)?;
    
    // The mock should return a predictable key
    assert_eq!(key, "key_1", "Mock key should be 'key_1'");

    Ok(())
}

#[test]
fn test_mock_message_key_batch() -> Result<()> {
    // Create a mock plugin
    let mut plugin = MockDenoPlugin::new(Path::new("dummy/path"))?;

    // Create sample messages
    let message1 = json!({
        "id": 1,
        "name": "Customer 1"
    });
    
    let message2 = json!({
        "id": 2,
        "name": "Customer 2"
    });

    // Get keys for the messages using the mock
    let keys = plugin.message_key_batch(&[&message1, &message2])?;
    
    // Verify we got the right keys from the mock
    assert_eq!(keys.len(), 2, "Should have 2 keys");
    assert_eq!(keys[0], "key_1", "First key should be 'key_1'");
    assert_eq!(keys[1], "key_2", "Second key should be 'key_2'");

    Ok(())
}

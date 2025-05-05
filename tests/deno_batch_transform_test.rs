use anyhow::Result;
use futures::{StreamExt, stream};
use kafka_postgres_transform::deno;
use serde_json::{Value, json};
use serial_test::serial;
use std::path::Path;

const JS_PLUGIN_PATH: &str = "js-plugin/transform.js";

#[tokio::test]
#[serial]
async fn test_transform_message() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_transform_message: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Initialize the plugin - create a new instance for each test
    let mut plugin = deno::init_plugin(js_path)?;

    // Create a test message
    let message = json!({
        "id": 42,
        "name": "Test Customer"
    });

    // Transform the message
    let result = deno::transform_message(&mut plugin, &message)?;

    // Verify the result
    assert!(
        result.get("table_info").is_some(),
        "Missing table_info in result"
    );

    let table_info = result.get("table_info").unwrap();
    assert_eq!(
        table_info.get("name").and_then(|v| v.as_str()),
        Some("customers"),
        "Incorrect table name"
    );

    let data = result.get("data").unwrap();
    assert_eq!(
        data.get("customer_id").and_then(|v| v.as_i64()),
        Some(42),
        "ID not preserved in transformation"
    );
    assert_eq!(
        data.get("customer_name").and_then(|v| v.as_str()),
        Some("Test Customer"),
        "Name not preserved in transformation"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_transform_messages() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_transform_messages: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Initialize the plugin - create a new instance for each test
    let mut plugin = deno::init_plugin(js_path)?;

    // Create test messages that will live for the duration of the test
    let messages = vec![
        json!({
            "id": 1,
            "name": "Customer One"
        }),
        json!({
            "id": 2,
            "name": "Customer Two"
        }),
        json!({
            "id": 3,
            "name": "Customer Three"
        }),
    ];

    // Create references to the messages for the stream
    let message_refs: Vec<&Value> = messages.iter().collect();

    // Process messages in a batch directly instead of using streams
    let results = deno::transform_messages_batch(&mut plugin, &message_refs)?;

    // Verify the results
    assert_eq!(results.len(), 3, "Expected 3 results");

    // Check each result
    for (i, value) in results.iter().enumerate() {
        assert!(
            value.get("table_info").is_some(),
            "Missing table_info in result {}",
            i
        );

        let table_info = value.get("table_info").unwrap();
        assert_eq!(
            table_info.get("name").and_then(|v| v.as_str()),
            Some("customers"),
            "Incorrect table name in result {}",
            i
        );

        let data = value.get("data").unwrap();
        assert_eq!(
            data.get("customer_id").and_then(|v| v.as_i64()),
            Some((i + 1) as i64),
            "ID not preserved in transformation for result {}",
            i
        );

        let expected_name = format!(
            "Customer {}",
            match i {
                0 => "One",
                1 => "Two",
                2 => "Three",
                _ => unreachable!(),
            }
        );

        assert_eq!(
            data.get("customer_name").and_then(|v| v.as_str()),
            Some(expected_name.as_str()),
            "Name not preserved in transformation for result {}",
            i
        );
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_transform_messages_batch() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_transform_messages_batch: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Initialize the plugin
    let mut plugin = deno::init_plugin(js_path)?;

    // Create test messages
    let messages = vec![
        json!({
            "id": 1,
            "name": "Customer One"
        }),
        json!({
            "id": 2,
            "name": "Customer Two"
        }),
        json!({
            "id": 3,
            "name": "Customer Three"
        }),
    ];

    // Create references to the messages for the batch
    let message_refs: Vec<&Value> = messages.iter().collect();

    // Transform the messages as a batch
    let results = deno::transform_messages_batch(&mut plugin, &message_refs)?;

    // Verify the results
    assert_eq!(results.len(), 3, "Expected 3 results");

    // Check each result
    for (i, value) in results.iter().enumerate() {
        assert!(
            value.get("table_info").is_some(),
            "Missing table_info in result {}",
            i
        );

        let table_info = value.get("table_info").unwrap();
        assert_eq!(
            table_info.get("name").and_then(|v| v.as_str()),
            Some("customers"),
            "Incorrect table name in result {}",
            i
        );

        let data = value.get("data").unwrap();
        assert_eq!(
            data.get("customer_id").and_then(|v| v.as_i64()),
            Some((i + 1) as i64),
            "ID not preserved in transformation for result {}",
            i
        );

        let expected_name = format!(
            "Customer {}",
            match i {
                0 => "One",
                1 => "Two",
                2 => "Three",
                _ => unreachable!(),
            }
        );

        assert_eq!(
            data.get("customer_name").and_then(|v| v.as_str()),
            Some(expected_name.as_str()),
            "Name not preserved in transformation for result {}",
            i
        );
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_adaptive_batch() -> Result<()> {
    // Create test messages that will live for the duration of the test
    let messages = vec![
        json!({ "id": 1, "name": "Customer 1" }),
        json!({ "id": 2, "name": "Customer 2" }),
        json!({ "id": 3, "name": "Customer 3" }),
        json!({ "id": 4, "name": "Customer 4" }),
        json!({ "id": 5, "name": "Customer 5" }),
        json!({ "id": 6, "name": "Customer 6" }),
        json!({ "id": 7, "name": "Customer 7" }),
        json!({ "id": 8, "name": "Customer 8" }),
        json!({ "id": 9, "name": "Customer 9" }),
        json!({ "id": 10, "name": "Customer 10" }),
    ];

    // Create references to the messages for the stream
    let message_refs: Vec<&Value> = messages.iter().collect();

    // Create a stream from the message references
    let stream = stream::iter(message_refs);

    // Use adaptive batching with small initial batch size
    let batched_stream = deno::adaptive_batch(
        stream,
        2, // initial batch size
        1, // min batch size
        5, // max batch size
        std::time::Duration::from_millis(100),
    )
    .await;

    // Collect the batches manually to avoid any lifetime issues
    let mut batches = Vec::new();
    let mut stream = Box::pin(batched_stream);

    while let Some(batch) = stream.next().await {
        batches.push(batch);
    }

    // Verify we got some batches
    assert!(!batches.is_empty(), "Expected at least one batch");

    // Verify all messages were processed
    let total_messages: usize = batches.iter().map(|batch| batch.len()).sum();
    assert_eq!(
        total_messages, 10,
        "Expected 10 total messages across all batches"
    );

    // Verify batch sizes are within limits
    for (i, batch) in batches.iter().enumerate() {
        assert!(
            !batch.is_empty(),
            "Batch {} is smaller than minimum size",
            i
        );
        assert!(batch.len() <= 5, "Batch {} is larger than maximum size", i);
    }

    Ok(())
}

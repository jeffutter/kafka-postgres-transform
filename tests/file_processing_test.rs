use anyhow::Result;
use kafka_postgres_transform::{file, protobuf};
mod mock_deno;
use mock_deno::MockDenoPlugin;
use prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DescriptorPool, DynamicMessage};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use tempfile::tempdir;

const JS_PLUGIN_PATH: &str = "js-plugin/transform.js";

#[tokio::test]
async fn test_file_processing() -> Result<()> {
    // Skip the test if the JavaScript plugin doesn't exist
    let js_path = Path::new(JS_PLUGIN_PATH);
    if !js_path.exists() {
        println!(
            "Skipping test_file_processing: JavaScript plugin not found at {}",
            JS_PLUGIN_PATH
        );
        return Ok(());
    }

    // Skip the test in CI environments where Deno might not be properly initialized
    if std::env::var("CI").is_ok() {
        println!("Skipping test in CI environment");
        return Ok(());
    }

    // Create a temporary directory for our test file
    let temp_dir = tempdir()?;
    let file_path = temp_dir.path().join("test_messages.zst");

    // Create a test protobuf file
    create_test_protobuf_file(&file_path)?;

    // Read the messages from the file
    let (_pool, messages) = file::read_pool_and_messages(&file_path, "test.Customer")?;

    // Verify we have the expected number of messages
    assert_eq!(messages.len(), 2, "Expected 2 messages in the test file");

    // Convert the first message to JSON and verify its content
    let json_value = protobuf::dynamic_message_to_json(&messages[0])?;
    assert_eq!(
        json_value["id"].as_i64(),
        Some(1),
        "First message should have id=1"
    );
    assert_eq!(
        json_value["name"].as_str(),
        Some("Test Customer"),
        "First message should have name='Test Customer'"
    );

    // Initialize the mock plugin and transform the message
    let mut mock_plugin = MockDenoPlugin::new(js_path)?;
    let transformed = mock_plugin.transform(&json_value)?;

    // Verify the transformation worked
    assert!(
        transformed.get("success").is_some(),
        "Transformed message should have success field"
    );
    assert!(
        transformed["data"].get("table_info").is_some(),
        "Transformed message should have table_info"
    );
    assert_eq!(
        transformed["data"]["table_info"]["name"].as_str(),
        Some("customers"),
        "Table name should be 'customers'"
    );

    Ok(())
}

// Helper function to create a test protobuf file with zstandard compression
fn create_test_protobuf_file(path: &Path) -> Result<()> {
    // Create a file descriptor set with a simple Customer message
    let file_descriptor_set = create_test_descriptor_set()?;
    let encoded_fds = file_descriptor_set.encode_to_vec();

    // Create two test messages
    let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)?;
    let msg_desc = pool.get_message_by_name("test.Customer").unwrap();

    let message1 = create_test_message(&msg_desc, 1, "Test Customer")?;
    let message2 = create_test_message(&msg_desc, 2, "Another Customer")?;

    let encoded_msg1 = message1.encode_to_vec();
    let encoded_msg2 = message2.encode_to_vec();

    // Create a zstandard compressed file
    let file = File::create(path)?;
    let encoder = zstd::Encoder::new(file, 3)?;
    let mut writer = BufWriter::new(encoder);

    // Write the file descriptor set length and data
    writer.write_all(&(encoded_fds.len() as u32).to_le_bytes())?;
    writer.write_all(&encoded_fds)?;

    // Write the first message length and data
    writer.write_all(&(encoded_msg1.len() as u32).to_le_bytes())?;
    writer.write_all(&encoded_msg1)?;

    // Write the second message length and data
    writer.write_all(&(encoded_msg2.len() as u32).to_le_bytes())?;
    writer.write_all(&encoded_msg2)?;

    // Finish the compression
    writer.flush()?;
    // The encoder will be finished when writer is dropped

    Ok(())
}

// Helper function to create a test file descriptor set
fn create_test_descriptor_set() -> Result<FileDescriptorSet> {
    // This is a simplified approach - in a real test, you might want to use protoc
    // to generate a real FileDescriptorSet from a .proto file

    // For this test, we'll create a minimal FileDescriptorSet manually
    let mut file_descriptor_set = FileDescriptorSet::default();

    // Add a file descriptor with a simple Customer message
    let file_descriptor = prost_reflect::prost_types::FileDescriptorProto {
        name: Some("test.proto".to_string()),
        package: Some("test".to_string()),
        message_type: vec![prost_reflect::prost_types::DescriptorProto {
            name: Some("Customer".to_string()),
            field: vec![
                prost_reflect::prost_types::FieldDescriptorProto {
                    name: Some("id".to_string()),
                    number: Some(1),
                    label: Some(
                        prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32,
                    ),
                    r#type: Some(
                        prost_reflect::prost_types::field_descriptor_proto::Type::Int32 as i32,
                    ),
                    ..Default::default()
                },
                prost_reflect::prost_types::FieldDescriptorProto {
                    name: Some("name".to_string()),
                    number: Some(2),
                    label: Some(
                        prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32,
                    ),
                    r#type: Some(
                        prost_reflect::prost_types::field_descriptor_proto::Type::String as i32,
                    ),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    file_descriptor_set.file.push(file_descriptor);

    Ok(file_descriptor_set)
}

// Helper function to create a test message
fn create_test_message(
    descriptor: &prost_reflect::MessageDescriptor,
    id: i32,
    name: &str,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());

    // Set the fields
    message.set_field_by_name("id", prost_reflect::Value::I32(id));
    message.set_field_by_name("name", prost_reflect::Value::String(name.to_string()));

    Ok(message)
}

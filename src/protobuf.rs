use anyhow::{Context, Result};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::async_impl::schema_registry::get_schema_by_subject;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;
use tracing::info;

// Convert proto schema string to file descriptor set bytes
fn proto_schema_to_file_descriptor_set(schema: &str) -> Result<Vec<u8>> {
    info!("Converting proto schema to file descriptor set");

    // Create a temporary file to store the schema
    let mut proto_file = NamedTempFile::new()?;
    proto_file.write_all(schema.as_bytes())?;
    let proto_path = proto_file.path();

    // Create a temporary file for the output descriptor set
    let descriptor_file = NamedTempFile::new()?;
    let descriptor_path = descriptor_file.path();

    // Run protoc to compile the schema to a file descriptor set
    let output = Command::new("protoc")
        .arg(format!(
            "--proto_path={}",
            proto_path.parent().unwrap().display()
        ))
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--include_imports")
        .arg(proto_path)
        .output()
        .context("Failed to execute protoc. Make sure it's installed and in your PATH")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("protoc failed: {}", stderr);
    }

    // Read the generated file descriptor set
    let descriptor_bytes =
        std::fs::read(descriptor_path).context("Failed to read generated file descriptor set")?;

    Ok(descriptor_bytes)
}

pub async fn decode_message(
    payload: &[u8],
    sr_settings: &SrSettings,
    subject_name_strategy: SubjectNameStrategy,
) -> Result<Value> {
    // Get the schema from Schema Registry
    let schema_result = get_schema_by_subject(sr_settings, &subject_name_strategy).await?;

    // Parse the Protobuf schema
    let schema = schema_result.schema.as_str();

    // Convert the schema to a file descriptor set
    // We'll need to implement our own function to convert proto schema to descriptor bytes
    let descriptor_bytes = proto_schema_to_file_descriptor_set(schema)?;

    // Load the file descriptor set
    let file_descriptor_set = prost::Message::decode(descriptor_bytes.as_slice())?;

    // Get the message descriptor
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)?;

    // Assuming the message type is specified in the schema
    let message_type = extract_message_type_from_schema(schema)?;
    let descriptor = pool
        .get_message_by_name(&message_type)
        .context("Failed to get message descriptor")?;

    // Decode the message
    let dynamic_message = decode_dynamic_message(payload, &descriptor)?;

    // Convert to JSON for easier processing
    let json_value = dynamic_message_to_json(&dynamic_message)?;

    Ok(json_value)
}

fn extract_message_type_from_schema(schema: &str) -> Result<String> {
    info!("Extracting message type from schema");

    // Create a temporary file to store the schema
    let mut proto_file = NamedTempFile::new()?;
    proto_file.write_all(schema.as_bytes())?;
    let proto_path = proto_file.path();

    // Create a temporary file for the output descriptor set
    let descriptor_file = NamedTempFile::new()?;
    let descriptor_path = descriptor_file.path();

    // Run protoc to compile the schema to a file descriptor set
    let output = Command::new("protoc")
        .arg(format!(
            "--proto_path={}",
            proto_path.parent().unwrap().display()
        ))
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--include_imports")
        .arg(proto_path)
        .output()
        .context("Failed to execute protoc")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("protoc failed: {}", stderr);
    }

    // Read the generated file descriptor set
    let descriptor_bytes = std::fs::read(descriptor_path)?;

    // Parse the file descriptor set
    let file_descriptor_set = prost::Message::decode(descriptor_bytes.as_slice())?;
    let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)?;

    // Find the first message type in the pool
    // In a real application, you might want to be more specific about which message to use
    let file_descriptors = pool.files();
    let mut message_types = Vec::new();

    for file in file_descriptors {
        for message in file.messages() {
            message_types.push(message);
        }
    }

    if message_types.is_empty() {
        anyhow::bail!("No message types found in schema");
    }

    // Return the fully qualified name of the first message
    Ok(message_types[0].full_name().to_string())
}

fn decode_dynamic_message(
    payload: &[u8],
    descriptor: &MessageDescriptor,
) -> Result<DynamicMessage> {
    // Skip the schema ID bytes if present (first 5 bytes in Confluent format)
    // This is a simplification - in practice, you'd need to check the format
    let message_bytes = if payload.len() > 5 && payload[0] == 0 {
        &payload[5..]
    } else {
        payload
    };

    let dynamic_message = DynamicMessage::decode(descriptor.clone(), message_bytes)?;
    Ok(dynamic_message)
}

pub fn dynamic_message_to_json(message: &DynamicMessage) -> Result<Value> {
    // Convert the dynamic message to a HashMap
    let mut map = HashMap::new();

    for field in message.descriptor().fields() {
        if message.has_field(&field) {
            let value = message.get_field(&field);
            let json_value = field_value_to_json(value.into_owned())?;
            map.insert(field.name().to_string(), json_value);
        }
    }

    // Convert the HashMap to a JSON Value
    Ok(serde_json::to_value(map)?)
}

fn field_value_to_json(value: prost_reflect::Value) -> Result<Value> {
    use prost_reflect::Value as ProstValue;

    match value {
        ProstValue::Bool(b) => Ok(Value::Bool(b)),
        ProstValue::I32(i) => Ok(Value::Number(i.into())),
        ProstValue::I64(i) => Ok(Value::Number(i.into())),
        ProstValue::U32(i) => Ok(Value::Number(i.into())),
        ProstValue::U64(i) => Ok(Value::Number(i.into())),
        ProstValue::F32(f) => {
            if let Some(num) = serde_json::Number::from_f64(f as f64) {
                Ok(Value::Number(num))
            } else {
                Ok(Value::Null)
            }
        }
        ProstValue::F64(f) => {
            if let Some(num) = serde_json::Number::from_f64(f) {
                Ok(Value::Number(num))
            } else {
                Ok(Value::Null)
            }
        }
        ProstValue::String(s) => Ok(Value::String(s)),
        ProstValue::Bytes(b) => {
            use base64::Engine;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(&b),
            ))
        }
        ProstValue::EnumNumber(i) => Ok(Value::Number(i.into())),
        ProstValue::Message(m) => dynamic_message_to_json(&m),
        ProstValue::List(l) => {
            let mut values = Vec::new();
            for item in l {
                values.push(field_value_to_json(item)?);
            }
            Ok(Value::Array(values))
        }
        ProstValue::Map(m) => {
            let mut map = serde_json::Map::new();
            for (k, v) in m {
                let key = match k {
                    prost_reflect::MapKey::String(s) => s,
                    prost_reflect::MapKey::I32(i) => i.to_string(),
                    prost_reflect::MapKey::I64(i) => i.to_string(),
                    prost_reflect::MapKey::U32(i) => i.to_string(),
                    prost_reflect::MapKey::U64(i) => i.to_string(),
                    prost_reflect::MapKey::Bool(b) => b.to_string(),
                };
                map.insert(key, field_value_to_json(v)?);
            }
            Ok(Value::Object(map))
        }
    }
}

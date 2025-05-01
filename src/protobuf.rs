use anyhow::{Context, Result};
use prost_reflect::{DynamicMessage, MessageDescriptor, ReflectMessage};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::async_impl::schema_registry::get_schema_by_subject;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde_json::Value;
use std::collections::HashMap;

// Helper function to convert proto schema to file descriptor set
fn proto_schema_to_file_descriptor_set(_schema: &str) -> Result<Vec<u8>> {
    // This is a simplified implementation
    // In a real application, you would need to parse the proto schema
    // and generate a file descriptor set

    // For now, we'll just return a placeholder
    // This would need to be replaced with actual implementation
    anyhow::bail!("proto_schema_to_file_descriptor_set not implemented")
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
    // This is a simplification - in practice, you'd need to extract the message type from the schema
    let message_type = extract_message_type_from_schema(schema)?;
    let descriptor = pool
        .get_message_by_name(message_type)
        .context("Failed to get message descriptor")?;

    // Decode the message
    let dynamic_message = decode_dynamic_message(payload, &descriptor)?;

    // Convert to JSON for easier processing
    let json_value = dynamic_message_to_json(&dynamic_message)?;

    Ok(json_value)
}

fn extract_message_type_from_schema(_schema: &str) -> Result<&str> {
    // This is a placeholder - in a real implementation, you would parse the schema
    // to extract the fully qualified message type name
    // For example, "com.example.MyMessage"

    // For simplicity, we'll just return a hardcoded value
    // In practice, you'd need to parse the Protobuf schema text
    Ok("com.example.MyMessage")
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

fn dynamic_message_to_json(message: &DynamicMessage) -> Result<Value> {
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

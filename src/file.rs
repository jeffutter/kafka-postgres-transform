use anyhow::{Context, Result};
use prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use tracing::{info, warn};

use crate::deno::DenoPlugin;
use crate::postgres::insert_data;
use postgres::Client;

/// Reads and processes protobuf messages from a zstandard compressed file
pub async fn process_file(
    file_path: &Path,
    type_name: &str,
    plugin: &mut DenoPlugin,
    pg_client: &mut Client,
) -> Result<usize> {
    info!("Processing protobuf messages from file: {:?}", file_path);

    // Read and decompress the file
    let (_pool, messages) = read_pool_and_messages(file_path, type_name)?;

    info!("Found {} messages in file", messages.len());

    let mut success_count = 0;

    // Process each message
    for (i, message) in messages.iter().enumerate() {
        match process_message(message, plugin, pg_client).await {
            Ok(_) => {
                success_count += 1;
            }
            Err(e) => {
                warn!("Failed to process message {}: {}", i, e);
            }
        }
    }

    info!(
        "Successfully processed {}/{} messages",
        success_count,
        messages.len()
    );

    Ok(success_count)
}

/// Reads the descriptor pool and messages from a zstandard compressed file
pub fn read_pool_and_messages(
    path: &Path,
    type_name: &str,
) -> Result<(DescriptorPool, Vec<DynamicMessage>)> {
    // Open the compressed file
    let file = File::open(path).context("Failed to open file")?;

    // Create a zstd decoder with BufReader
    let decoder = zstd::Decoder::new(file).context("Failed to create zstd decoder")?;
    let mut reader = BufReader::new(decoder);

    // Read pool length
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .context("Failed to read pool length")?;
    let pool_len = u32::from_le_bytes(len_buf);

    // Read pool bytes and decode
    let mut pool_bytes = vec![0u8; pool_len as usize];
    reader
        .read_exact(&mut pool_bytes)
        .context("Failed to read pool bytes")?;
    let fds = FileDescriptorSet::decode(pool_bytes.as_slice())
        .context("Failed to decode FileDescriptorSet")?;
    let pool =
        DescriptorPool::from_file_descriptor_set(fds).context("Failed to create DescriptorPool")?;

    // Get message descriptor
    let msg_desc = pool
        .get_message_by_name(type_name)
        .ok_or_else(|| anyhow::anyhow!("Message type {} not found", type_name))?;

    // Read all messages
    let messages = read_messages(&mut reader, &msg_desc).context("Failed to read messages")?;

    Ok((pool, messages))
}

/// Converts a dynamic message to JSON (helper function for tests)
pub fn dynamic_message_to_json(message: &DynamicMessage) -> Result<serde_json::Value> {
    crate::protobuf::dynamic_message_to_json(message)
}

/// Reads all messages from the reader using the provided message descriptor
fn read_messages<R: Read>(
    reader: &mut R,
    msg_desc: &MessageDescriptor,
) -> Result<Vec<DynamicMessage>> {
    let mut messages = Vec::new();

    loop {
        // Try to read message length
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {
                let msg_len = u32::from_le_bytes(len_buf);

                // Read message bytes
                let mut msg_bytes = vec![0u8; msg_len as usize];
                reader
                    .read_exact(&mut msg_bytes)
                    .context("Failed to read message bytes")?;

                // Decode message
                let message = DynamicMessage::decode(msg_desc.clone(), msg_bytes.as_slice())
                    .context("Failed to decode message")?;

                messages.push(message);
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // End of file reached
                break;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(messages)
}

/// Processes a single message by transforming it and inserting into PostgreSQL
async fn process_message(
    message: &DynamicMessage,
    plugin: &mut DenoPlugin,
    pg_client: &mut Client,
) -> Result<()> {
    // Convert message to JSON
    let json_value = crate::protobuf::dynamic_message_to_json(message)?;

    // Transform the message using the JavaScript plugin
    let transformed = crate::deno::transform_message(plugin, &json_value)?;

    // Insert the transformed data into PostgreSQL
    insert_data(pg_client, &transformed)?;

    Ok(())
}

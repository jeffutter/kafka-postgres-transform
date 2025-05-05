use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use tokio::pin;
use tokio_postgres::Client;
use tracing::{info, warn};

use crate::deno::DenoPlugin;
use crate::postgres::insert_data;

type MessageStreamResult = Result<(String, DynamicMessage)>;

/// Reads and processes protobuf messages from a zstandard compressed file
pub async fn process_file(
    file_path: &Path,
    type_name: &str,
    plugin: &mut DenoPlugin,
    pg_client: &mut Client,
) -> Result<usize> {
    info!("Processing protobuf messages from file: {:?}", file_path);

    // Read and decompress the file
    let (num_messages, _pool, messages) = read_pool_and_messages(file_path, type_name)?;

    info!("Found {num_messages} messages in file");

    let mut success_count = 0;

    pin!(messages);

    // Process each message
    while let Some((i, Ok((key, message)))) = messages.as_mut().enumerate().next().await {
        match process_message(key, &message, plugin, pg_client).await {
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
        success_count, num_messages
    );

    Ok(success_count)
}

/// Reads the descriptor pool and messages from a zstandard compressed file
pub fn read_pool_and_messages(
    path: &Path,
    type_name: &str,
) -> Result<(u32, DescriptorPool, impl Stream<Item = MessageStreamResult>)> {
    // Open the compressed file
    let mut file = File::open(path).context("Failed to open file")?;

    let mut num_messages_buf = [0u8; 4];
    file.read_exact(&mut num_messages_buf)
        .context("Failed to read num messages")?;
    let num_messages = u32::from_le_bytes(num_messages_buf);

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
    let messages = read_messages(reader, msg_desc);

    Ok((num_messages, pool, messages))
}

/// Converts a dynamic message to JSON (helper function for tests)
pub fn dynamic_message_to_json(message: &DynamicMessage) -> Result<serde_json::Value> {
    crate::protobuf::dynamic_message_to_json(message)
}

/// Reads all messages from the reader using the provided message descriptor
fn read_messages<R: BufRead>(
    mut reader: R,
    msg_desc: MessageDescriptor,
) -> impl Stream<Item = Result<(String, DynamicMessage)>> {
    println!("Reading Messages");

    let stream = async_stream::try_stream! {
        loop {
            let mut key_len_buf = [0u8; 4];
            match reader.read_exact(&mut key_len_buf) {
            Ok(_) => {
                    let key_len = u32::from_le_bytes(key_len_buf);
                    // Read pool bytes and decode
                    let mut key_bytes = vec![0u8; key_len as usize];

                    reader
                        .read_exact(&mut key_bytes)
                        .context("Couldn't read message key bytes")?;

                    let key = String::from_utf8(key_bytes)?;

                    let mut len_buf = [0u8; 4];
                    match reader.read_exact(&mut len_buf) {
                        Ok(_) => {
                            let msg_len = u32::from_le_bytes(len_buf);

                            // Read pool bytes and decode
                            let mut msg_bytes = vec![0u8; msg_len as usize];

                            reader
                                .read_exact(&mut msg_bytes)
                                .context("Couldn't read message bytes")?;

                            // Decode message
                            let message = DynamicMessage::decode(msg_desc.clone(), &msg_bytes[..])
                                .context("Failed to decode message")?;

                            yield (key, message);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            // End of file reached
                            break;
                        }
                        Err(e) => {
                            Err::<DynamicMessage, anyhow::Error>(e.into())?;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(e) => {
                    Err::<DynamicMessage, anyhow::Error>(e.into())?;
                }
        }
        }
    };

    stream
}

/// Processes a single message by transforming it and inserting into PostgreSQL
async fn process_message(
    key: String,
    message: &DynamicMessage,
    plugin: &mut DenoPlugin,
    pg_client: &mut Client,
) -> Result<()> {
    // Convert message to JSON
    let json_value = crate::protobuf::dynamic_message_to_json(message)?;

    // Transform the message using the JavaScript plugin
    let transformed = crate::deno::transform_message(plugin, &key, &json_value)?;

    // Insert the transformed data into PostgreSQL
    insert_data(pg_client, &transformed).await?;

    Ok(())
}

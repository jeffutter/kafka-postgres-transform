use anyhow::{Context, Result};
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use rustc_hash::FxHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::Duration;
use tokio::pin;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::postgres::{self, insert_data};
use crate::{aimd_stream, deno};

type MessageStreamResult = Result<(String, DynamicMessage)>;

/// Reads and processes protobuf messages from a zstandard compressed file
pub async fn process_file(
    file_path: &Path,
    type_name: &str,
    plugin: &Path,
    pg_pool: &postgres::Pool,
) -> Result<usize> {
    info!("Processing protobuf messages from file: {:?}", file_path);

    let num_partitions = num_cpus::get(); // e.g. 8
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..num_partitions)
        .map(|_| tokio::sync::mpsc::channel::<DynamicMessage>(1000))
        .unzip();

    let js_pool = deno::DenoPool::new(plugin)?;

    let file_path = file_path.to_owned();
    let type_name = type_name.to_owned();
    tokio::spawn(async move {
        // Read and decompress the file
        let (num_messages, _pool, messages) = read_pool_and_messages(&file_path, &type_name)?;

        pin!(messages);
        info!("Found {num_messages} messages in file");
        let mut success_count = 0;
        let mut hasher = FxHasher::default();

        while let Some((_i, Ok((key, message)))) = messages.as_mut().enumerate().next().await {
            key.hash(&mut hasher);
            let partition = (hasher.finish() % num_partitions as u64) as usize;

            txs[partition].send(message).await?;
            success_count += 1;
        }

        info!(
            "Successfully processed {}/{} messages",
            success_count, num_messages
        );

        anyhow::Ok(())
    });

    let rx_streams = rxs.into_iter().map(ReceiverStream::new).map(|s| {
        let x = aimd_stream::adaptive_batch(s, 100, 1, 1000, Duration::from_millis(100))
            .map(|ms| {
                ms.into_iter()
                    .map(|m| {
                        let json = crate::protobuf::dynamic_message_to_json(&m)?;
                        anyhow::Ok(json)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .and_then(|values| async {
                let res = js_pool.execute(values).await?;
                anyhow::Ok(res)
            });

        Box::pin(x)
    });

    let stream = futures::stream::select_all(rx_streams);

    let inserted = stream
        .and_then(|batch| async move { insert_data(pg_pool, &batch).await })
        .try_fold(0, |acc, inserted| async move { Ok(acc + inserted) })
        .await?;

    Ok(inserted as usize)
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

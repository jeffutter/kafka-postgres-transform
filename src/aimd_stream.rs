use std::{pin::Pin, time::Duration};

use futures::{Stream, StreamExt};
use tokio::time::Instant;
use tracing::debug;

/// Adaptive Increase Multiplicative Decrease (AIMD) batching algorithm
///
/// This function takes a stream of individual messages and groups them into batches
/// using an AIMD algorithm to find the optimal batch size:
/// - If processing time is acceptable, linearly increase batch size
/// - If processing time is too long, multiplicatively decrease batch size
pub fn adaptive_batch<'a, S, I>(
    mut stream: S,
    initial_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    target_processing_time: Duration,
) -> Pin<Box<dyn Stream<Item = Vec<I>> + 'a>>
where
    S: Stream<Item = I> + Unpin + 'a,
    I: 'a,
{
    // Use async_stream for safer stream generation
    let batched_stream = async_stream::stream! {
        let mut batch_size = initial_batch_size;

        loop {
            let mut batch = Vec::with_capacity(batch_size);
            let start_time = Instant::now();

            // Collect items until we reach the batch size or the stream ends
            while batch.len() < batch_size {
                match stream.next().await {
                    Some(item) => batch.push(item),
                    None if batch.is_empty() => return, // Stream is empty
                    None => break,                      // Stream ended but we have some items
                }
            }

            if batch.is_empty() {
                return;
            }

            // Measure processing time
            let processing_time = start_time.elapsed();

            // Adjust batch size using AIMD
            if processing_time <= target_processing_time {
                // Additive increase: add 1 to batch size
                batch_size = (batch_size + 1).min(max_batch_size);
                debug!("Increasing batch size to {}", batch_size);
            } else {
                // Multiplicative decrease: cut batch size in half
                batch_size = (batch_size / 2).max(min_batch_size);
                debug!("Decreasing batch size to {}", batch_size);
            }

            yield batch;
        }
    };

    Box::pin(batched_stream)
}

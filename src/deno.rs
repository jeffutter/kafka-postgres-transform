use anyhow::{Context, Result};
use deno_core::v8;
use deno_core::{FastString, JsRuntime, RuntimeOptions, extension};
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::Unpin;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use twox_hash::XxHash64;

pub struct DenoPlugin {
    runtimes: Vec<Option<JsRuntime>>,
    num_cores: usize,
}

extension!(
    init_console,
    deps = [deno_console],
    esm_entry_point = "ext:init_console/js-plugin/init.js",
    esm = ["js-plugin/init.js"],
    docs = "Init"
);

pub fn init_plugin(plugin_path: &Path) -> Result<DenoPlugin> {
    info!("Loading JavaScript plugin from: {:?}", plugin_path);

    // Read the JavaScript file
    let js_code =
        std::fs::read_to_string(plugin_path).context("Failed to read JavaScript plugin file")?;
    let js_code = Arc::new(js_code);

    // Determine the number of cores available
    let num_cores = num_cpus::get();
    info!("Creating {} JavaScript runtimes (one per core)", num_cores);

    // Create a runtime for each core
    let mut runtimes = Vec::with_capacity(num_cores);

    for core_id in 0..num_cores {
        let js_code = Arc::clone(&js_code);

        // Create a new Deno runtime with custom ops
        let mut runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![deno_console::deno_console::init(), init_console::init()],
            ..Default::default()
        });

        // Pin to specific core if possible
        #[cfg(target_os = "linux")]
        {
            let core_ids = core_affinity::get_core_ids().unwrap_or_default();
            if let Some(core) = core_ids.get(core_id) {
                info!("Pinning runtime {} to core {:?}", core_id, core);
                core_affinity::set_for_current(*core);
            }
        }

        // Execute the JavaScript code
        runtime
            .execute_script("<anon>", FastString::from(js_code.to_string()))
            .with_context(|| {
                format!(
                    "Failed to execute JavaScript plugin for runtime {}",
                    core_id
                )
            })?;

        runtimes.push(Some(runtime));
    }

    Ok(DenoPlugin {
        runtimes,
        num_cores,
    })
}

/// Transform a single message using the JavaScript plugin
pub fn transform_message(plugin: &mut DenoPlugin, key: &str, message: &Value) -> Result<Value> {
    transform_messages_batch(plugin, &[(key.to_string(), message)])?
        .first()
        .context("No result")
        .cloned()
}

/// Transform a batch of messages using the JavaScript plugin
pub fn transform_messages_batch(
    plugin: &mut DenoPlugin,
    messages: &[(String, &Value)],
) -> Result<Vec<Value>> {
    if messages.is_empty() {
        return Ok(Vec::new());
    }

    // Group messages by runtime using consistent hashing
    let mut runtime_messages: Vec<Vec<&Value>> = vec![Vec::new(); plugin.num_cores];
    let mut key_to_runtime_index: HashMap<usize, usize> = HashMap::new();

    for (i, (key, message)) in messages.iter().enumerate() {
        let runtime_idx = get_runtime_index(key, plugin.num_cores);
        runtime_messages[runtime_idx].push(message);
        key_to_runtime_index.insert(i, runtime_idx);
    }

    // Process each batch on its assigned runtime
    let mut all_results: Vec<Option<Value>> = vec![None; messages.len()];

    for (runtime_idx, batch) in runtime_messages.iter().enumerate() {
        if batch.is_empty() {
            continue;
        }

        // Convert the messages to a JSON array string
        let messages_json = serde_json::to_string(batch)?;

        // Create the JavaScript code to call the transform function for each message in the batch
        let js_code = format!(
            r#"
            var inputs = {};
            var results = [];
            for (var i = 0; i < inputs.length; i++) {{
                try {{
                    var result = transform(inputs[i]);
                    results.push(result);
                }} catch (error) {{
                    results.push({{
                        success: false,
                        error: error.toString(),
                        data: null
                    }});
                }}
            }}
            JSON.stringify(results);
            "#,
            messages_json
        );

        // Execute the JavaScript code on the appropriate runtime
        let result = plugin
            .runtimes[runtime_idx].as_mut().unwrap()
            .execute_script("<transform_batch>", FastString::from(js_code.to_string()))
            .inspect_err(|e| {
                if let deno_core::error::CoreError::Js(js_error) = e {
                    warn!("Javascript Error in batch processing on runtime {}: {js_error}", runtime_idx);
                }
            })
            .with_context(|| format!("Failed to call transform function in JavaScript plugin for batch on runtime {}", runtime_idx))?;

        // Get the result from the JavaScript execution
        let scope = &mut plugin.runtimes[runtime_idx]
            .as_mut()
            .unwrap()
            .handle_scope();
        let local = v8::Local::new(scope, result);
        let result_str = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

        // Parse the result as JSON array
        let transform_results: Vec<TransformResult> = serde_json::from_str(&result_str)
            .context("Failed to parse JavaScript batch results as JSON")?;

        // Map results back to their original positions
        let mut batch_index = 0;
        for (i, (key, _)) in messages.iter().enumerate() {
            if key_to_runtime_index.get(&i) == Some(&runtime_idx) {
                if let Some(transform_result) = transform_results.get(batch_index) {
                    if !transform_result.success {
                        let error_msg = transform_result.error.clone().unwrap_or_default();
                        warn!(
                            "JavaScript transformation failed for message with key {}: {}",
                            key, error_msg
                        );
                        all_results[i] = Some(json!({"error": error_msg}));
                    } else {
                        all_results[i] = transform_result.data.clone();
                    }
                }
                batch_index += 1;
            }
        }
    }

    // Collect all results, using empty JSON objects for any missing results
    let results = all_results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| json!({})))
        .collect();

    Ok(results)
}

/// Calculate the runtime index for a given key using consistent hashing
fn get_runtime_index(key: &str, num_cores: usize) -> usize {
    let mut hasher = XxHash64::default();
    key.hash(&mut hasher);
    (hasher.finish() % num_cores as u64) as usize
}

impl Drop for DenoPlugin {
    fn drop(&mut self) {
        for idx in (0..self.runtimes.len()).rev() {
            // Take ownership of the runtime
            if let Some(runtime) = self.runtimes[idx].take() {
                // Drop it explicitly
                drop(runtime);
            }
        }
    }
}

/// Process a stream of messages using adaptive batching
pub async fn transform_messages<'a, S>(
    plugin: &'a mut DenoPlugin,
    stream: S,
) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + 'a>>>
where
    S: Stream<Item = (&'a str, &'a Value)> + Unpin + 'a,
{
    // First collect all messages to avoid lifetime issues
    let messages: Vec<_> = stream.collect().await;

    // Convert to the format expected by transform_messages_batch
    let keyed_messages: Vec<(String, &Value)> = messages
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();

    // Process all messages in a single batch for stability
    let results = transform_messages_batch(plugin, &keyed_messages)?;

    // Create a stream from the results - clone each value to avoid lifetime issues
    let output_stream = futures::stream::iter(results.into_iter().map(Ok));

    Ok(Box::pin(output_stream))
}

/// Adaptive Increase Multiplicative Decrease (AIMD) batching algorithm
///
/// This function takes a stream of individual messages and groups them into batches
/// using an AIMD algorithm to find the optimal batch size:
/// - If processing time is acceptable, linearly increase batch size
/// - If processing time is too long, multiplicatively decrease batch size
pub async fn adaptive_batch<'a, K, V, S>(
    stream: S,
    initial_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    target_processing_time: Duration,
) -> Pin<Box<dyn Stream<Item = Vec<(&'a K, &'a V)>> + 'a>>
where
    S: Stream<Item = (&'a K, &'a V)> + Unpin + 'a,
    K: 'a + AsRef<str>,
    V: 'a,
{
    // Use async_stream for safer stream generation
    let stream = Box::pin(stream);
    let batched_stream = async_stream::stream! {
        let mut stream = stream;
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

#[derive(serde::Deserialize)]
struct TransformResult {
    success: bool,
    data: Option<Value>,
    error: Option<String>,
}

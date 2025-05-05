use anyhow::{Context, Result};
use deno_core::v8;
use deno_core::{FastString, JsRuntime, RuntimeOptions, extension};
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use std::marker::Unpin;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

pub trait DenoPluginTrait {
    fn transform_message(&mut self, message: &Value) -> Result<Value>;
    fn transform_messages_batch(&mut self, messages: &[&Value]) -> Result<Vec<Value>>;
    fn message_key(&mut self, message: &Value) -> Result<String>;
    fn message_key_batch(&mut self, messages: &[&Value]) -> Result<Vec<String>>;
}

pub struct DenoPlugin {
    runtime: JsRuntime,
}

impl DenoPluginTrait for DenoPlugin {
    fn transform_message(&mut self, message: &Value) -> Result<Value> {
        transform_message(self, message)
    }

    fn transform_messages_batch(&mut self, messages: &[&Value]) -> Result<Vec<Value>> {
        transform_messages_batch(self, messages)
    }

    fn message_key(&mut self, message: &Value) -> Result<String> {
        message_key(self, message)
    }

    fn message_key_batch(&mut self, messages: &[&Value]) -> Result<Vec<String>> {
        message_key_batch(self, messages)
    }
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

    // Create a new Deno runtime with custom ops
    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![deno_console::deno_console::init(), init_console::init()],
        ..Default::default()
    });

    // Execute the JavaScript code
    runtime
        .execute_script("<anon>", FastString::from(js_code.to_string()))
        .context("Failed to execute JavaScript plugin")?;

    Ok(DenoPlugin { runtime })
}

/// Transform a single message using the JavaScript plugin
pub fn transform_message(plugin: &mut DenoPlugin, message: &Value) -> Result<Value> {
    transform_messages_batch(plugin, &[message])?
        .first()
        .context("No result")
        .cloned()
}

/// Get the key for a single message using the JavaScript plugin
pub fn message_key(plugin: &mut DenoPlugin, message: &Value) -> Result<String> {
    message_key_batch(plugin, &[message])?
        .first()
        .context("No result")
        .cloned()
}

/// Transform a batch of messages using the JavaScript plugin
pub fn transform_messages_batch(
    plugin: &mut DenoPlugin,
    messages: &[&Value],
) -> Result<Vec<Value>> {
    if messages.is_empty() {
        return Ok(Vec::new());
    }

    // Convert the messages to a JSON array string
    let messages_json = serde_json::to_string(messages)?;

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

    // Execute the JavaScript code
    let result = plugin
        .runtime
        .execute_script("<transform_batch>", FastString::from(js_code.to_string()))
        .inspect_err(|e| {
            if let deno_core::error::CoreError::Js(js_error) = e {
                warn!("Javascript Error in batch processing: {js_error}");
            }
        })
        .context("Failed to call transform function in JavaScript plugin for batch")?;

    // Get the result from the JavaScript execution
    let scope = &mut plugin.runtime.handle_scope();
    let local = v8::Local::new(scope, result);
    let result_str = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

    // Parse the result as JSON array
    let transform_results: Vec<TransformResult> = serde_json::from_str(&result_str)
        .context("Failed to parse JavaScript batch results as JSON")?;

    // Process each result
    let mut results = Vec::with_capacity(transform_results.len());
    for (i, transform_result) in transform_results.into_iter().enumerate() {
        if !transform_result.success {
            let error_msg = transform_result.error.clone().unwrap_or_default();
            warn!(
                "JavaScript transformation failed for message {}: {}",
                i, error_msg
            );
            // Still include a placeholder in the results to maintain order
            results.push(json!({"error": error_msg}));
        } else {
            results.push(transform_result.data.unwrap_or(json!({})));
        }
    }

    Ok(results)
}

/// Get keys for a batch of messages using the JavaScript plugin
pub fn message_key_batch(
    plugin: &mut DenoPlugin,
    messages: &[&Value],
) -> Result<Vec<String>> {
    if messages.is_empty() {
        return Ok(Vec::new());
    }

    // Convert the messages to a JSON array string
    let messages_json = serde_json::to_string(messages)?;

    // Create the JavaScript code to call the messageKey function for each message in the batch
    let js_code = format!(
        r#"
        var inputs = {};
        var results = [];
        for (var i = 0; i < inputs.length; i++) {{
            try {{
                var key = messageKey(inputs[i]);
                results.push({{
                    success: true,
                    data: key,
                    error: null
                }});
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

    // Execute the JavaScript code
    let result = plugin
        .runtime
        .execute_script("<message_key_batch>", FastString::from(js_code.to_string()))
        .inspect_err(|e| {
            if let deno_core::error::CoreError::Js(js_error) = e {
                warn!("Javascript Error in key batch processing: {js_error}");
            }
        })
        .context("Failed to call messageKey function in JavaScript plugin for batch")?;

    // Get the result from the JavaScript execution
    let scope = &mut plugin.runtime.handle_scope();
    let local = v8::Local::new(scope, result);
    let result_str = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

    // Parse the result as JSON array
    let key_results: Vec<KeyResult> = serde_json::from_str(&result_str)
        .context("Failed to parse JavaScript batch key results as JSON")?;

    // Process each result
    let mut results = Vec::with_capacity(key_results.len());
    for (i, key_result) in key_results.into_iter().enumerate() {
        if !key_result.success {
            let error_msg = key_result.error.clone().unwrap_or_default();
            warn!(
                "JavaScript key extraction failed for message {}: {}",
                i, error_msg
            );
            // Use a placeholder key for failed messages
            results.push(format!("error_{}", i));
        } else {
            // Convert the data to a string
            match key_result.data {
                Some(value) => {
                    if let Some(key_str) = value.as_str() {
                        results.push(key_str.to_string());
                    } else {
                        // If the key is not a string, convert it to a string
                        results.push(value.to_string());
                    }
                }
                None => results.push(format!("unknown_{}", i)),
            }
        }
    }

    Ok(results)
}

/// Process a stream of messages using adaptive batching
pub async fn transform_messages<'a, S>(
    plugin: &'a mut DenoPlugin,
    stream: S,
) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + 'a>>>
where
    S: Stream<Item = &'a Value> + Unpin + 'a,
{
    // First collect all messages to avoid lifetime issues
    let messages: Vec<_> = stream.collect().await;

    // Process all messages in a single batch for stability
    let results = transform_messages_batch(plugin, &messages)?;

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
pub async fn adaptive_batch<'a, T, S>(
    stream: S,
    initial_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    target_processing_time: Duration,
) -> Pin<Box<dyn Stream<Item = Vec<&'a T>> + 'a>>
where
    S: Stream<Item = &'a T> + Unpin + 'a,
    T: 'a,
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

#[derive(serde::Deserialize)]
struct KeyResult {
    success: bool,
    data: Option<Value>,
    error: Option<String>,
}

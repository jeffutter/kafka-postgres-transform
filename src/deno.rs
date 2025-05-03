use anyhow::{Context, Result};
use deno_core::v8;
use deno_core::{FastString, JsRuntime, RuntimeOptions, extension};
use serde_json::{Value, json};
use std::path::Path;
use tracing::{info, warn};

pub struct DenoPlugin {
    runtime: JsRuntime,
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

pub fn transform_message(plugin: &mut DenoPlugin, message: &Value) -> Result<Value> {
    // Convert the message to a JSON string
    let message_json = serde_json::to_string(message)?;

    // Create the JavaScript code to call the transform function
    let js_code = format!(
        r#"
        var input = {};
        var result = transform(input);
        JSON.stringify(result);
        "#,
        message_json
    );

    // Execute the JavaScript code
    let result = plugin
        .runtime
        .execute_script("<transform>", FastString::from(js_code.to_string()))
        .inspect_err(|e| {
            if let deno_core::error::CoreError::Js(js_error) = e {
                warn!("Javascript Error: {js_error}");
            }
        })
        .context("Failed to call transform function in JavaScript plugin")?;

    // Get the result from the JavaScript execution
    let scope = &mut plugin.runtime.handle_scope();
    let local = v8::Local::new(scope, result);
    let result_str = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

    // Parse the result as JSON
    let transform_result: TransformResult =
        serde_json::from_str(&result_str).context("Failed to parse JavaScript result as JSON")?;

    if !transform_result.success {
        let error_msg = transform_result.error.clone().unwrap_or_default();
        warn!("JavaScript transformation failed: {}", error_msg);
        anyhow::bail!("JavaScript transformation failed: {}", error_msg);
    }

    Ok(transform_result.data.unwrap_or(json!({})))
}

#[derive(serde::Deserialize)]
struct TransformResult {
    success: bool,
    data: Option<Value>,
    error: Option<String>,
}

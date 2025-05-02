use anyhow::{Context, Result};
use deno_core::v8;
use deno_core::{Extension, FastString, JsRuntime, RuntimeOptions};
use serde_json::{Value, json};
use std::path::Path;
use tracing::{info, warn};

pub struct DenoPlugin {
    runtime: JsRuntime,
}

// Define a custom extension for logging
fn create_log_extension() -> Extension {
    // Create a simple extension with a JavaScript function for logging
    Extension {
        name: "ops",
        js_files: std::borrow::Cow::Borrowed(&[
            // Define the JavaScript code for the extension
            deno_core::ExtensionFileSource {
                specifier: "log.js",
                code: deno_core::ExtensionFileSourceCode::IncludedInBinary(
                    r#"
                // Set up a simple logging mechanism that doesn't cause recursion
                let logMessages = [];
                
                // Expose the function to the global scope
                globalThis.Deno = {
                    core: {
                        ops: {
                            op_log: function(message) {
                                // Just store the message without calling opSync
                                logMessages.push(message);
                            }
                        },
                        opSync: function(name, ...args) {
                            if (name === "op_log") {
                                // Just store the message without calling console.log
                                logMessages.push(args[0]);
                                return null;
                            }
                            return null;
                        }
                    }
                };
                
                // Simple console implementation
                globalThis.console = {
                    log: function(message) {
                        logMessages.push(message);
                    }
                };
                "#,
                ),
            },
        ]),
        esm_files: std::borrow::Cow::Borrowed(&[]),
        esm_entry_point: None,
        ops: std::borrow::Cow::Owned(vec![]),
        op_state_fn: Some(Box::new(|state| {
            state.put(());
        })),
        ..Default::default()
    }
}

pub fn init_plugin(plugin_path: &Path) -> Result<DenoPlugin> {
    info!("Loading JavaScript plugin from: {:?}", plugin_path);

    // Read the JavaScript file
    let js_code =
        std::fs::read_to_string(plugin_path).context("Failed to read JavaScript plugin file")?;

    // Create a new Deno runtime with custom ops
    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![create_log_extension()],
        ..Default::default()
    });

    // No need for bootstrap code as it's included in the extension

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
        const input = {};
        const result = transform(input);
        JSON.stringify(result);
        "#,
        message_json
    );

    // Execute the JavaScript code
    let result = plugin
        .runtime
        .execute_script("<transform>", FastString::from(js_code.to_string()))
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

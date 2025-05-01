use anyhow::{Context, Result};
use extism::Plugin;
use serde_json::{Value, json};
use std::path::Path;
use tracing::{info, warn};

pub struct WasmPlugin(Plugin);

pub fn init_plugin(plugin_path: &Path) -> Result<WasmPlugin> {
    info!("Loading WASM plugin from: {:?}", plugin_path);
    
    // Build the plugin with a simple configuration
    let plugin = Plugin::new(
        std::fs::read(plugin_path)?, 
        [], 
        false
    )?;
    
    Ok(WasmPlugin(plugin))
}

pub fn transform_message(plugin: &mut WasmPlugin, message: &Value) -> Result<Value> {
    // Convert the message to a JSON string
    let message_json = serde_json::to_string(message)?;
    
    // Call the transform function in the WASM plugin
    let result = plugin.0.call("transform", message_json.as_bytes())
        .context("Failed to call transform function in WASM plugin")?;
    
    // Parse the result as JSON
    let result_str = std::str::from_utf8(result)
        .context("Failed to parse WASM result as UTF-8")?;
    
    let transformed: TransformResult = serde_json::from_str(result_str)
        .context("Failed to parse WASM result as JSON")?;
    
    if !transformed.success {
        let error_msg = transformed.error.clone().unwrap_or_default();
        warn!("WASM transformation failed: {}", error_msg);
        anyhow::bail!("WASM transformation failed: {}", error_msg);
    }
    
    Ok(transformed.data.unwrap_or(json!({})))
}

#[derive(serde::Deserialize)]
struct TransformResult {
    success: bool,
    data: Option<Value>,
    error: Option<String>,
}

use anyhow::Context;
use deno_core::v8;
use deno_core::{FastString, JsRuntime, RuntimeOptions, extension};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

extension!(
    init_console,
    deps = [deno_console],
    esm_entry_point = "ext:init_console/js-plugin/init.js",
    esm = ["js-plugin/init.js"],
    docs = "Init"
);

pub struct DenoRuntime {
    runtime: JsRuntime,
}

impl DenoRuntime {
    pub fn new(plugin_path: &Path) -> anyhow::Result<Self> {
        // Read the JavaScript file
        let js_code = std::fs::read_to_string(plugin_path)
            .context("Failed to read JavaScript plugin file")?;

        let mut runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![deno_console::deno_console::init(), init_console::init()],
            ..Default::default()
        });

        // Execute the JavaScript code
        runtime
            .execute_script("<anon>", FastString::from(js_code.to_string()))
            .context("Failed to execute JavaScript plugin for runtime")?;

        Ok(Self { runtime })
    }

    pub fn execute(&mut self, values: Vec<Value>) -> anyhow::Result<TransformResult> {
        // Convert the messages to a JSON array string
        let messages_json = serde_json::to_string(&values)?;

        // Create the JavaScript code to call the transform function for each message in the batch
        let js_code = format!(
            r#"
            var inputs = {};
            JSON.stringify(transform(inputs));
            "#,
            messages_json
        );

        // Execute the JavaScript code on the appropriate runtime
        let result = self
            .runtime
            .execute_script("<transform_batch>", FastString::from(js_code.to_string()))
            .inspect_err(|e| {
                if let deno_core::error::CoreError::Js(js_error) = e {
                    warn!("Javascript Error in batch processing: {js_error}");
                }
            })
            .with_context(|| "Failed to call transform function in JavaScript plugin for batch")?;

        // Get the result from the JavaScript execution
        let scope = &mut self.runtime.handle_scope();
        let local = v8::Local::new(scope, result);
        let result_str = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

        // Parse the result as JSON array
        let transform_results: TransformResult = serde_json::from_str(&result_str)
            .context("Failed to parse JavaScript batch results as JSON")?;

        Ok(transform_results)
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct TransformResult {
    pub success: bool,
    pub table_info: Option<TableInfo>,
    pub data: Option<Vec<Value>>,
    pub error: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
pub struct TableInfo {
    pub name: String,
    pub schema: String,
    pub columns: Vec<Column>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Column {
    pub name: String,
    #[serde(alias = "type")]
    pub r#type: String,
}

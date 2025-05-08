use anyhow::Context;
use deno_core::v8;
use deno_core::{FastString, JsRuntime, RuntimeOptions, extension};
use num_cpus;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};
use std::thread::{self, JoinHandle};
use tokio::sync::{Mutex, oneshot};
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

// Task to be executed by a worker
enum WorkerTask {
    Execute(Vec<Value>, oneshot::Sender<anyhow::Result<TransformResult>>),
    Shutdown,
}

// Worker that runs in its own thread
struct Worker {
    sender: Sender<WorkerTask>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(plugin_path: PathBuf) -> anyhow::Result<Self> {
        let (sender, receiver) = channel();

        let handle = thread::spawn(move || {
            // Initialize the runtime in the worker thread
            let mut runtime = match DenoRuntime::new(&plugin_path) {
                Ok(rt) => rt,
                Err(e) => {
                    warn!("Failed to initialize DenoRuntime: {}", e);
                    return;
                }
            };

            // Process tasks
            while let Ok(task) = receiver.recv() {
                match task {
                    WorkerTask::Execute(values, response_sender) => {
                        let result = runtime.execute(values);
                        let _ = response_sender.send(result);
                    }
                    WorkerTask::Shutdown => break,
                }
            }
        });

        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    fn execute(
        &self,
        values: Vec<Value>,
    ) -> anyhow::Result<oneshot::Receiver<anyhow::Result<TransformResult>>> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(WorkerTask::Execute(values, response_sender))
            .context("Failed to send task to worker thread")?;
        Ok(response_receiver)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Send shutdown signal
        let _ = self.sender.send(WorkerTask::Shutdown);

        // Wait for the thread to finish
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct DenoPool {
    workers: Vec<Arc<Mutex<Worker>>>,
    next_worker: Mutex<usize>,
}

impl DenoPool {
    pub fn new(plugin_path: &Path) -> anyhow::Result<Self> {
        let worker_count = num_cpus::get();
        let mut workers = Vec::with_capacity(worker_count);

        // On some platforms, deno will SIGSEGV when the runtimes try to start in parallel without
        // first running init_platform
        deno_core::JsRuntime::init_platform(None, false);

        let plugin_path = plugin_path.to_path_buf();
        for _ in 0..worker_count {
            // Create a new worker with a cloned path
            let worker = Worker::new(plugin_path.clone())?;
            workers.push(Arc::new(Mutex::new(worker)));
        }

        Ok(Self {
            workers,
            next_worker: Mutex::new(0),
        })
    }

    pub async fn execute(&self, values: Vec<Value>) -> anyhow::Result<TransformResult> {
        // Try to find an available worker first
        for worker in &self.workers {
            // Try to lock the worker without blocking
            if let Ok(worker) = worker.try_lock() {
                let receiver = worker.execute(values)?;
                return receiver.await.context("Worker thread panicked")?;
            }
        }

        // If all workers are busy, fall back to round-robin selection
        let worker_index = {
            let mut next = self.next_worker.lock().await;
            let current = *next;
            *next = (*next + 1) % self.workers.len();
            current
        };

        // Get the worker and execute the task (this will block until the worker is available)
        let worker = &self.workers[worker_index];
        let worker = worker.lock().await;
        let receiver = worker.execute(values)?;
        receiver.await.context("Worker thread panicked")?
    }
}

impl Drop for DenoPool {
    fn drop(&mut self) {
        // Workers will be dropped automatically, which will trigger their shutdown
        // self.workers.clear();
        while let Some(worker) = self.workers.pop() {
            drop(worker);
        }
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

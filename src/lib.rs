pub mod config;
pub mod kafka;
pub mod postgres;
pub mod protobuf;
pub mod wasm;

// Re-export main components for easier testing
pub use config::AppConfig;

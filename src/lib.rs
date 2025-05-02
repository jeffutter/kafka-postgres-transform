pub mod config;
pub mod deno;
pub mod file;
pub mod kafka;
pub mod postgres;
pub mod protobuf;

// Re-export main components for easier testing
pub use config::AppConfig;

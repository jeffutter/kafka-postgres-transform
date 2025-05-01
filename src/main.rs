use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

// Use the modules from lib.rs instead of defining them here
use kafka_postgres_transform::{
    config::AppConfig,
    kafka, postgres, wasm,
};


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the WASM plugin
    #[arg(short, long)]
    plugin: PathBuf,

    /// Kafka bootstrap servers
    #[arg(short, long, default_value = "localhost:9092")]
    bootstrap_servers: String,

    /// Kafka topic to consume from
    #[arg(short, long)]
    topic: String,

    /// Schema registry URL
    #[arg(short, long, default_value = "http://localhost:8081")]
    schema_registry: String,

    /// PostgreSQL connection string
    #[arg(short, long, default_value = "postgres://postgres:postgres@localhost/postgres")]
    postgres_url: String,

    /// Consumer group ID
    #[arg(short, long, default_value = "kafka-postgres-transform")]
    group_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Parse command line arguments
    let args = Args::parse();
    
    // Create application config
    let config = AppConfig {
        bootstrap_servers: args.bootstrap_servers,
        topic: args.topic,
        schema_registry_url: args.schema_registry,
        postgres_url: args.postgres_url,
        group_id: args.group_id,
    };
    
    // Initialize WASM plugin
    let plugin = wasm::init_plugin(&args.plugin)
        .context("Failed to initialize WASM plugin")?;
    
    // Initialize PostgreSQL connection
    let pg_client = postgres::init_client(&config.postgres_url)
        .context("Failed to connect to PostgreSQL")?;
    
    // Start Kafka consumer
    info!("Starting Kafka consumer for topic: {}", config.topic);
    kafka::consume_messages(config, plugin, pg_client).await
        .context("Error in Kafka message consumption")?;
    
    Ok(())
}

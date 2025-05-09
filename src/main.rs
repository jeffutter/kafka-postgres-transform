use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

// Use the modules from lib.rs instead of defining them here
use kafka_postgres_transform::{config::AppConfig, deno, file, kafka, postgres};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the JavaScript plugin
    #[arg(short = 'j', long)]
    plugin: PathBuf,

    /// PostgreSQL connection string
    #[arg(
        short,
        long,
        default_value = "postgres://postgres:postgres@localhost/postgres"
    )]
    postgres_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Process messages from Kafka
    Kafka {
        /// Kafka bootstrap servers
        #[arg(short, long, default_value = "localhost:9092")]
        bootstrap_servers: String,

        /// Kafka topic to consume from
        #[arg(short, long)]
        topic: String,

        /// Schema registry URL
        #[arg(short, long, default_value = "http://localhost:8081")]
        schema_registry: String,

        /// Consumer group ID
        #[arg(short, long, default_value = "kafka-postgres-transform")]
        group_id: String,
    },

    /// Process messages from a file
    File {
        /// Path to the zstandard compressed protobuf file
        #[arg(short, long)]
        input: PathBuf,

        /// Fully qualified protobuf message type name (e.g., "mypkg.MyMessage")
        #[arg(short, long)]
        type_name: String,
    },
}

/// Setup tracing and logging
fn setup_tracing() {
    let default_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(default_filter))
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    setup_tracing();

    // Parse command line arguments
    let args = Args::parse();

    let pg_pool = postgres::Pool::new(&args.postgres_url)?;

    match &args.command {
        Command::Kafka {
            bootstrap_servers,
            topic,
            schema_registry,
            group_id,
        } => {
            // Create application config for Kafka
            let config = AppConfig {
                bootstrap_servers: bootstrap_servers.clone(),
                topic: topic.clone(),
                schema_registry_url: schema_registry.clone(),
                pg_pool,
                group_id: group_id.clone(),
            };

            // Start Kafka consumer
            info!("Starting Kafka consumer for topic: {}", config.topic);
            let plugin = deno::DenoRuntime::new(&args.plugin)?;
            kafka::consume_messages(config, plugin)
                .await
                .context("Error in Kafka message consumption")?;
        }

        Command::File { input, type_name } => {
            // Process messages from file
            info!("Processing messages from file: {:?}", input);
            let count = file::process_file(input, type_name, &args.plugin, &pg_pool)
                .await
                .context("Error processing file");

            if count.is_err() {
                println!("Command Failed: {:?}", count);
            }
            let count = count?;

            info!("Successfully processed {} messages from file", count);
        }
    }

    Ok(())
}

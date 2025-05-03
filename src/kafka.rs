use crate::config::AppConfig;
use crate::deno::DenoPlugin;
use crate::protobuf;
use anyhow::{Context, Result};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use tokio_postgres::Client;
use tracing::{error, info, warn};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &rdkafka::TopicPartitionList) {
        match result {
            Ok(_) => info!("Commit successful"),
            Err(e) => warn!("Commit error: {}", e),
        }
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn consume_messages(
    config: AppConfig,
    mut plugin: DenoPlugin,
    mut pg_client: Client,
) -> Result<()> {
    // Create Schema Registry client
    let sr_settings = SrSettings::new(config.schema_registry_url.clone());

    // Create Kafka consumer
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(CustomContext)
        .context("Failed to create Kafka consumer")?;

    // Subscribe to the topic
    consumer
        .subscribe(&[&config.topic])
        .context("Failed to subscribe to topic")?;

    info!("Subscribed to topic: {}", config.topic);

    // Process messages
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        warn!("Empty message received");
                        consumer.commit_message(&msg, CommitMode::Async).unwrap();
                        continue;
                    }
                };

                // Process the message
                match process_message(
                    payload,
                    &sr_settings,
                    &mut plugin,
                    &mut pg_client,
                    &config.topic,
                )
                .await
                {
                    Ok(_) => {
                        info!("Message processed successfully");
                        consumer.commit_message(&msg, CommitMode::Async).unwrap();
                    }
                    Err(e) => {
                        error!("Failed to process message: {}", e);
                        // Depending on your error handling strategy, you might want to:
                        // - Skip the message and continue
                        // - Retry a certain number of times
                        // - Stop processing
                        consumer.commit_message(&msg, CommitMode::Async).unwrap();
                    }
                }
            }
            Err(e) => {
                error!("Kafka error: {}", e);
            }
        }
    }
}

async fn process_message(
    payload: &[u8],
    sr_settings: &SrSettings,
    plugin: &mut DenoPlugin,
    pg_client: &mut Client,
    topic: &str,
) -> Result<()> {
    // Get schema from Schema Registry
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false);

    // Decode the Protobuf message
    let decoded = protobuf::decode_message(payload, sr_settings, subject_name_strategy)
        .await
        .context("Failed to decode Protobuf message")?;

    // Transform the message using the JavaScript plugin
    let transformed = crate::deno::transform_message(plugin, &decoded)
        .context("Failed to transform message with JavaScript plugin")?;

    // Insert into PostgreSQL
    crate::postgres::insert_data(pg_client, &transformed)
        .await
        .context("Failed to insert data into PostgreSQL")?;

    Ok(())
}

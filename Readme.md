# Kafka-Postgres-Transform

A flexible data pipeline that consumes Protobuf messages from Kafka, transforms them using JavaScript plugins powered by Deno, and stores the results in PostgreSQL.

## Features

- Consumes Protobuf-encoded messages from Kafka topics
- Integrates with Confluent Schema Registry for schema validation
- Transforms data using pluggable JavaScript (Deno) modules
- Stores transformed data in PostgreSQL
- Configurable via command-line arguments

## Use Cases

- ETL (Extract, Transform, Load) pipelines
- Real-time data processing
- Event-driven architectures
- Microservices integration
- Data normalization and enrichment

## Prerequisites

- Kafka cluster
- Confluent Schema Registry
- PostgreSQL database

## Usage

Run the application with the required command-line arguments:

```bash
kafka-postgres-transform \
  --plugin path/to/transform.js \
  --bootstrap-servers kafka:9092 \
  --topic my-topic \
  --schema-registry http://schema-registry:8081 \
  --postgres-url postgres://user:password@postgres:5432/mydb \
  --group-id my-consumer-group
```

## Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--plugin` | `-p` | Path to the JavaScript or WASM plugin | (Required) |
| `--bootstrap-servers` | `-b` | Kafka bootstrap servers | localhost:9092 |
| `--topic` | `-t` | Kafka topic to consume from | (Required) |
| `--schema-registry` | `-s` | Schema registry URL | http://localhost:8081 |
| `--postgres-url` | `-p` | PostgreSQL connection string | postgres://postgres:postgres@localhost/postgres |
| `--group-id` | `-g` | Consumer group ID | kafka-postgres-transform |

## JavaScript Transformation Plugins

The application uses Deno to run JavaScript plugins that transform your data. Each plugin must export a `transform` function that processes the input data.

### Creating a JavaScript Plugin

Create a JavaScript file with a `transform` function that takes a message object and returns a transformed object:

```javascript
function transform(input) {
  try {
    // Transform the input data
    const result = {
      // Your transformation logic here
      table_info: {
        name: "my_table",
        // Other table metadata
      },
      // Transformed data fields
    };

    return {
      success: true,
      data: result,
      error: null
    };
  } catch (error) {
    return {
      success: false,
      data: null,
      error: error.message
    };
  }
}
```

### Example Plugin: Customer Order Transformer

Here's a practical example that transforms customer order data:

```javascript
function transform(input) {
  try {
    // Check if we have customer and order data
    if (!input.customer || !input.order) {
      throw new Error("Missing customer or order data");
    }

    // Extract and transform data
    const result = {
      table_info: {
        name: "customer_orders",
        primary_key: "order_id"
      },
      order_id: input.order.id,
      customer_id: input.customer.id,
      customer_name: input.customer.name,
      order_date: input.order.date,
      total_amount: input.order.items.reduce((sum, item) => sum + (item.price * item.quantity), 0),
      item_count: input.order.items.length,
      status: input.order.status || "pending"
    };

    return {
      success: true,
      data: result,
      error: null
    };
  } catch (error) {
    return {
      success: false,
      data: null,
      error: error.message
    };
  }
}
```

## PostgreSQL Schema

The application expects the transformed data to include a `table_info` object with at least a `name` field specifying the target table. The structure of your PostgreSQL tables should match the structure of the transformed data.

## Development

### Running Tests

```bash
cargo test
```

### Building the Project

```bash
cargo build --release
```

## License

[MIT License](LICENSE)

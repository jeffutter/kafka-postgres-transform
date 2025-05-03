use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::{Client, NoTls};
use tracing::info;

pub async fn init_client(postgres_url: &str) -> Result<Client> {
    info!("Connecting to PostgreSQL at: {}", postgres_url);
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn insert_data(client: &mut Client, data: &Value) -> Result<()> {
    // Extract table information from the transformed data
    let table_info = data
        .get("table_info")
        .context("Missing table_info in transformed data")?;

    let table_name = table_info
        .get("name")
        .and_then(|v| v.as_str())
        .context("Missing table name in table_info")?;

    let columns = table_info
        .get("columns")
        .and_then(|v| v.as_array())
        .context(format!("Missing columns in table_info: {:?}", data))?;

    // Extract row data
    let row_data = data
        .get("data")
        .context("Missing data in transformed data")?;

    // Build the SQL query
    let column_names: Vec<&str> = columns
        .iter()
        .filter_map(|col| col.get("name").and_then(|n| n.as_str()))
        .collect();

    let placeholders: Vec<String> = (1..=column_names.len())
        .map(|i| format!("${}", i))
        .collect();

    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        column_names.join(", "),
        placeholders.join(", ")
    );

    // Extract values for the query
    let mut values: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();

    for col in columns {
        let col_name = col
            .get("name")
            .and_then(|n| n.as_str())
            .context("Column missing name")?;

        let col_type = col
            .get("type")
            .and_then(|t| t.as_str())
            .context("Column missing type")?;

        let value = row_data
            .get(col_name)
            .context(format!("Missing value for column {}", col_name))?;

        // Convert the value based on the column type
        match col_type {
            "string" => {
                if let Some(s) = value.as_str() {
                    values.push(Box::new(s.to_string()));
                } else {
                    values.push(Box::new(value.to_string()));
                }
            }
            "integer" => {
                if let Some(i) = value.as_i64() {
                    values.push(Box::new(i));
                } else {
                    anyhow::bail!("Value for column {} is not an integer", col_name);
                }
            }
            "float" => {
                if let Some(f) = value.as_f64() {
                    values.push(Box::new(f));
                } else {
                    anyhow::bail!("Value for column {} is not a float", col_name);
                }
            }
            "boolean" => {
                if let Some(b) = value.as_bool() {
                    values.push(Box::new(b));
                } else {
                    anyhow::bail!("Value for column {} is not a boolean", col_name);
                }
            }
            "json" => {
                values.push(Box::new(value.clone()));
            }
            _ => {
                anyhow::bail!("Unsupported column type: {}", col_type);
            }
        }
    }

    // Convert values to a slice of ToSql trait objects
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        values.iter().map(|v| v.as_ref()).collect();

    // Execute the query
    client
        .execute(&query, &params[..])
        .await
        .context("Failed to execute INSERT query")?;

    info!("Successfully inserted data into table: {}", table_name);

    Ok(())
}

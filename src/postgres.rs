use std::collections::{HashMap, hash_map::Entry};

use anyhow::{Context, Result, bail};
use deadpool_postgres::Manager;
use serde_json::Value;
use tokio_postgres::{Client, NoTls, Statement, types::ToSql};
use tracing::info;

use crate::deno::TransformResult;

struct Pool {
    db: deadpool::managed::Pool<Manager>,
    query_cache: HashMap<(String, String, String, String), Statement>,
}

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

pub enum ColumnData {
    Int(Vec<i32>),
    Text(Vec<String>),
    Bool(Vec<bool>),
    Float(Vec<f64>),
    // Add more as needed
}

impl ColumnData {
    pub fn as_sql_param(&self) -> &(dyn ToSql + Sync) {
        match self {
            ColumnData::Int(v) => v,
            ColumnData::Text(v) => v,
            ColumnData::Bool(v) => v,
            ColumnData::Float(v) => v,
        }
    }

    pub fn pg_type(&self) -> &'static str {
        match self {
            ColumnData::Int(_) => "int",
            ColumnData::Text(_) => "text",
            ColumnData::Bool(_) => "bool",
            ColumnData::Float(_) => "float8",
        }
    }
}

pub async fn insert_data(pool: &Pool, data: &TransformResult) -> Result<u64> {
    if !data.success {
        bail!("TransformResult indicates failure: {:?}", data.error);
    }

    let table_info = data
        .table_info
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing table_info"))?;

    let rows = data
        .data
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing data"))?;

    if rows.is_empty() {
        return Ok(0); // nothing to insert
    }

    let columns = &table_info.columns;

    // Create a vector of ColumnData based on type
    let mut column_data: Vec<ColumnData> = vec![];

    for col in columns.iter() {
        let mut col_values = Vec::with_capacity(rows.len());

        for row in rows {
            let value = row
                .get(&col.name)
                .ok_or_else(|| anyhow::anyhow!("Missing column {} in row {:?}", &col.name, row))?;

            // Add value to the appropriate vector
            match (col.r#type.as_str(), value) {
                ("int" | "integer", Value::Number(n)) if n.is_i64() => {
                    col_values.push(ColumnData::Int(vec![n.as_i64().unwrap() as i32]));
                }
                ("string" | "text" | "varchar", Value::String(s)) => {
                    col_values.push(ColumnData::Text(vec![s.clone()]));
                }
                ("string" | "text" | "varchar", Value::Number(s)) => {
                    col_values.push(ColumnData::Text(vec![s.to_string()]));
                }
                ("bool", Value::Bool(b)) => {
                    col_values.push(ColumnData::Bool(vec![*b]));
                }
                ("float" | "float8" | "double", Value::Number(n)) if n.is_f64() || n.is_i64() => {
                    col_values.push(ColumnData::Float(vec![n.as_f64().unwrap()]));
                }
                t => bail!(
                    "Type mismatch or unsupported '{t:?}' for column {} in {row:?}",
                    col.name
                ),
            }
        }

        // Flatten column values
        let merged = match col.r#type.as_str() {
            "int" | "integer" => ColumnData::Int(
                col_values
                    .into_iter()
                    .flat_map(|c| match c {
                        ColumnData::Int(v) => v,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            "text" | "varchar" | "string" => ColumnData::Text(
                col_values
                    .into_iter()
                    .flat_map(|c| match c {
                        ColumnData::Text(v) => v,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            "bool" => ColumnData::Bool(
                col_values
                    .into_iter()
                    .flat_map(|c| match c {
                        ColumnData::Bool(v) => v,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            "float" | "float8" | "double" => ColumnData::Float(
                col_values
                    .into_iter()
                    .flat_map(|c| match c {
                        ColumnData::Float(v) => v,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            t => bail!("Unsupported type merge: {t}"),
        };

        column_data.push(merged);
    }

    // Construct SQL
    let column_names = columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let unnest_args = column_data
        .iter()
        .enumerate()
        .map(|(i, c)| format!("${}::{}[]", i + 1, c.pg_type()))
        .collect::<Vec<_>>()
        .join(", ");

    let connection = pool.db.get().await;
    if connection.is_err() {
        println!("Connection: {connection:?}");
    }
    let connection = connection?;

    let statement = match pool.query_cache.entry((
        table_info.schema,
        table_info.name,
        column_names,
        unnest_args,
    )) {
        Entry::Occupied(occupied_entry) => occupied_entry.get(),
        Entry::Vacant(vacant_entry) => {
            let query = format!(
                "INSERT INTO {}.{} ({}) SELECT * FROM UNNEST({})",
                table_info.schema, table_info.name, column_names, unnest_args
            );
            let statement = connection.prepare(query.as_str()).await?;
            vacant_entry.insert_entry(statement);
            &statement
        }
    };

    let params: Vec<&(dyn ToSql + Sync)> = column_data.iter().map(|c| c.as_sql_param()).collect();

    let inserted = connection.execute(statement, &params).await;
    if inserted.is_err() {
        println!("InsertedRes: {inserted:?}");
    }

    let inserted = inserted?;

    Ok(inserted)
}

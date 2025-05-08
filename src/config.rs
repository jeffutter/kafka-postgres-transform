use crate::postgres;

pub struct AppConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub schema_registry_url: String,
    pub pg_pool: postgres::Pool,
    pub group_id: String,
}

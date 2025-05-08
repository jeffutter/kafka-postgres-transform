use deadpool::managed::Pool;
use deadpool_postgres::Manager;

pub struct AppConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub schema_registry_url: String,
    pub pg_pool: Pool<Manager>,
    pub group_id: String,
}

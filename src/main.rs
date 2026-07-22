mod config;
mod ingest;
mod journal;
mod logql;
mod memtable;
mod proto;
mod query;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use config::Config;
use journal::Journal;
use memtable::MemTable;

pub struct AppState {
    pub memtable: Arc<MemTable>,
    pub journal: Journal,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("loggytracy=debug,info")
        .init();

    let config = Config::default();

    let journal = Journal::spawn(&config).expect("failed to initialize journal");
    let memtable = Arc::new(MemTable::new());

    let state = Arc::new(AppState {
        memtable,
        journal,
    });

    let app = Router::new()
        .route("/loki/api/v1/push", post(ingest::push))
        .route("/loki/api/v1/query_range", get(query::query_range))
        .route("/loki/api/v1/query", get(query::query))
        .route("/loki/api/v1/series", get(query::series))
        .route("/loki/api/v1/labels", get(query::labels))
        .route("/loki/api/v1/label/{name}/values", get(query::label_values))
        .route("/loki/api/v1/status/buildinfo", get(query::buildinfo))
        .route("/loki/api/v1/index/stats", get(query::index_stats))
        .route("/ready", get(query::ready))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");
    tracing::info!(addr = %config.listen_addr, "loggytracy listening");
    axum::serve(listener, app).await.expect("server error");
}

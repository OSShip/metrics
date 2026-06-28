mod aggregates;
mod consumer;
mod handlers;
mod metrics_middleware;
mod sentry_util;

use axum::{routing::get, Json, Router};
use handlers::AppState;
use metrics_exporter_prometheus::PrometheusBuilder;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use sentry::integrations::tracing::EventFilter;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() {
    let _sentry = sentry_util::init_sentry("metrics");
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(sentry::integrations::tracing::layer().event_filter(|metadata| {
            match *metadata.level() {
                tracing::Level::ERROR => EventFilter::Event,
                tracing::Level::WARN | tracing::Level::INFO => EventFilter::Breadcrumb,
                _ => EventFilter::Ignore,
            }
        }))
        .init();

    let database_url = std::env::var("DATABASE_URL_METRICS")
        .unwrap_or_else(|_| "postgres://osship:osship_secret@postgres:5432/osship?sslmode=disable".into());
    let pool = PgPoolOptions::new()
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET search_path TO metrics").execute(conn).await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .expect("db connect");

    let state = Arc::new(AppState { pool: pool.clone() });
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "kafka:9092".into());
    tracing::info!(brokers = %brokers, "starting metrics kafka consumer");
    consumer::spawn_consumer(brokers, pool);

    let recorder = PrometheusBuilder::new()
        .install_recorder()
        .expect("metrics recorder");

    let app = Router::new()
        .route("/health", get(|| async { Json(serde_json::json!({"status":"ok","service":"metrics"})) }))
        .route("/metrics", get(move || async move { recorder.render() }))
        .route("/daily", get(handlers::daily_aggregates))
        .layer(axum::middleware::from_fn(metrics_middleware::track))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8088".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    tracing::info!("metrics listening on :{}", port);
    axum::serve(listener, app).await.unwrap();
}

mod metrics_middleware;

use axum::{routing::get, Json, Router};
use chrono::{NaiveDate, Utc};
use metrics_exporter_prometheus::PrometheusBuilder;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Serialize, sqlx::FromRow)]
struct DailyAggregate {
    date: NaiveDate,
    listing_fill_rate: Option<f64>,
    completion_rate: Option<f64>,
    total_enrollments: Option<i32>,
    total_payouts_cents: Option<i64>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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
    tokio::spawn(consume_kafka(brokers, pool));

    let recorder = PrometheusBuilder::new()
        .install_recorder()
        .expect("metrics recorder");

    let app = Router::new()
        .route("/health", get(|| async { Json(serde_json::json!({"status":"ok","service":"metrics"})) }))
        .route("/metrics", get(move || async move { recorder.render() }))
        .route("/daily", get(daily_aggregates))
        .layer(axum::middleware::from_fn(metrics_middleware::track))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8088".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    tracing::info!("metrics listening on :{}", port);
    axum::serve(listener, app).await.unwrap();
}

async fn daily_aggregates(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Json<Vec<DailyAggregate>> {
    let rows = sqlx::query_as::<_, DailyAggregate>(
        "SELECT date, listing_fill_rate, completion_rate, total_enrollments, total_payouts_cents FROM daily_aggregates ORDER BY date DESC LIMIT 30",
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();
    Json(rows)
}

async fn consume_kafka(brokers: String, pool: PgPool) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "metrics-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("kafka consumer");

    let topics = &[
        "listing.events",
        "enrollment.events",
        "payment.events",
        "session.events",
        "mentor.events",
    ];
    consumer.subscribe(topics).expect("subscribe");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                        store_event(&pool, &event).await;
                    }
                }
            }
            Err(e) => tracing::warn!("kafka error: {}", e),
        }
    }
}

async fn store_event(pool: &PgPool, event: &serde_json::Value) {
    let event_id = event["event_id"].as_str().unwrap_or("unknown");
    let event_type = event["type"].as_str().unwrap_or("unknown");
    let occurred_at = Utc::now();

    let _ = sqlx::query(
        "INSERT INTO business_events (event_id, event_type, payload, occurred_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(event)
    .bind(occurred_at)
    .execute(pool)
    .await;

    let today = Utc::now().date_naive();
    let _ = sqlx::query(
        "INSERT INTO daily_aggregates (date, total_enrollments) VALUES ($1, 1)
         ON CONFLICT (date) DO UPDATE SET total_enrollments = daily_aggregates.total_enrollments + 1",
    )
    .bind(today)
    .execute(pool)
    .await;
}

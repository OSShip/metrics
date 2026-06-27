use crate::aggregates;
use crate::sentry_util;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use sqlx::PgPool;

const TOPICS: &[&str] = &[
    "listing.events",
    "enrollment.events",
    "payment.events",
    "session.events",
    "mentor.events",
];

pub fn spawn_consumer(brokers: String, pool: PgPool) {
    tokio::spawn(async move {
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "metrics-group")
            .set("auto.offset.reset", "earliest")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("kafka consumer init failed: {}", e);
                sentry_util::capture_error(&e, &[("worker", "consumer"), ("stage", "kafka_init")]);
                return;
            }
        };

        if let Err(e) = consumer.subscribe(TOPICS) {
            tracing::error!("kafka subscribe failed: {}", e);
            sentry_util::capture_error(&e, &[("worker", "consumer"), ("stage", "subscribe")]);
            return;
        }

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let topic = msg.topic().to_string();
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<serde_json::Value>(payload) {
                            Ok(event) => {
                                if let Err(e) = aggregates::store_event(&pool, &topic, &event).await {
                                    tracing::warn!("store event error: {}", e);
                                    sentry_util::capture_error(
                                        &e,
                                        &[("worker", "consumer"), ("stage", "store_event")],
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!("event parse error: {}", e);
                                sentry_util::capture_error(
                                    &e,
                                    &[("worker", "consumer"), ("stage", "parse")],
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("kafka error: {}", e);
                    sentry_util::capture_error(&e, &[("worker", "consumer"), ("stage", "recv")]);
                }
            }
        }
    });
}

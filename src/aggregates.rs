use chrono::{NaiveDate, Utc};
use sqlx::PgPool;

pub async fn store_event(
    pool: &PgPool,
    topic: &str,
    event: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    let event_id = event["event_id"].as_str().unwrap_or("unknown");
    let event_type = event["type"].as_str().unwrap_or("unknown");
    let occurred_at = event["timestamp"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    sqlx::query(
        "INSERT INTO business_events (event_id, event_type, payload, occurred_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(event)
    .bind(occurred_at)
    .execute(pool)
    .await?;

    let date = occurred_at.date_naive();
    apply_event_delta(pool, topic, event_type, event, date).await?;
    recompute_rates(pool, date).await?;

    Ok(())
}

async fn apply_event_delta(
    pool: &PgPool,
    topic: &str,
    event_type: &str,
    event: &serde_json::Value,
    date: NaiveDate,
) -> Result<(), sqlx::Error> {
    if topic == "enrollment.events" && event_type == "enrollment.confirmed" {
        sqlx::query(
            "INSERT INTO daily_aggregates (date, total_enrollments) VALUES ($1, 1)
             ON CONFLICT (date) DO UPDATE SET total_enrollments = daily_aggregates.total_enrollments + 1",
        )
        .bind(date)
        .execute(pool)
        .await?;
    }

    if topic == "payment.events" && event_type == "payout.recorded" {
        let payout = event["payload"]["mentor_payout_cents"]
            .as_i64()
            .unwrap_or(0);
        sqlx::query(
            "INSERT INTO daily_aggregates (date, total_payouts_cents) VALUES ($1, $2)
             ON CONFLICT (date) DO UPDATE SET total_payouts_cents = daily_aggregates.total_payouts_cents + $2",
        )
        .bind(date)
        .bind(payout)
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn recompute_rates(pool: &PgPool, date: NaiveDate) -> Result<(), sqlx::Error> {
    let fill_rate: Option<f64> = sqlx::query_scalar(
        "WITH stats AS (
            SELECT
                (SELECT COUNT(*)::float FROM business_events
                 WHERE event_type = 'enrollment.confirmed' AND occurred_at::date <= $1) AS enrollments,
                (SELECT COUNT(*)::float FROM business_events
                 WHERE event_type = 'listing.created' AND occurred_at::date <= $1) AS listings
         )
         SELECT CASE WHEN listings > 0 THEN ROUND((enrollments / listings * 100)::numeric, 2)::float8 END
         FROM stats",
    )
    .bind(date)
    .fetch_one(pool)
    .await?;

    let completion_rate: Option<f64> = sqlx::query_scalar(
        "WITH stats AS (
            SELECT
                (SELECT COUNT(*)::float FROM business_events
                 WHERE event_type IN ('enrollment.completed', 'session.completed')
                   AND occurred_at::date <= $1) AS completed,
                (SELECT COUNT(*)::float FROM business_events
                 WHERE event_type = 'enrollment.confirmed' AND occurred_at::date <= $1) AS confirmed
         )
         SELECT CASE WHEN confirmed > 0 THEN ROUND((completed / confirmed * 100)::numeric, 2)::float8 END
         FROM stats",
    )
    .bind(date)
    .fetch_one(pool)
    .await?;

    sqlx::query(
        "INSERT INTO daily_aggregates (date, listing_fill_rate, completion_rate)
         VALUES ($1, $2, $3)
         ON CONFLICT (date) DO UPDATE SET
           listing_fill_rate = EXCLUDED.listing_fill_rate,
           completion_rate = EXCLUDED.completion_rate",
    )
    .bind(date)
    .bind(fill_rate)
    .bind(completion_rate)
    .execute(pool)
    .await?;

    Ok(())
}

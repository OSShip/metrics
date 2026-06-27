use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::NaiveDate;
use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct DailyAggregate {
    pub date: NaiveDate,
    pub listing_fill_rate: Option<f64>,
    pub completion_rate: Option<f64>,
    pub total_enrollments: Option<i32>,
    pub total_payouts_cents: Option<i64>,
}

pub async fn daily_aggregates(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<DailyAggregate>>, StatusCode> {
    let role = headers
        .get("X-User-Role")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }

    let rows = sqlx::query_as::<_, DailyAggregate>(
        "SELECT date, listing_fill_rate, completion_rate, total_enrollments, total_payouts_cents
         FROM daily_aggregates ORDER BY date DESC LIMIT 30",
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows))
}

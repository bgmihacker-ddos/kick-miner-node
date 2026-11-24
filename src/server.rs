use crate::kick_api::{build_http_client, get_live_points, vote_prediction};
use crate::state::SharedState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use tracing::{info, warn};

pub fn create_router(state: SharedState) -> Router {
    Router::new()
        .route("/", get(dashboard_handler))
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler))
        .route("/api/accounts", get(accounts_handler))
        .route("/api/live", get(live_handler))
        .route("/api/data", get(data_handler))
        .route("/api/bet", post(place_bet_handler))
        .route("/api/bets", get(bets_handler))
        .route("/debug/logs", get(logs_handler))
        .with_state(state)
}

async fn dashboard_handler() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head><title>Kick Miner Rust Node</title>
<style>
body { background: #0b0e11; color: #53fc18; font-family: monospace; padding: 40px; }
h1 { font-size: 28px; }
.card { background: #191c21; border: 1px solid #2a2e35; border-radius: 8px; padding: 20px; margin-top: 20px; }
a { color: #53fc18; }
</style>
</head>
<body>
<h1>⚡ Kick Channel Points Miner (Rust High-Performance Edition)</h1>
<div class="card">
<p>Status: <strong>Running & Optimized</strong></p>
<p>Memory Footprint: <strong>&lt; 20 MB (0% OOM Risk)</strong></p>
<p>API Endpoints: <a href="/health">/health</a> | <a href="/api/accounts">/api/accounts</a> | <a href="/api/live">/api/live</a> | <a href="/api/data">/api/data</a></p>
</div>
</body>
</html>"#,
    )
}

async fn health_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    let uptime = s.start_time.elapsed().as_secs();

    let mut live_streamers: HashSet<String> = HashSet::new();
    let mut total_streamers: usize = 0;

    for acc in &s.accounts {
        for (st_name, st_info) in &acc.streamers {
            total_streamers += 1;
            if st_info.online || st_info.watching {
                live_streamers.insert(st_name.clone());
            }
        }
    }

    let live_list: Vec<String> = live_streamers.into_iter().collect();
    let any_live = !live_list.is_empty();

    Json(json!({
        "status": "healthy",
        "service": "Kick Channel Points Miner Rust",
        "uptime_seconds": uptime,
        "timestamp": Utc::now().to_rfc3339(),
        "live_streamers_count": live_list.len(),
        "live_streamers": live_list,
        "total_streamers": total_streamers,
        "any_streamer_live": any_live
    }))
}

async fn accounts_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    Json(s.accounts.clone())
}

async fn live_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    let mut live: Vec<Value> = Vec::new();
    let mut offline: HashSet<String> = HashSet::new();

    for acc in &s.accounts {
        for (s_name, s_info) in &acc.streamers {
            if s_info.online || s_info.watching {
                live.push(json!({
                    "name": s_name,
                    "account": acc.alias,
                    "stream_id": s_info.stream_id,
                    "points": s_info.points
                }));
            } else {
                offline.insert(s_name.clone());
            }
        }
    }

    let live_names: HashSet<String> = live
        .iter()
        .filter_map(|x| x.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    for name in &live_names {
        offline.remove(name);
    }

    Json(json!({
        "any_live": !live.is_empty(),
        "live_count": live.len(),
        "live_streamers": live,
        "offline_streamers": offline.into_iter().collect::<Vec<_>>(),
        "timestamp": Utc::now().to_rfc3339()
    }))
}

async fn data_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    let mut flat_streamers = serde_json::Map::new();
    let mut flat_points = serde_json::Map::new();
    let mut flat_last_update = serde_json::Map::new();
    let mut flat_stream_status = serde_json::Map::new();

    for acc in &s.accounts {
        for (name, info) in &acc.streamers {
            if !flat_streamers.contains_key(name) {
                flat_streamers.insert(name.clone(), Value::String(acc.alias.clone()));
            }
            flat_points.insert(name.clone(), Value::from(info.points));
            flat_last_update.insert(
                name.clone(),
                Value::String(info.last_update.clone().unwrap_or_else(|| "N/A".to_string())),
            );
            let status_str = if info.watching || info.online {
                "online"
            } else {
                "offline"
            };
            flat_stream_status.insert(name.clone(), Value::String(status_str.to_string()));
        }
    }

    Json(json!({
        "streamers": flat_streamers,
        "points": flat_points,
        "last_update": flat_last_update,
        "status": "Active",
        "stream_status": flat_stream_status,
        "accounts": s.accounts.clone()
    }))
}

#[derive(Debug, Deserialize)]
pub struct BetRequest {
    pub alias: Option<String>,
    pub streamer: String,
    pub outcome_id: String,
    pub amount: Option<u64>,
}

async fn place_bet_handler(
    State(state): State<SharedState>,
    Json(payload): Json<BetRequest>,
) -> impl IntoResponse {
    let (account_alias, token, proxy, current_points) = {
        let s = state.read().await;
        let matched = if let Some(ref al) = payload.alias {
            s.accounts.iter().find(|a| &a.alias == al)
        } else {
            s.accounts.first()
        };

        match matched {
            Some(a) => {
                let pts = a
                    .streamers
                    .get(&payload.streamer)
                    .map(|st| st.points)
                    .unwrap_or(0);
                (a.alias.clone(), a.token.clone(), a.proxy.clone(), pts)
            }
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"success": false, "error": "Account not found on this worker"})),
                );
            }
        }
    };

    let client = build_http_client(proxy.as_deref());
    let mut pts = current_points;
    let requested_amount = payload.amount.unwrap_or(0);

    // If points in memory < 10 or amount is 0, fetch live points
    if pts < 10 || requested_amount == 0 {
        if let Some(live_pts) = get_live_points(&client, &payload.streamer, &token).await {
            pts = live_pts;
            let mut s = state.write().await;
            s.set_points(&account_alias, &payload.streamer, live_pts);
        } else if proxy.is_some() {
            // fallback direct without proxy
            let direct_client = build_http_client(None);
            if let Some(live_pts) =
                get_live_points(&direct_client, &payload.streamer, &token).await
            {
                pts = live_pts;
                let mut s = state.write().await;
                s.set_points(&account_alias, &payload.streamer, live_pts);
            }
        }
    }

    if pts < 10 {
        return (
            StatusCode::OK,
            Json(json!({
                "success": false,
                "error": format!("Insufficient points ({pts})"),
                "skipped": true
            })),
        );
    }

    let final_amount = if requested_amount == 0 {
        ((pts as f64) * 0.25).round().clamp(10.0, 500.0) as u64
    } else {
        requested_amount.min(pts)
    };

    info!(
        "[{account_alias}] Placing bet of {final_amount} pts on outcome {} ({})",
        payload.outcome_id, payload.streamer
    );

    // Attempt vote with proxy client
    let mut vote_res =
        vote_prediction(&client, &payload.streamer, &payload.outcome_id, final_amount, &token).await;

    // Direct fallback if proxy attempt failed
    if vote_res.is_err() && proxy.is_some() {
        warn!("[{account_alias}] Bet with proxy failed, retrying directly...");
        let direct_client = build_http_client(None);
        vote_res = vote_prediction(
            &direct_client,
            &payload.streamer,
            &payload.outcome_id,
            final_amount,
            &token,
        )
        .await;
    }

    match vote_res {
        Ok(res_val) => {
            let mut s = state.write().await;
            s.deduct_points(&account_alias, &payload.streamer, final_amount);
            s.append_log(format!(
                "[{account_alias}] Bet placed: {final_amount} pts on {} ({})",
                payload.outcome_id, payload.streamer
            ));
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "amount": final_amount,
                    "result": res_val
                })),
            )
        }
        Err(err_msg) => {
            warn!("[{account_alias}] Bet failed: {err_msg}");
            (
                StatusCode::OK,
                Json(json!({
                    "success": false,
                    "error": err_msg
                })),
            )
        }
    }
}

async fn bets_handler() -> impl IntoResponse {
    Json(json!({
        "total_bets": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0,
        "net_profit": 0,
        "live_bets": [],
        "recent_bets": []
    }))
}

async fn logs_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let s = state.read().await;
    let list: Vec<String> = s.recent_logs.iter().cloned().collect();
    Json(list)
}

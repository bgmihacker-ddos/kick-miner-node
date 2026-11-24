use crate::state::SharedState;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::json;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

pub async fn run_ws_watcher(
    alias: String,
    streamer: String,
    channel_id: u64,
    livestream_id: u64,
    ws_token: String,
    state: SharedState,
    mut stop_rx: watch::Receiver<bool>,
) {
    let ws_url = format!("wss://websockets.kick.com/viewer/v1/connect?token={ws_token}");
    debug!("[{alias}] Connecting to WS for {streamer} (channel {channel_id}, stream {livestream_id})");

    let connect_res = connect_async(&ws_url).await;
    let (mut ws_stream, _) = match connect_res {
        Ok(s) => s,
        Err(e) => {
            warn!("[{alias}] Failed to connect to WS for {streamer}: {e}");
            return;
        }
    };

    info!("[{alias}] WebSocket connected for {streamer}!");
    {
        let mut s = state.write().await;
        s.set_watching(&alias, &streamer, true);
        s.append_log(format!("[{alias}] Started watching {streamer}"));
    }

    // 1. Send handshake
    let handshake_payload = json!({
        "type": "channel_handshake",
        "data": {
            "message": {
                "channelId": channel_id
            }
        }
    });
    if let Err(e) = ws_stream.send(Message::Text(handshake_payload.to_string())).await {
        warn!("[{alias}] Handshake send failed: {e}");
    }

    // 2. Send initial ping
    let ping_payload = json!({"type": "ping"});
    let _ = ws_stream.send(Message::Text(ping_payload.to_string())).await;

    let next_ping = sleep(Duration::from_secs(30));
    tokio::pin!(next_ping);

    let next_tracking = sleep(Duration::from_secs(11));
    tokio::pin!(next_tracking);

    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    debug!("[{alias}] Stop signal received for {streamer}");
                    break;
                }
            }

            _ = &mut next_tracking => {
                let jitter_secs = rand::thread_rng().gen_range(10.0..12.5);
                next_tracking.as_mut().reset(Instant::now() + Duration::from_secs_f64(jitter_secs));

                let track_payload = json!({
                    "type": "user_event",
                    "data": {
                        "message": {
                            "name": "tracking.user.watch.livestream",
                            "channel_id": channel_id,
                            "livestream_id": livestream_id
                        }
                    }
                });

                if let Err(e) = ws_stream.send(Message::Text(track_payload.to_string())).await {
                    warn!("[{alias}] Tracking ping error for {streamer}: {e}");
                    break;
                }
            }

            _ = &mut next_ping => {
                let jitter_secs = rand::thread_rng().gen_range(25.0..35.0);
                next_ping.as_mut().reset(Instant::now() + Duration::from_secs_f64(jitter_secs));

                let ping = json!({"type": "ping"});
                if let Err(e) = ws_stream.send(Message::Text(ping.to_string())).await {
                    debug!("[{alias}] Ping error for {streamer}: {e}");
                    break;
                }
            }

            msg_opt = ws_stream.next() => {
                match msg_opt {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Text(txt) => {
                                let trimmed = txt.trim();
                                if trimmed == "ping" || trimmed.contains("\"type\":\"ping\"") {
                                    let _ = ws_stream.send(Message::Text(json!({"type": "pong"}).to_string())).await;
                                }
                                // Drop chat messages immediately without saving in memory
                            }
                            Message::Ping(p) => {
                                let _ = ws_stream.send(Message::Pong(p)).await;
                            }
                            Message::Close(_) => {
                                info!("[{alias}] WS closed by server for {streamer}");
                                break;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        warn!("[{alias}] WS error for {streamer}: {e}");
                        break;
                    }
                    None => {
                        debug!("[{alias}] WS stream ended for {streamer}");
                        break;
                    }
                }
            }
        }
    }

    let _ = ws_stream.close(None).await;
    {
        let mut s = state.write().await;
        s.set_watching(&alias, &streamer, false);
        s.append_log(format!("[{alias}] Stopped watching {streamer}"));
    }
    info!("[{alias}] Finished watching {streamer}");
}

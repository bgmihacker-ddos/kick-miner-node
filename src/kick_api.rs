use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, ORIGIN, REFERER, USER_AGENT,
};
use reqwest::{Client, Proxy};
use serde_json::Value;
use std::time::Duration;
use tracing::{debug, error, warn};

pub fn build_http_client(proxy_url: Option<&str>) -> Client {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ),
    );
    headers.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
    headers.insert(
        HeaderName::from_static("accept-language"),
        HeaderValue::from_static("en-US,en;q=0.9"),
    );
    headers.insert(ORIGIN, HeaderValue::from_static("https://kick.com"));
    headers.insert(REFERER, HeaderValue::from_static("https://kick.com/"));
    headers.insert(
        HeaderName::from_static("x-client-token"),
        HeaderValue::from_static("e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823"),
    );
    headers.insert(
        HeaderName::from_static("x-requested-with"),
        HeaderValue::from_static("XMLHttpRequest"),
    );
    headers.insert(
        HeaderName::from_static("sec-ch-ua"),
        HeaderValue::from_static("\"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\", \"Not?A_Brand\";v=\"99\""),
    );
    headers.insert(
        HeaderName::from_static("sec-ch-ua-mobile"),
        HeaderValue::from_static("?0"),
    );
    headers.insert(
        HeaderName::from_static("sec-ch-ua-platform"),
        HeaderValue::from_static("\"Windows\""),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-dest"),
        HeaderValue::from_static("empty"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-mode"),
        HeaderValue::from_static("cors"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-site"),
        HeaderValue::from_static("same-site"),
    );

    let mut builder = Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(15));

    if let Some(proxy_str) = proxy_url {
        if !proxy_str.is_empty() {
            if let Ok(p) = Proxy::all(proxy_str) {
                builder = builder.proxy(p);
            } else {
                warn!("Invalid proxy string: {}", proxy_str);
            }
        }
    }

    builder.build().unwrap_or_else(|_| Client::new())
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ChannelDetails {
    pub channel_id: u64,
    pub user_id: u64,
    pub is_live: bool,
    pub livestream_id: Option<u64>,
    pub title: String,
}

pub async fn get_channel_info(
    client: &Client,
    streamer: &str,
    auth_token: Option<&str>,
) -> Option<ChannelDetails> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}");
    let mut req = client
        .get(&url)
        .header(REFERER, format!("https://kick.com/{streamer}/"));

    if let Some(tok) = auth_token {
        if !tok.is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {tok}"));
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            debug!("Channel API error for {streamer}: {e}");
            return None;
        }
    };

    if !resp.status().is_success() {
        debug!("Channel API [{streamer}] HTTP status: {}", resp.status());
        return None;
    }

    let val: Value = resp.json().await.ok()?;
    let channel_id = val.get("id").and_then(|v| v.as_u64())?;
    let user_id = val
        .get("user_id")
        .and_then(|v| v.as_u64())
        .or_else(|| val.pointer("/user/id").and_then(|v| v.as_u64()))
        .unwrap_or(channel_id);

    let livestream = val.get("livestream");
    let (is_live, livestream_id, title) = if let Some(ls) = livestream {
        if ls.is_null() {
            (false, None, String::new())
        } else {
            let ls_id = ls.get("id").and_then(|v| v.as_u64());
            let title = ls
                .get("session_title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            (ls_id.is_some(), ls_id, title)
        }
    } else {
        (false, None, String::new())
    };

    Some(ChannelDetails {
        channel_id,
        user_id,
        is_live,
        livestream_id,
        title,
    })
}

pub async fn get_ws_token(
    client: &Client,
    streamer: &str,
    channel_id: u64,
    user_id: u64,
    token: &str,
) -> Option<String> {
    let url = "https://websockets.kick.com/viewer/v1/token";
    let resp = client
        .get(url)
        .header(REFERER, format!("https://kick.com/{streamer}/"))
        .header("X-Chatroom", channel_id.to_string())
        .header("X-User-Id", user_id.to_string())
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        warn!("Failed to fetch WS token for {streamer}: HTTP {}", resp.status());
        return None;
    }

    let data: Value = resp.json().await.ok()?;
    let ws_tok = data
        .pointer("/data/token")
        .or_else(|| data.pointer("/data/websocket_token"))
        .or_else(|| data.get("token"))
        .or_else(|| data.get("websocket_token"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    ws_tok
}

pub async fn get_live_points(client: &Client, streamer: &str, token: &str) -> Option<u64> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}/points");
    let resp = client
        .get(&url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(REFERER, format!("https://kick.com/{streamer}/"))
        .send()
        .await
        .ok()?;

    if resp.status().is_success() {
        let val: Value = resp.json().await.ok()?;
        let pts = val
            .pointer("/data/points")
            .or_else(|| val.get("points"))
            .and_then(|v| v.as_u64());
        if let Some(p) = pts {
            return Some(p);
        }
    }

    // Fallback: query /channels/{streamer}
    let alt_url = format!("https://kick.com/api/v2/channels/{streamer}");
    if let Ok(alt_resp) = client
        .get(&alt_url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
    {
        if alt_resp.status().is_success() {
            if let Ok(val) = alt_resp.json::<Value>().await {
                if let Some(pts) = val
                    .pointer("/data/user/points")
                    .or_else(|| val.pointer("/user/points"))
                    .and_then(|v| v.as_u64())
                {
                    return Some(pts);
                }
            }
        }
    }

    None
}

pub async fn vote_prediction(
    client: &Client,
    streamer: &str,
    outcome_id: &str,
    amount: u64,
    token: &str,
) -> Result<Value, String> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}/predictions/vote");
    let payload = serde_json::json!({
        "amount": amount,
        "outcome_id": outcome_id
    });

    let resp = match client
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(REFERER, format!("https://kick.com/{streamer}/"))
        .header(CONTENT_TYPE, "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return Err(format!("Network error: {e}")),
    };

    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        match serde_json::from_str::<Value>(&body_text) {
            Ok(v) => Ok(v),
            Err(_) => Ok(serde_json::json!({"status": "ok", "raw": body_text})),
        }
    } else {
        error!("Vote prediction failed for {streamer} (HTTP {status}): {body_text}");
        Err(format!("HTTP {status}: {body_text}"))
    }
}

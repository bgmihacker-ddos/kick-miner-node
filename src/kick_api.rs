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

/// Executes an HTTP request via curl-impersonate (Chrome 116 TLS fingerprint) to bypass Cloudflare
pub async fn exec_impersonate(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
    proxy: Option<&str>,
) -> Result<String, String> {
    let curl_bin = if std::path::Path::new("/usr/local/bin/curl_chrome116").exists() {
        Some("/usr/local/bin/curl_chrome116")
    } else if std::path::Path::new("curl_chrome116").exists() {
        Some("curl_chrome116")
    } else {
        None
    };

    let bin = match curl_bin {
        Some(b) => b,
        None => return Err("curl_chrome116 not available".to_string()),
    };

    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("-s").arg("-S").arg("--max-time").arg("12");

    if method == "POST" {
        cmd.arg("-X").arg("POST");
    }

    for (k, v) in headers {
        cmd.arg("-H").arg(format!("{}: {}", k, v));
    }

    if let Some(p) = proxy {
        if !p.is_empty() {
            cmd.arg("-x").arg(p);
        }
    }

    if let Some(b) = body {
        cmd.arg("-d").arg(b);
    }

    cmd.arg(url);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("exec error: {e}"))?;

    let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();
    if output.status.success() {
        Ok(stdout_str)
    } else {
        let stderr_str = String::from_utf8_lossy(&output.stderr).to_string();
        Err(format!("curl error: {stderr_str} | stdout: {stdout_str}"))
    }
}

pub async fn get_channel_info(
    client: &Client,
    streamer: &str,
    auth_token: Option<&str>,
    proxy: Option<&str>,
) -> Option<ChannelDetails> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}");
    let ref_hdr = format!("https://kick.com/{streamer}/");
    let mut hdrs: Vec<(&str, &str)> = vec![
        ("Accept", "application/json, text/plain, */*"),
        ("Referer", &ref_hdr),
    ];
    let auth_str;
    if let Some(tok) = auth_token {
        if !tok.is_empty() {
            auth_str = format!("Bearer {tok}");
            hdrs.push(("Authorization", &auth_str));
        }
    }

    let raw_val: Option<Value> = if let Ok(stdout) = exec_impersonate("GET", &url, &hdrs, None, proxy).await {
        serde_json::from_str::<Value>(&stdout).ok()
    } else {
        // Fallback to reqwest
        let mut req = client.get(&url).header(REFERER, &ref_hdr);
        if let Some(tok) = auth_token {
            if !tok.is_empty() {
                req = req.header(AUTHORIZATION, format!("Bearer {tok}"));
            }
        }
        if let Ok(resp) = req.send().await {
            if resp.status().is_success() {
                resp.json().await.ok()
            } else {
                debug!("Channel API [{streamer}] HTTP status: {}", resp.status());
                None
            }
        } else {
            None
        }
    };

    let val = raw_val?;
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
    _channel_id: u64,
    _user_id: u64,
    token: &str,
    proxy: Option<&str>,
) -> Option<String> {
    let url = "https://websockets.kick.com/viewer/v1/token";
    let ref_hdr = format!("https://kick.com/{streamer}/");
    let auth_str = format!("Bearer {token}");
    let hdrs: Vec<(&str, &str)> = vec![
        ("Accept", "application/json, text/plain, */*"),
        ("Origin", "https://kick.com"),
        ("Referer", &ref_hdr),
        ("Authorization", &auth_str),
        (
            "x-client-token",
            "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823",
        ),
        ("x-requested-with", "XMLHttpRequest"),
    ];

    let raw_val: Option<Value> = if let Ok(stdout) = exec_impersonate("GET", url, &hdrs, None, proxy).await {
        serde_json::from_str::<Value>(&stdout).ok()
    } else {
        let resp = client
            .get(url)
            .header(REFERER, &ref_hdr)
            .header(ORIGIN, "https://kick.com")
            .header("x-client-token", "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823")
            .header("x-requested-with", "XMLHttpRequest")
            .header(AUTHORIZATION, &auth_str)
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await
            .ok()?;
        if resp.status().is_success() {
            resp.json().await.ok()
        } else {
            warn!("Failed to fetch WS token for {streamer}: HTTP {}", resp.status());
            None
        }
    };

    let data = raw_val?;
    let ws_tok = data
        .pointer("/data/token")
        .or_else(|| data.pointer("/data/websocket_token"))
        .or_else(|| data.get("token"))
        .or_else(|| data.get("websocket_token"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    ws_tok
}

pub async fn get_live_points(
    client: &Client,
    streamer: &str,
    token: &str,
    proxy: Option<&str>,
) -> Option<u64> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}/points");
    let ref_hdr = format!("https://kick.com/{streamer}/");
    let auth_str = format!("Bearer {token}");
    let hdrs: Vec<(&str, &str)> = vec![
        ("Accept", "application/json, text/plain, */*"),
        ("Referer", &ref_hdr),
        ("Authorization", &auth_str),
    ];

    if let Ok(stdout) = exec_impersonate("GET", &url, &hdrs, None, proxy).await {
        if let Ok(val) = serde_json::from_str::<Value>(&stdout) {
            let pts = val
                .pointer("/data/points")
                .or_else(|| val.get("points"))
                .and_then(|v| v.as_u64());
            if let Some(p) = pts {
                return Some(p);
            }
        }
    }

    // Fallback reqwest
    let resp = client
        .get(&url)
        .header(AUTHORIZATION, &auth_str)
        .header(REFERER, &ref_hdr)
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

    None
}

pub async fn vote_prediction(
    client: &Client,
    streamer: &str,
    outcome_id: &str,
    amount: u64,
    token: &str,
    proxy: Option<&str>,
) -> Result<Value, String> {
    let url = format!("https://kick.com/api/v2/channels/{streamer}/predictions/vote");
    let ref_hdr = format!("https://kick.com/{streamer}/");
    let auth_str = format!("Bearer {token}");
    let hdrs: Vec<(&str, &str)> = vec![
        ("Accept", "application/json, text/plain, */*"),
        ("Referer", &ref_hdr),
        ("Authorization", &auth_str),
        ("Content-Type", "application/json"),
    ];
    let payload = serde_json::json!({
        "amount": amount,
        "outcome_id": outcome_id
    });
    let payload_str = payload.to_string();

    if let Ok(stdout) = exec_impersonate("POST", &url, &hdrs, Some(&payload_str), proxy).await {
        if let Ok(v) = serde_json::from_str::<Value>(&stdout) {
            return Ok(v);
        } else {
            return Ok(serde_json::json!({"status": "ok", "raw": stdout}));
        }
    }

    // Fallback reqwest
    let resp = match client
        .post(&url)
        .header(AUTHORIZATION, &auth_str)
        .header(REFERER, &ref_hdr)
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

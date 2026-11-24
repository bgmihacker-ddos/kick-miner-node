use chrono::Utc;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct StreamerStatus {
    pub online: bool,
    pub watching: bool,
    pub points: u64,
    pub priority: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_update: Option<String>,
}

impl Default for StreamerStatus {
    fn default() -> Self {
        Self {
            online: false,
            watching: false,
            points: 0,
            priority: 1,
            stream_id: None,
            channel_id: None,
            last_update: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct AccountStatus {
    pub alias: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub token: String,
    #[serde(skip_serializing)]
    pub proxy: Option<String>,
    pub streamers: HashMap<String, StreamerStatus>,
    #[serde(skip_serializing)]
    pub max_concurrent: usize,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AppState {
    pub start_time: Instant,
    pub accounts: Vec<AccountStatus>,
    pub common_streamers: Vec<String>,
    pub recent_logs: VecDeque<String>,
}

impl AppState {
    pub fn new(streamers: Vec<String>) -> Self {
        Self {
            start_time: Instant::now(),
            accounts: Vec::new(),
            common_streamers: streamers,
            recent_logs: VecDeque::with_capacity(250),
        }
    }

    pub fn append_log(&mut self, msg: String) {
        if self.recent_logs.len() >= 250 {
            self.recent_logs.pop_front();
        }
        let ts = Utc::now().format("%H:%M:%S").to_string();
        self.recent_logs.push_back(format!("[{ts}] {msg}"));
    }

    pub fn update_streamer_online(
        &mut self,
        streamer: &str,
        is_live: bool,
        stream_id: Option<u64>,
        channel_id: Option<u64>,
    ) {
        let ts = Utc::now().format("%H:%M:%S").to_string();
        for acc in &mut self.accounts {
            let entry = acc
                .streamers
                .entry(streamer.to_string())
                .or_insert_with(StreamerStatus::default);
            entry.online = is_live;
            if is_live {
                entry.stream_id = stream_id;
                entry.channel_id = channel_id;
            } else {
                entry.watching = false;
            }
            entry.last_update = Some(ts.clone());
        }
    }

    pub fn set_watching(&mut self, alias: &str, streamer: &str, watching: bool) {
        if let Some(acc) = self.accounts.iter_mut().find(|a| a.alias == alias) {
            if let Some(s) = acc.streamers.get_mut(streamer) {
                s.watching = watching;
            }
        }
    }

    pub fn set_points(&mut self, alias: &str, streamer: &str, pts: u64) {
        let ts = Utc::now().format("%H:%M:%S").to_string();
        if let Some(acc) = self.accounts.iter_mut().find(|a| a.alias == alias) {
            let entry = acc
                .streamers
                .entry(streamer.to_string())
                .or_insert_with(StreamerStatus::default);
            entry.points = pts;
            entry.last_update = Some(ts);
        }
    }

    pub fn deduct_points(&mut self, alias: &str, streamer: &str, amount: u64) {
        if let Some(acc) = self.accounts.iter_mut().find(|a| a.alias == alias) {
            if let Some(s) = acc.streamers.get_mut(streamer) {
                s.points = s.points.saturating_sub(amount);
            }
        }
    }
}

pub type SharedState = Arc<RwLock<AppState>>;

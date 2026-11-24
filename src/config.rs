use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    #[serde(default = "default_alias")]
    pub alias: String,
    pub token: String,
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub streamers: Vec<String>,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
}

fn default_alias() -> String {
    "account".to_string()
}

fn default_max_concurrent() -> usize {
    4
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct AppConfig {
    #[serde(default)]
    pub Streamers: Vec<String>,
    #[serde(default)]
    pub Accounts: Vec<AccountConfig>,
    #[serde(default = "default_check_interval")]
    pub Check_interval: u64,
    #[serde(default = "default_port")]
    pub WorkerPort: u16,
    #[serde(default)]
    pub Debug: bool,
}

fn default_check_interval() -> u64 {
    60
}

fn default_port() -> u16 {
    10000
}

impl AppConfig {
    pub fn load() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut config: AppConfig = if let Ok(json_env) = env::var("CONFIG_JSON") {
            info!("Loading configuration from CONFIG_JSON environment variable");
            serde_json::from_str(&json_env)?
        } else if let Ok(content) = fs::read_to_string("config.json") {
            info!("Loading configuration from config.json file");
            serde_json::from_str(&content)?
        } else if let Ok(content) = fs::read_to_string("config.example.json") {
            warn!("config.json not found, falling back to config.example.json");
            serde_json::from_str(&content)?
        } else {
            return Err("No config source found (neither CONFIG_JSON nor config.json)".into());
        };

        // Override port from PORT env var if present (e.g. on Render)
        if let Ok(port_str) = env::var("PORT") {
            if let Ok(p) = port_str.trim().parse::<u16>() {
                config.WorkerPort = p;
            }
        }

        // Propagate global streamers to accounts if account streamers list is empty
        for acc in &mut config.Accounts {
            if acc.streamers.is_empty() && !config.Streamers.is_empty() {
                acc.streamers = config.Streamers.clone();
            }
        }

        info!(
            "Loaded config: {} accounts, {} streamers, port {}",
            config.Accounts.len(),
            config.Streamers.len(),
            config.WorkerPort
        );

        Ok(config)
    }
}

mod config;
mod kick_api;
mod miner_manager;
mod server;
mod state;
mod ws_watcher;

use config::AppConfig;
use miner_manager::run_miner_loop;
use server::create_router;
use state::{AccountStatus, AppState, SharedState, StreamerStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kick_miner=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("=======================================================");
    info!("⚡ Kick Channel Points Miner - Rust High-Performance Edition");
    info!("=======================================================");

    let config = AppConfig::load()?;
    let port = config.WorkerPort;

    let mut initial_state = AppState::new(config.Streamers.clone());

    for acc in &config.Accounts {
        let mut streamers_map: HashMap<String, StreamerStatus> = HashMap::new();
        for s in &acc.streamers {
            streamers_map.insert(s.clone(), StreamerStatus::default());
        }

        initial_state.accounts.push(AccountStatus {
            alias: acc.alias.clone(),
            username: acc.alias.clone(),
            token: acc.token.clone(),
            proxy: acc.proxy.clone(),
            streamers: streamers_map,
            max_concurrent: acc.max_concurrent,
        });
    }

    let shared_state: SharedState = Arc::new(RwLock::new(initial_state));

    // Spawn background miner loop
    let miner_cfg = config.clone();
    let miner_state = Arc::clone(&shared_state);
    tokio::spawn(async move {
        run_miner_loop(miner_cfg, miner_state).await;
    });

    // Start HTTP API server
    let app = create_router(shared_state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🚀 Web server listening on http://0.0.0.0:{}", port);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

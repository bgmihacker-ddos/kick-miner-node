use crate::config::AppConfig;
use crate::kick_api::{build_http_client, get_channel_info, get_live_points, get_ws_token};
use crate::state::SharedState;
use crate::ws_watcher::run_ws_watcher;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, info};

pub async fn run_miner_loop(config: AppConfig, state: SharedState) {
    let global_client = build_http_client(None);
    let mut account_clients: HashMap<String, reqwest::Client> = HashMap::new();
    for acc in &config.Accounts {
        let client = build_http_client(acc.proxy.as_deref());
        account_clients.insert(acc.alias.clone(), client);
    }

    // Pick first available account token for authenticated channel status checks
    let primary_token = config.Accounts.first().map(|a| a.token.clone());

    // Active watcher stop senders: key = (alias, streamer)
    let mut active_watchers: HashMap<(String, String), watch::Sender<bool>> = HashMap::new();
    let mut cycle_count: u64 = 0;

    info!(
        "Starting miner loop with interval of {}s across {} streamers and {} accounts",
        config.Check_interval,
        config.Streamers.len(),
        config.Accounts.len()
    );

    // Initial immediate balance fetch for all accounts in background so watchers can start immediately
    let bg_accounts = config.Accounts.clone();
    let bg_account_clients = account_clients.clone();
    let bg_global_client = global_client.clone();
    let bg_state = Arc::clone(&state);

    tokio::spawn(async move {
        info!("Background points balance fetcher started for all accounts...");
        for acc in &bg_accounts {
            let acc_client = bg_account_clients
                .get(&acc.alias)
                .cloned()
                .unwrap_or_else(|| bg_global_client.clone());

            for streamer in &acc.streamers {
                if let Some(pts) =
                    get_live_points(&acc_client, streamer, &acc.token, acc.proxy.as_deref()).await
                {
                    let mut s = bg_state.write().await;
                    s.set_points(&acc.alias, streamer, pts);
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
        info!("Background initial points balance fetch complete!");
    });

    loop {
        cycle_count += 1;
        debug!("Miner check cycle #{cycle_count}");

        // 1. Check online status for all streamers (using primary account token to avoid Cloudflare 403)
        let mut live_streamers: HashMap<String, (u64, u64, u64)> = HashMap::new(); // streamer -> (channel_id, user_id, stream_id)
        for streamer in &config.Streamers {
            if let Some(details) = get_channel_info(
                &global_client,
                streamer,
                primary_token.as_deref(),
                None,
            )
            .await
            {
                {
                    let mut s = state.write().await;
                    s.update_streamer_online(
                        streamer,
                        details.is_live,
                        details.livestream_id,
                        Some(details.channel_id),
                    );
                }
                if details.is_live {
                    if let Some(ls_id) = details.livestream_id {
                        live_streamers.insert(
                            streamer.clone(),
                            (details.channel_id, details.user_id, ls_id),
                        );
                    }
                }
            }
            // slight stagger between streamer checks
            sleep(Duration::from_millis(100)).await;
        }

        if !live_streamers.is_empty() {
            info!(
                "Live streamers detected ({}): {:?}",
                live_streamers.len(),
                live_streamers.keys().collect::<Vec<_>>()
            );
        }

        // 2. Stop watchers for streamers that are no longer live
        active_watchers.retain(|(alias, streamer), stop_tx| {
            if !live_streamers.contains_key(streamer) {
                info!("[{alias}] Streamer {streamer} went offline; stopping watcher");
                let _ = stop_tx.send(true);
                false
            } else {
                !stop_tx.is_closed()
            }
        });

        // 3. For each account, launch watchers up to max_concurrent
        for acc in &config.Accounts {
            let acc_client = account_clients
                .get(&acc.alias)
                .cloned()
                .unwrap_or_else(|| global_client.clone());

            let mut current_watching_count = active_watchers
                .keys()
                .filter(|(alias, _)| alias == &acc.alias)
                .count();

            for streamer in &acc.streamers {
                if current_watching_count >= acc.max_concurrent {
                    break;
                }

                let key = (acc.alias.clone(), streamer.clone());
                if active_watchers.contains_key(&key) {
                    continue;
                }

                if let Some(&(channel_id, user_id, livestream_id)) = live_streamers.get(streamer) {
                    debug!("[{}] Acquiring WS token for live streamer {}", acc.alias, streamer);
                    if let Some(ws_token) = get_ws_token(
                        &acc_client,
                        streamer,
                        channel_id,
                        user_id,
                        &acc.token,
                        acc.proxy.as_deref(),
                    )
                    .await
                    {
                        let (stop_tx, stop_rx) = watch::channel(false);
                        active_watchers.insert(key, stop_tx);
                        current_watching_count += 1;

                        let a_alias = acc.alias.clone();
                        let s_name = streamer.clone();
                        let shared = Arc::clone(&state);

                        tokio::spawn(async move {
                            run_ws_watcher(
                                a_alias,
                                s_name,
                                channel_id,
                                livestream_id,
                                ws_token,
                                shared,
                                stop_rx,
                            )
                            .await;
                        });
                    }
                }
            }

            // 4. Update points periodically for live streamers (priority every 2 cycles)
            if cycle_count % 2 == 0 {
                for (live_s, _) in &live_streamers {
                    if acc.streamers.contains(live_s) {
                        if let Some(pts) = get_live_points(
                            &acc_client,
                            live_s,
                            &acc.token,
                            acc.proxy.as_deref(),
                        )
                        .await
                        {
                            let mut s = state.write().await;
                            s.set_points(&acc.alias, live_s, pts);
                        }
                    }
                }
            }
        }

        sleep(Duration::from_secs(config.Check_interval)).await;
    }
}

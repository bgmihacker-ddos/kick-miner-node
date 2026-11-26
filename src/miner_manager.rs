use crate::config::AppConfig;
use crate::kick_api::{build_http_client, get_channel_info, get_live_points, get_ws_token};
use crate::state::SharedState;
use crate::ws_watcher::run_ws_watcher;
use std::collections::{HashMap, HashSet};
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
        // Let main loop detect live streamers and connect WS viewers FIRST with zero WAF pressure
        sleep(Duration::from_secs(20)).await;
        info!("Background points balance fetcher started gently for all accounts...");
        let mut fail_streak = 0u32;
        for acc in &bg_accounts {
            let acc_client = bg_account_clients
                .get(&acc.alias)
                .cloned()
                .unwrap_or_else(|| bg_global_client.clone());

            for streamer in &acc.streamers {
                if let Some(pts) =
                    get_live_points(&acc_client, streamer, &acc.token, acc.proxy.as_deref()).await
                {
                    fail_streak = 0;
                    let mut s = bg_state.write().await;
                    s.set_points(&acc.alias, streamer, pts);
                    sleep(Duration::from_millis(650)).await;
                } else {
                    fail_streak += 1;
                    // Exponential backoff if Kick WAF throttles
                    let backoff_ms = if fail_streak >= 3 { 8000 } else { 1500 };
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }
        info!("Background initial points balance fetch complete!");
    });

    loop {
        cycle_count += 1;
        debug!("Miner check cycle #{cycle_count}");

        // 1. Check online status for all streamers (using primary account token to avoid Cloudflare 403)
        let mut live_streamers: HashMap<String, (u64, u64, u64)> = HashMap::new(); // streamer -> (channel_id, user_id, stream_id)
        let mut confirmed_offline: HashSet<String> = HashSet::new();
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
                } else {
                    confirmed_offline.insert(streamer.clone());
                }
            }
            // stagger between streamer checks to avoid WAF rate limit
            sleep(Duration::from_millis(250)).await;
        }

        if !live_streamers.is_empty() {
            info!(
                "Live streamers detected ({}): {:?}",
                live_streamers.len(),
                live_streamers.keys().collect::<Vec<_>>()
            );
        }

        // 2. Only stop watchers if Kick CONFIRMED the streamer went offline (never drop on transient 429)
        active_watchers.retain(|(alias, streamer), stop_tx| {
            if confirmed_offline.contains(streamer) {
                info!("[{alias}] Streamer {streamer} went offline; stopping watcher");
                let _ = stop_tx.send(true);
                false
            } else {
                !stop_tx.is_closed()
            }
        });

        // 3. For each account, launch watchers up to max_concurrent (staggered to avoid Kick 429)
        let mut ws_fail_streak = 0u32;
        for acc in &config.Accounts {
            if ws_fail_streak < 2 {
                let acc_client = account_clients
                    .get(&acc.alias)
                    .cloned()
                    .unwrap_or_else(|| global_client.clone());

                let mut current_watching_count = active_watchers
                    .keys()
                    .filter(|(alias, _)| alias == &acc.alias)
                    .count();

                for streamer in &acc.streamers {
                    if current_watching_count >= acc.max_concurrent || ws_fail_streak >= 2 {
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
                            ws_fail_streak = 0;
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
                            sleep(Duration::from_millis(450)).await;
                        } else {
                            ws_fail_streak += 1;
                            sleep(Duration::from_millis(1200)).await;
                        }
                    }
                }
            }

            // 4. Update points periodically for live streamers (every 4 cycles with gentle stagger)
            if cycle_count % 4 == 0 {
                let acc_client = account_clients
                    .get(&acc.alias)
                    .cloned()
                    .unwrap_or_else(|| global_client.clone());
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
                        sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }

        sleep(Duration::from_secs(config.Check_interval)).await;
    }
}

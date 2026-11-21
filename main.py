import asyncio
import json
import sys
import os
import time
import traceback

from loguru import logger
from localization import load_language, t

from account_manager import AccountManager
from discord_webhook import DiscordWebhook
import web_server
import urllib.request
import ssl

# тест памяти
ENABLE_MEMORY_MONITOR = False 

log_memory_usage = None
if ENABLE_MEMORY_MONITOR:
    try:
        from memory_monitor import log_memory_usage
    except ImportError:
        logger.warning("⚠️ memory_monitor.py не найден, хотя ENABLE_MEMORY_MONITOR=True")

telegram_bot = None
discord_hook = None
account_manager = None
github_sync = None


def _perform_health_query(url: str) -> int:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
        }
    )
    with urllib.request.urlopen(req, timeout=25, context=ctx) as response:
        return response.getcode()


async def start_healthcheck_query(target_url: str = "", interval: int = 300):
    """
    Render Cross-Miner Anti-Sleep Mesh:
    Pings Master Panel and all miner nodes every 5 minutes (300s).
    Ensures neither Master Panel nor any miner in the Render cluster ever goes to sleep.
    """
    endpoints = [
        ("Master Panel", "https://kick-master-panel-pwz6.onrender.com/health"),
        ("Miner 2 (Self)", "https://miner-2-dl7d.onrender.com/health"),
        ("Miner 3 (Singapore)", "https://miner-3-1.onrender.com/health"),
        ("Miner 4 (Frankfurt)", "https://miner-4.onrender.com/health"),
        ("Miner 5 (Oregon)", "https://miner-5-y5rn.onrender.com/health"),
        ("Miner 6 (Ohio)", "https://miner-6.onrender.com/health"),
        ("Miner 7 (Virginia)", "https://miner-7.onrender.com/health"),
        ("Miner 8 (Frankfurt)", "https://miner-8.onrender.com/health"),
    ]

    logger.info(f"💓 Render Anti-Sleep Cross-Pinger active (monitoring {len(endpoints)} nodes every {interval}s)")
    await asyncio.sleep(15)

    pulse_count = 0
    while True:
        try:
            pulse_count += 1
            success_count = 0
            for name, url in endpoints:
                try:
                    code = await asyncio.to_thread(_perform_health_query, url)
                    if code == 200:
                        success_count += 1
                    else:
                        logger.debug(f"[AntiSleep] {name} returned HTTP {code}")
                except Exception as ex:
                    logger.debug(f"[AntiSleep] {name} ping notice: {ex}")

            if pulse_count == 1 or pulse_count % 3 == 0 or success_count < len(endpoints):
                logger.info(f"💓 [AntiSleep Pulse #{pulse_count}] {success_count}/{len(endpoints)} nodes kept awake & online")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"⚠️ Anti-Sleep cross-ping loop notice: {e}")

        await asyncio.sleep(interval)

async def main():
    global account_manager, telegram_bot, discord_hook

    # 1. Config loading
    try:
        if os.environ.get("CONFIG_JSON"):
            logger.info("📄 Loading configuration from CONFIG_JSON environment variable")
            config = json.loads(os.environ["CONFIG_JSON"])
        elif os.path.exists("config.json"):
            with open("config.json", "r", encoding="utf-8") as f:
                config = json.load(f)
        elif os.path.exists("config.example.json"):
            logger.warning("⚠️ config.json not found, falling back to config.example.json")
            with open("config.example.json", "r", encoding="utf-8") as f:
                config = json.load(f)
        else:
            raise FileNotFoundError("config.json not found and CONFIG_JSON environment variable is not set")

        logger.remove()
        log_level = "DEBUG" if config.get("Debug", False) else "INFO"
        logger.add(sys.stderr, level=log_level)
        logger.info(f"🔧 Log level: {log_level}")

        load_language(config.get("Language", "en"))

    except Exception as e:
        logger.add(sys.stderr, level="INFO")
        logger.critical(f"Config load error: {e}")
        return

    # 1.5. GitHub Auto-Sync (Render Persistence)
    global github_sync
    try:
        from utils.github_sync import GitHubSync
        from account_manager import log_bet_to_csv
        github_sync = GitHubSync(config)
        if github_sync.enabled:
            github_sync.pull_on_startup()
            github_sync.start()
            setattr(log_bet_to_csv, "_sync_instance", github_sync)
    except Exception as e:
        logger.warning(f"🐙 GitHub Auto-Sync init warning: {e}")

    # 2. Account Manager
    account_manager = AccountManager(config)
    all_streamers = account_manager.get_all_streamers_flat()
    logger.info(f"📋 Total unique streamers: {len(all_streamers)}")

    # 3. Discord Webhook
    discord_hook = DiscordWebhook(config)
    if discord_hook.enabled:
        account_manager.set_discord(discord_hook)
        logger.info("🟣 Discord webhook enabled")

    # 4. Telegram
    if config.get("Telegram", {}).get("enabled", False):
        try:
            from tg_bot.bot import TelegramBot
            telegram_bot = TelegramBot(config)
            telegram_bot.set_account_manager(account_manager)
            account_manager.set_telegram(telegram_bot)
            await telegram_bot.start()
        except Exception as e:
            logger.error(f"Telegram bot failed to start: {e}")

    # 5. Web Dashboard
    web_cfg = config.get("WebDashboard", {})
    is_render = bool(os.environ.get("RENDER") or os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("PORT"))
    web_enabled = web_cfg.get("enabled", True) or is_render
    port = int(os.environ.get("PORT", web_cfg.get("port", 5000)))

    if web_enabled:
        try:
            web_server.start_server(account_manager, port)
            logger.info(f"🌍 Web Dashboard: http://0.0.0.0:{port}")
        except Exception as e:
            logger.error(f"Web Dashboard failed to start: {e}")

    # 6. Render Anti-Sleep Cross-Pinger Mesh (every 5 minutes / 300s)
    health_cfg = config.get("HealthCheck", {})
    if health_cfg.get("enabled", True):
        interval = health_cfg.get("interval", 300)
        asyncio.create_task(start_healthcheck_query(interval=interval))

    # 6.5. Render Memory Guard (aggressive GC above 320MB to prevent Render 512MB OOM)
    async def _memory_guard_loop():
        import gc
        try:
            import psutil
            proc = psutil.Process()
        except Exception:
            proc = None

        tick = 0
        while True:
            try:
                tick += 1
                if proc:
                    rss_mb = proc.memory_info().rss / 1024 / 1024
                    if rss_mb > 320.0:
                        before = rss_mb
                        gc.collect()
                        after = proc.memory_info().rss / 1024 / 1024
                        logger.warning(f"🧹 [MemoryGuard] High RAM ({before:.1f}MB) -> Reclaimed to {after:.1f}MB")
                if tick % 4 == 0:
                    gc.collect()
            except Exception:
                pass
            await asyncio.sleep(30)

    asyncio.create_task(_memory_guard_loop())
    logger.info("🛡️ Render Memory Guard active (protecting 512MB RAM limit)")

    # 7. Send startup notifications
    if discord_hook.enabled:
        discord_hook.send_startup(
            account_manager.get_all_status()
        )

    if log_memory_usage:
        asyncio.create_task(log_memory_usage(interval=60))
        logger.info("📊 Memory Monitor: STARTED")

    # 8. Start all accounts
    await account_manager.start_all()


if __name__ == "__main__":
    while True:
        try:
            logger.info("🚀 Starting Miner...")
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("👋 Stopped by user")
            if github_sync:
                github_sync.stop()
            if discord_hook and discord_hook.enabled:
                discord_hook.send_restart("User stopped (Ctrl+C)")
            sys.exit(0)
        except SystemExit:
            logger.info("🔄 Restarting on SystemExit...")
            if discord_hook and discord_hook.enabled:
                discord_hook.send_restart("SystemExit")
        except Exception as e:
            logger.critical(f"🔥 Critical error: {e}")
            traceback.print_exc()
            if discord_hook and discord_hook.enabled:
                discord_hook.send_error("System", "main.py", str(e))

        logger.info("🔄 Restarting in 5 seconds...")
        time.sleep(5)

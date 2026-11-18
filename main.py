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


async def start_healthcheck_query(target_url: str, interval: int = 60):
    logger.info("💓 Render Keep-Alive / Healthcheck query active")
    logger.info(f"🎯 Target URL: {target_url} (querying every {interval}s)")

    # Wait 10 seconds before initial ping to allow the web server to start up
    await asyncio.sleep(10)

    ping_count = 0
    while True:
        try:
            status_code = await asyncio.to_thread(_perform_health_query, target_url)
            ping_count += 1
            if ping_count == 1 or ping_count % 10 == 0 or status_code != 200:
                logger.info(
                    f"💓 Healthcheck query #{ping_count} to {target_url} -> HTTP {status_code} (Render keep-alive OK)"
                )
            else:
                logger.debug(
                    f"💓 Healthcheck query #{ping_count} to {target_url} -> HTTP {status_code}"
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"⚠️ Healthcheck query to {target_url} failed: {e}")

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

    # 3. Web Dashboard vs. Worker API (START FIRST TO SATISFY RENDER PORT CHECK)
    web_cfg = config.get("WebDashboard", {})
    worker_mode = config.get("WorkerMode", False)
    is_render = bool(os.environ.get("RENDER") or os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("PORT"))
    web_enabled = web_cfg.get("enabled", True) or is_render
    port = int(os.environ.get("PORT", config.get("WorkerPort", web_cfg.get("port", 5000))))

    if worker_mode:
        from worker_api import WorkerAPI, memory_scrubber_task
        api = WorkerAPI(account_manager, port=port)
        await api.start()
        asyncio.create_task(memory_scrubber_task(interval_minutes=15))
        logger.success(f"🚀 Worker Mode ACTIVE on port {port}")
    elif web_enabled:
        try:
            import web_server
            web_server.start_server(account_manager, port)
            logger.info(f"🌍 Web Dashboard: http://0.0.0.0:{port}")
        except Exception as e:
            logger.error(f"Web Dashboard failed to start: {e}")

    # 4. Discord Webhook
    discord_hook = DiscordWebhook(config)
    if discord_hook.enabled:
        account_manager.set_discord(discord_hook)
        logger.info("🟣 Discord webhook enabled")

    # 5. Telegram (Runs in background so it never blocks Render health check)
    if config.get("Telegram", {}).get("enabled", False):
        try:
            from tg_bot.bot import TelegramBot
            telegram_bot = TelegramBot(config)
            telegram_bot.set_account_manager(account_manager)
            account_manager.set_telegram(telegram_bot)
            asyncio.create_task(telegram_bot.start())
            logger.success("🤖 Telegram Bot: Starting in background...")
        except Exception as e:
            logger.error(f"Telegram bot failed to start: {e}")

    # 6. Render Keep-Alive / Healthcheck query task (every 60 sec)
    health_cfg = config.get("HealthCheck", {})
    if health_cfg.get("enabled", True):
        interval = health_cfg.get("interval", 60)
        configured_url = health_cfg.get("url") or os.environ.get("HEALTHCHECK_URL")

        if configured_url:
            health_url = configured_url
        elif os.environ.get("RENDER_EXTERNAL_HOSTNAME"):
            health_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME').rstrip('/')}/health"
        elif os.environ.get("RENDER_EXTERNAL_URL"):
            health_url = f"{os.environ.get('RENDER_EXTERNAL_URL').rstrip('/')}/health"
        else:
            health_url = f"http://127.0.0.1:{port}/health"

        asyncio.create_task(start_healthcheck_query(health_url, interval=interval))

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

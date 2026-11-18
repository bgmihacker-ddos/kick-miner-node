import json
import time
import random
import threading
from typing import Optional, Dict, Any, List, Tuple
from loguru import logger
from curl_cffi import requests

class KickChatClient:
    """Handles Kick chat API interactions using curl_cffi with automatic direct failover."""
    _chatroom_cache: Dict[str, int] = {}
    _cache_lock = threading.Lock()

    @staticmethod
    def get_chatroom_id(streamer_slug: str, token: str, proxy: Optional[str] = None) -> Optional[int]:
        slug = streamer_slug.lower().strip()
        with KickChatClient._cache_lock:
            if slug in KickChatClient._chatroom_cache:
                return KickChatClient._chatroom_cache[slug]

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization": f"Bearer {token}",
            "Origin": "https://kick.com",
            "Referer": f"https://kick.com/{slug}",
        }

        # Try proxy first (if configured), then automatically failover to direct
        proxy_attempts = [proxy, None] if proxy else [None]
        for p in proxy_attempts:
            try:
                proxies = {"http": p, "https": p} if p else None
                with requests.Session(impersonate="chrome120", proxies=proxies) as session:
                    resp = session.get(f"https://kick.com/api/v2/channels/{slug}", headers=headers, timeout=12)
                    if resp.status_code == 200:
                        data = resp.json()
                        chatroom_id = None
                        if "chatroom" in data and isinstance(data["chatroom"], dict):
                            chatroom_id = data["chatroom"].get("id")
                        elif "data" in data and isinstance(data["data"], dict) and "chatroom" in data["data"]:
                            chatroom_id = data["data"]["chatroom"].get("id")
                        
                        if chatroom_id:
                            with KickChatClient._cache_lock:
                                KickChatClient._chatroom_cache[slug] = int(chatroom_id)
                            return int(chatroom_id)
            except Exception as e:
                if p:
                    logger.debug(f"[KickChat] Proxy failed resolving chatroom for {slug}, trying direct...")

        logger.warning(f"[KickChat] Could not find chatroom ID for {slug}")
        return None

    @staticmethod
    def send_comment(
        streamer_slug: str,
        chatroom_id: int,
        token: str,
        message: str,
        proxy: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Sends a single comment to the streamer's chatroom.
        If the assigned proxy fails (e.g. 402 tunnel error), automatically fails over to direct.
        Returns: (success: bool, status_message: str)
        """
        slug = streamer_slug.lower().strip()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Origin": "https://kick.com",
            "Referer": f"https://kick.com/{slug}",
        }
        payload = {
            "content": message.strip(),
            "type": "message"
        }

        proxy_attempts = [proxy, None] if proxy else [None]
        last_error = "Unknown error"

        for p in proxy_attempts:
            proxies = {"http": p, "https": p} if p else None
            try:
                with requests.Session(impersonate="chrome120", proxies=proxies) as session:
                    resp = session.post(
                        f"https://kick.com/api/v2/messages/send/{chatroom_id}",
                        headers=headers,
                        json=payload,
                        timeout=15
                    )

                    if resp.status_code in (200, 201):
                        mode = "via proxy" if p else "via direct failover"
                        logger.success(f"[KickChat] Sent to {slug}: \"{message}\" ({mode})")
                        return True, "Message sent successfully"
                    
                    try:
                        res_json = resp.json()
                        msg = res_json.get("message") or res_json.get("error") or str(res_json)
                        last_error = f"HTTP {resp.status_code}: {msg}"
                    except Exception:
                        last_error = f"HTTP {resp.status_code}: {resp.text[:100]}"
                    
                    if p:
                        logger.warning(f"[KickChat] Proxy returned {last_error}, retrying direct...")
            except Exception as e:
                last_error = str(e)
                if p:
                    logger.warning(f"[KickChat] Proxy network error ({e}), retrying direct...")

        logger.error(f"[KickChat] Failed sending comment to {slug}: {last_error}")
        return False, last_error


class CommentCampaignManager:
    """
    Manages staggered comment campaigns across accounts.
    Enforces 'Only 1 comment per account per streamer' until reset.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.comment_history: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.is_running = False
        self.current_streamer = ""
        self.total_accounts = 0
        self.processed_accounts = 0
        self.success_count = 0
        self.fail_count = 0
        self.current_status = "Idle"
        self.logs: List[Dict[str, Any]] = []
        self._thread: Optional[threading.Thread] = None

    def has_account_commented(self, account_alias: str, streamer_slug: str) -> bool:
        with self._lock:
            slug = streamer_slug.lower().strip()
            return slug in self.comment_history.get(account_alias, {})

    def get_commented_streamers_for_account(self, account_alias: str) -> List[str]:
        with self._lock:
            return list(self.comment_history.get(account_alias, {}).keys())

    def reset_history(self, streamer_slug: Optional[str] = None):
        with self._lock:
            slug = streamer_slug.lower().strip() if streamer_slug else None
            if slug:
                for acc in self.comment_history:
                    if slug in self.comment_history[acc]:
                        del self.comment_history[acc][slug]
                logger.info(f"[KickChat] Reset comment history for streamer {slug}")
            else:
                self.comment_history.clear()
                logger.info("[KickChat] Reset all comment history")

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "is_running": self.is_running,
                "current_streamer": self.current_streamer,
                "total_accounts": self.total_accounts,
                "processed_accounts": self.processed_accounts,
                "success_count": self.success_count,
                "fail_count": self.fail_count,
                "current_status": self.current_status,
                "logs": list(self.logs[-50:]),
            }

    def start_campaign(
        self,
        streamer_slug: str,
        comments: List[str],
        interval_seconds: int,
        workers: list,
        selected_accounts: Optional[List[str]] = None
    ) -> Tuple[bool, str]:
        with self._lock:
            if self.is_running:
                return False, "A comment campaign is already running"

            cleaned_comments = [c.strip() for c in comments if c.strip()]
            if not cleaned_comments:
                return False, "Comment list cannot be empty"

            slug = streamer_slug.lower().strip()
            if not slug:
                return False, "Target streamer cannot be empty"

            eligible_workers = []
            for w in workers:
                alias = getattr(w, "alias", None)
                if not alias:
                    continue
                if selected_accounts and alias not in selected_accounts:
                    continue
                token = getattr(w, "token", "")
                if not token:
                    continue
                if slug in self.comment_history.get(alias, {}):
                    continue
                eligible_workers.append(w)

            if not eligible_workers:
                return False, f"No eligible accounts found (they may have already commented on {slug} or have no valid token)"

            self.is_running = True
            self.current_streamer = slug
            self.total_accounts = len(eligible_workers)
            self.processed_accounts = 0
            self.success_count = 0
            self.fail_count = 0
            self.current_status = f"Starting campaign for {slug} with {len(eligible_workers)} accounts..."
            self.logs.clear()

        self._thread = threading.Thread(
            target=self._run_campaign,
            args=(slug, cleaned_comments, max(5, interval_seconds), eligible_workers),
            daemon=True,
            name=f"CommentCampaign-{slug}"
        )
        self._thread.start()
        return True, f"Campaign started for {slug} across {len(eligible_workers)} accounts"

    def _run_campaign(self, slug: str, comments: List[str], interval: int, workers: list):
        logger.info(f"[KickChat] Campaign started for {slug} with {len(workers)} accounts (Interval: {interval}s)")

        chatroom_id = None
        for w in workers:
            token = getattr(w, "token", "")
            proxy = getattr(w, "proxy", None)
            chatroom_id = KickChatClient.get_chatroom_id(slug, token, proxy)
            if chatroom_id:
                break

        if not chatroom_id:
            with self._lock:
                self.is_running = False
                self.current_status = f"Failed to resolve chatroom for {slug}"
                self.logs.append({
                    "time": time.strftime("%H:%M:%S"),
                    "account": "SYSTEM",
                    "comment": "-",
                    "status": f"❌ Could not resolve chatroom ID for streamer {slug}"
                })
            logger.error(f"[KickChat] Could not resolve chatroom ID for {slug}")
            return

        for idx, w in enumerate(workers):
            alias = getattr(w, "alias", f"Account_{idx+1}")
            token = getattr(w, "token", "")
            proxy = getattr(w, "proxy", None)

            comment_text = comments[idx % len(comments)]

            with self._lock:
                self.current_status = f"Account '{alias}' posting comment ({idx+1}/{len(workers)})..."

            success, status_msg = KickChatClient.send_comment(slug, chatroom_id, token, comment_text, proxy)

            with self._lock:
                self.processed_accounts += 1
                if success:
                    self.success_count += 1
                    status_display = f"✅ Sent: \"{comment_text}\""
                else:
                    self.fail_count += 1
                    status_display = f"❌ Failed: {status_msg}"

                if alias not in self.comment_history:
                    self.comment_history[alias] = {}
                self.comment_history[alias][slug] = {
                    "timestamp": time.time(),
                    "comment": comment_text,
                    "success": success,
                    "status": status_msg
                }

                self.logs.append({
                    "time": time.strftime("%H:%M:%S"),
                    "account": alias,
                    "comment": comment_text,
                    "status": status_display
                })

            if idx < len(workers) - 1:
                jitter = random.uniform(-2.0, 2.0)
                actual_sleep = max(3.0, interval + jitter)
                with self._lock:
                    self.current_status = f"Waiting {actual_sleep:.1f}s before next account..."
                time.sleep(actual_sleep)

        with self._lock:
            self.is_running = False
            self.current_status = f"Completed! {self.success_count} sent, {self.fail_count} failed."
            self.logs.append({
                "time": time.strftime("%H:%M:%S"),
                "account": "SYSTEM",
                "comment": "-",
                "status": f"🏁 Campaign finished for {slug}. ({self.success_count}/{len(workers)} succeeded)"
            })
        logger.info(f"[KickChat] Campaign finished for {slug}: {self.success_count} success, {self.fail_count} failed.")

# Singleton instance
comment_manager = CommentCampaignManager()

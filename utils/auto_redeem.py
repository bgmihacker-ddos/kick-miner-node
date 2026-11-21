"""
Auto Redeem Module for Kick Channel Points
Supports automatic redemption with:
1. Instagram handle / Discord / Riot ID / Steam detection
2. UPI / GPay alert safety
3. Target keywords matching (VP, Valorant, Gift Cards, Steam, etc.)
4. Full Kick ULID transaction compliance
"""

import os
import time
import json
import logging
import urllib.request
import urllib.error
import asyncio
from typing import Dict, List, Optional, Any

logger = logging.getLogger("AutoRedeem")

ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def generate_ulid() -> str:
    """Generate 26-character Crockford Base32 ULID."""
    timestamp = int(time.time() * 1000)
    rand_bytes = os.urandom(10)
    time_chars = [''] * 10
    for i in range(9, -1, -1):
        time_chars[i] = ENCODING[timestamp % 32]
        timestamp //= 32
    rand_chars = [''] * 16
    rand_int = int.from_bytes(rand_bytes, 'big')
    for i in range(15, -1, -1):
        rand_chars[i] = ENCODING[rand_int % 32]
        rand_int //= 32
    return ''.join(time_chars) + ''.join(rand_chars)

class KickAutoRedeem:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.enabled = self.config.get("enabled", True)
        self.keywords = [
            k.lower() for k in self.config.get("keywords", [
                "vp", "valorant", "upi", "steam", "gpay",
                "play card", "gift card", "code", "jersey"
            ])
        ]
        self.exclude_keywords = [
            k.lower() for k in self.config.get("exclude_keywords", [
                "hydrate", "dance", "water", "song request",
                "timeout", "emote-only", "posture", "highlight"
            ])
        ]
        self.profiles = self.config.get("profiles", {})
        if not self.profiles:
            self.profiles = {
                "instagram": "_.nand._.patel._",
                "instagram_url": "https://www.instagram.com/_.nand._.patel._",
                "discord": "nandpatel2758",
                "upi": "patelnand@fam",
                "default": "Insta: _.nand._.patel._ | Discord: nandpatel2758"
            }
        self.redeemed_cache: Dict[str, float] = {}
        self.upi_alerted_cache: Dict[str, float] = {}
        self.cooldown_seconds = self.config.get("cooldown_seconds", 43200)

    def is_upi_reward(self, reward: Dict[str, Any]) -> bool:
        """Check if reward asks for or pays out in UPI / GPay."""
        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        prompt = (reward.get("prompt") or "").lower()
        text = f"{title} {desc} {prompt}"
        return any(k in text for k in ["upi", "gpay", "googlepay", "phonepe", "paytm"])

    def build_redemption_message(self, reward: Dict[str, Any]) -> str:
        """
        Inspects the reward's title, description, and prompt.
        Defaults to Instagram and Discord as instructed by the user.
        """
        if not reward.get("is_user_input_required", False):
            return ""

        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        prompt = (reward.get("prompt") or "").lower()
        text = f"{title} {desc} {prompt}"

        insta = self.profiles.get("instagram", "_.nand._.patel._").strip()
        discord = self.profiles.get("discord", "nandpatel2758").strip()
        riot = self.profiles.get("riot_id", "").strip()
        steam = self.profiles.get("steam", "").strip()

        asks_insta = any(k in text for k in ["insta", "instagram", "ig:"])
        asks_discord = any(k in text for k in ["discord", "dc:"])
        asks_steam = any(k in text for k in ["steam", "trade url", "steam id"])
        asks_riot = any(k in text for k in ["riot", "valorant id", "riot id", "tag", "uid", "in game id", "in-game"])

        parts = []
        if asks_insta and insta:
            parts.append(f"Insta: {insta}")
        if asks_discord and discord:
            parts.append(f"Discord: {discord}")
        if asks_steam and steam:
            parts.append(f"Steam: {steam}")
        if asks_riot and riot:
            parts.append(f"Riot ID: {riot}")

        if parts:
            return " | ".join(parts)

        return f"Insta: {insta} | Discord: {discord}"

    def is_target_reward(self, reward: Dict[str, Any]) -> bool:
        """Check if reward matches desired target keywords and is enabled."""
        if not reward.get("is_enabled", True) or reward.get("is_paused", False):
            return False

        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        text = f"{title} {desc}"

        if any(ex in title for ex in self.exclude_keywords):
            return False

        return any(k in text for k in self.keywords)

    def fetch_rewards(self, streamer: str, proxy: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch available channel point rewards for a streamer."""
        url = f"https://kick.com/api/v2/channels/{streamer}/rewards"
        handlers = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        opener = urllib.request.build_opener(*handlers)
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            }
        )
        try:
            with opener.open(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("data", [])
        except Exception as e:
            logger.debug(f"Failed to fetch rewards for {streamer}: {e}")
            return []

    def redeem(
        self,
        streamer: str,
        reward: Dict[str, Any],
        token: str,
        account_alias: str,
        proxy: Optional[str] = None
    ) -> Dict[str, Any]:
        """Attempt to redeem a channel points reward after inspecting prompt & description."""
        reward_id = reward.get("id")
        reward_title = reward.get("title", "Unknown")
        cost = reward.get("cost", 0)
        desc = reward.get("description", "")
        prompt = reward.get("prompt", "")

        cache_key = f"{streamer}:{reward_id}:{account_alias}"
        last_time = self.redeemed_cache.get(cache_key, 0)
        if time.time() - last_time < self.cooldown_seconds:
            return {"status": "cooldown", "message": "Reward on cooldown for this account"}

        if self.is_upi_reward(reward):
            alert_time = self.upi_alerted_cache.get(cache_key, 0)
            if time.time() - alert_time < 3600:
                return {"status": "upi_alerted_recently"}
            self.upi_alerted_cache[cache_key] = time.time()
            logger.warning(
                f"[{account_alias}] 🔔 UPI Reward target reached on {streamer}: "
                f"'{reward_title}' ({cost:,} pts). Dispatched Telegram notification instead of auto-submitting as requested."
            )
            return {
                "status": "upi_ready_alert",
                "reward": reward_title,
                "cost": cost,
                "description": desc,
                "upi_id": self.profiles.get("upi", "patelnand@fam")
            }

        url = f"https://kick.com/api/v2/channels/{streamer}/rewards/{reward_id}/redeem"
        ulid = generate_ulid()
        msg = self.build_redemption_message(reward)

        logger.info(
            f"[{account_alias}] 🔍 Reward description for '{reward_title}':\n"
            f"    Streamer: {streamer}\n"
            f"    Description: \"{desc}\"\n"
            f"    Prompt: \"{prompt}\"\n"
            f"    Input required: {reward.get('is_user_input_required')}\n"
            f"    👉 Submitting: \"{msg}\""
        )

        payload = {
            "transaction_id": ulid,
            "message": msg
        }

        handlers = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        opener = urllib.request.build_opener(*handlers)
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            },
            method="POST"
        )

        try:
            with opener.open(req, timeout=10) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))
                self.redeemed_cache[cache_key] = time.time()
                logger.success(
                    f"[{account_alias}] 🎉 SUCCESS! Auto-redeemed '{reward_title}' ({cost:,} pts) on {streamer}!"
                )
                return {
                    "status": "success",
                    "reward": reward_title,
                    "cost": cost,
                    "message_sent": msg,
                    "description": desc,
                    "data": resp_data
                }
        except urllib.error.HTTPError as e:
            err_msg = ""
            try:
                err_msg = e.read().decode("utf-8")
            except Exception:
                pass
            if e.code == 402:
                return {"status": "insufficient_funds", "message": err_msg}
            logger.warning(
                f"[{account_alias}] ⚠️ Redemption returned HTTP {e.code} for '{reward_title}' on {streamer}: {err_msg}"
            )
            return {"status": "error", "code": e.code, "message": err_msg}
        except Exception as e:
            logger.error(f"[{account_alias}] Redemption error for '{reward_title}' on {streamer}: {e}")
            return {"status": "error", "message": str(e)}

    async def check_and_redeem(
        self,
        streamer: str,
        current_points: int,
        token: str,
        account_alias: str,
        proxy: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Check all rewards for streamer and redeem any eligible target reward."""
        if not self.enabled:
            return []

        rewards = await asyncio.to_thread(self.fetch_rewards, streamer, proxy)
        if not rewards:
            return []

        results = []
        for r in rewards:
            cost = r.get("cost", 0)
            if current_points >= cost and self.is_target_reward(r):
                logger.info(
                    f"[{account_alias}] 🎯 Eligible reward found: '{r.get('title')}' "
                    f"({cost:,} pts, balance: {current_points:,} pts). Processing..."
                )
                res = await asyncio.to_thread(
                    self.redeem,
                    streamer, r, token, account_alias, proxy
                )
                results.append(res)
                if res.get("status") == "success":
                    current_points -= cost
                    if current_points <= 0:
                        break
        return results

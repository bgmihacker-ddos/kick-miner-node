from typing import Dict, List, Optional, Tuple, Any
from curl_cffi import requests
from loguru import logger
import time
import random


class KickRewards:
    """
    Automates Kick Daily Rewards, Crate Claims, and Streak Tracking.
    Targets Kick's gamification endpoints:
      - GET  https://web.kick.com/api/v1/gamification/challenges
      - POST https://web.kick.com/api/v1/gamification/challenges/{challenge_id}/claim
      - GET  https://web.kick.com/api/v1/gamification/users/{user_id}/streak
    """

    def __init__(self, token: str, proxy: Optional[str] = None, alias: str = "Account"):
        self.token = token
        self.proxy = proxy
        self.alias = alias
        self.user_id: Optional[int] = None
        self.username: Optional[str] = None
        self.streak_days: int = 0
        self.last_check_time: float = 0.0
        self.has_pending_challenge: bool = False
        self.watch_progress: int = 0
        self.watch_threshold: int = 60

        proxies = None
        if proxy:
            proxies = {"http": proxy, "https": proxy}

        self.session = requests.Session(
            impersonate="chrome120",
            proxies=proxies,
        )

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization": f"Bearer {self.token}",
            "Origin": "https://kick.com",
            "Referer": "https://kick.com/",
            "x-app-platform": "web",
            "Sec-Ch-Ua": '"Chromium";v="120", "Google Chrome";v="120", "Not?A_Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        })

    def get_user_info(self) -> Tuple[Optional[int], Optional[str]]:
        if self.user_id and self.username:
            return self.user_id, self.username
        try:
            r = self.session.get("https://kick.com/api/v1/user", timeout=12)
            if r.status_code == 200:
                data = r.json()
                self.user_id = data.get("id")
                self.username = data.get("username")
                return self.user_id, self.username
        except Exception as e:
            logger.debug(f"[{self.alias}] Error fetching user profile for rewards: {e}")
        return None, None

    def get_streak(self) -> int:
        uid, _ = self.get_user_info()
        if not uid:
            return 0
        try:
            r = self.session.get(f"https://web.kick.com/api/v1/gamification/users/{uid}/streak", timeout=12)
            if r.status_code == 200:
                self.streak_days = int(r.json().get("data", {}).get("length_days", 0))
                return self.streak_days
        except Exception as e:
            logger.debug(f"[{self.alias}] Error fetching streak: {e}")
        return self.streak_days

    def check_and_claim_rewards(self) -> List[Dict[str, Any]]:
        """
        Checks for available rewards/crates and claims them if claimable.
        Also tracks 1-hour (60-minute) watch-time progress.
        Returns a list of claimed reward records.
        """
        self.last_check_time = time.time()
        claimed_rewards: List[Dict[str, Any]] = []
        self.has_pending_challenge = False

        try:
            r = self.session.get("https://web.kick.com/api/v1/gamification/challenges", timeout=15)
            if r.status_code != 200:
                logger.debug(f"[{self.alias}] Gamification challenges status: {r.status_code}")
                return claimed_rewards

            data = r.json()
            challenges = data.get("data", [])
            for chal in challenges:
                status = chal.get("status")
                cid = chal.get("id")
                name = chal.get("name", "Daily Reward")
                recurrence = chal.get("recurrence", "daily")
                cond = chal.get("condition", {})
                prog = cond.get("progress", 0)
                thresh = cond.get("threshold", 60)

                self.watch_progress = prog
                self.watch_threshold = thresh

                if status == "claimable":
                    logger.info(f"[{self.alias}] 🎁 Found claimable {name} (60m watch achieved!). Claiming now...")
                    claim_res = self.session.post(
                        f"https://web.kick.com/api/v1/gamification/challenges/{cid}/claim",
                        json={},
                        timeout=15,
                    )
                    if claim_res.status_code == 200:
                        claim_data = claim_res.json().get("data", {})
                        winner = claim_data.get("winner", {})
                        item_id = winner.get("id")
                        rarity = winner.get("rarity", "common")

                        streak = self.get_streak()
                        logger.success(
                            f"[{self.alias}] 🎉 Claimed {name}! Rarity: {rarity.upper()} | Current Streak: {streak} days 🔥"
                        )
                        claimed_rewards.append({
                            "name": name,
                            "challenge_id": cid,
                            "recurrence": recurrence,
                            "rarity": rarity,
                            "winner": winner,
                            "streak": streak,
                        })
                    else:
                        logger.warning(
                            f"[{self.alias}] Failed to claim {name}: HTTP {claim_res.status_code} - {claim_res.text[:120]}"
                        )
                elif status == "in_progress":
                    self.has_pending_challenge = True
                    logger.info(
                        f"[{self.alias}] ⏳ {name} in progress: watched {prog}/{thresh} min. Will claim once 1 hour reached."
                    )
                elif status == "claimed":
                    logger.debug(f"[{self.alias}] {name} already claimed for today.")

            # Refresh streak info
            self.get_streak()

        except Exception as e:
            logger.error(f"[{self.alias}] Error in daily rewards claimer: {e}")

        return claimed_rewards

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

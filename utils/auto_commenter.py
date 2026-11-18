import asyncio
import random
import time
import re
from typing import List, Optional, Dict, Any
from loguru import logger
from utils.kick_chat import KickChatClient

# Safe universal comments for background loop (ultra-authentic, varied, human)
SAFE_BACKGROUND_COMMENTS = [
    # Stream Vibe & Quality
    "w stream",
    "chill stream aaj",
    "good vibes here",
    "audio ekdum crisp hai",
    "stream quality clean hai aaj",
    "mast chal raha hai stream",
    "bhai full chill stream",
    "cleaaan stream",
    "video bitrate mast aa raha hai",
    "audio thoda sa badha do bro",
    "frame drop toh nahi ho raha na aaj",
    "stream quality 10/10 hai",
    "clean setup bro",
    
    # Casual viewer greetings & chatter
    "hello bhai",
    "ram ram bhai",
    "kya haal hai bhai",
    "bhai chai pi li kya",
    "aaram se khelo",
    "bhai discord link pinned hai kya",
    "bgmi khelega baadme kya",
    "which game after this bro",
    "which server is this",
    "stream kitne baje tak chalega bhai",
    "are you solo or duo",
    "bhai match mast chal raha hai",
    "sahi khel rahe ho",
    "full focus mode on hai",
    "lets go chat",
    "kya scene chal raha hai",
    "koi na aaram se khelo bro",
    "bhai clutch time",
    "shabaash",
    "badhiya gameplay",
    "bhai chat padho thoda",
    "subah se stream kar rahe ho kya",
    "maza aa raha hai stream dekhne me",
    
    # In-game reactions
    "clean shot",
    "w aim bro",
    "sheeeesh",
    "clutch god",
    "op gameplay",
    "damnn that was clean",
    "crazy flick haha",
    "what a shot",
    "insane reflex bhai",
    "bro is locked in",
    "w gameplay",
    "bhai fire aim",
    "smooth spray control",
    "flick dekho yaar",
    "aim locked in hai aaj",
    "ez kill",
    "bhai god level gameplay",
    "nice spray",
    "kya timing thi",
    
    # Micro neutral chat reactions
    "w",
    "gg",
    "ggs",
    "sheesh",
    "op",
    "hahaha",
    "lol",
    "lmaooo",
    "xd",
    "bruh",
    "arre re",
    "wait what",
    "nice one",
    "pog",
    "hyped",
    "bhai op",
    "nt nt",
    "lets gooo"
]

def humanize_comment(text: str) -> str:
    """
    Transforms any comment into authentic, human live stream chatter:
    - Strips formal trailing periods/semicolons
    - Converts formal casing to natural stream lowercase or hype caps
    - Adds natural human letter elongation (bhai -> bhaiii, op -> oppp)
    - Adds natural gaming emojis/slang quirks
    """
    if not text:
        return ""
    
    t = text.strip().strip('"').strip("'")
    t = re.sub(r'[\.;]+$', '', t).strip()
    
    words = t.split()
    processed_words = []
    for w in words:
        clean_w = re.sub(r'[^\w]', '', w.lower())
        if clean_w in {"w", "gg", "ggs", "op", "lmao", "nt"}:
            processed_words.append(clean_w.upper())
        elif clean_w in {"xd", "kekw"}:
            processed_words.append(clean_w)
        else:
            processed_words.append(w.lower())
    
    t = " ".join(processed_words)
    
    # 25% chance of realistic human letter stretching
    if random.random() < 0.25:
        stretch_map = {
            r'\bbhai\b': 'bhaiii',
            r'\byaar\b': 'yaarrr',
            r'\bsheesh\b': 'sheeeesh',
            r'\bclean\b': 'cleaaan',
            r'\blets go\b': 'lets gooo',
            r'\blol\b': 'lolll',
            r'\blmao\b': 'lmaooo',
            r'\bop\b': 'oppp',
            r'\bnice\b': 'niceee',
            r'\bclutch\b': 'clutchhh',
            r'\barre re\b': 'arre reee'
        }
        for pat, repl in stretch_map.items():
            if re.search(pat, t, re.IGNORECASE) and random.random() < 0.6:
                t = re.sub(pat, repl, t, flags=re.IGNORECASE)
                break

    # 18% chance of subtle emote
    if random.random() < 0.18 and not any(em in t for em in ['🔥', '💀', '😂', '🫡', '❤️']):
        emojis = ['🔥', '💀', '😂', 'gg', 'xd', 'W']
        t = f"{t} {random.choice(emojis)}"
        
    return t


class AutoCommenter:
    """
    Background automated chat commenter with true round-robin account rotation.
    Rotates through all active watching accounts so each account chats in turn.
    Sends safe, natural, humanized comments while the target streamer is live.
    """
    def __init__(self, manager, config: Optional[dict] = None):
        self.manager = manager
        cfg = config or {}
        self.enabled: bool = cfg.get("enabled", True)
        self.target_streamer: str = cfg.get("target_streamer", "xyroplayzz").lower().strip()
        
        # Increased chat rate: default 40s to 80s (average 60s per worker)
        self.min_interval: int = int(cfg.get("min_interval_seconds", 40))
        self.max_interval: int = int(cfg.get("max_interval_seconds", 80))
        
        custom_comments = cfg.get("comments", [])
        self.comments: List[str] = list(custom_comments) if custom_comments else list(SAFE_BACKGROUND_COMMENTS)
        
        self.last_comment_time: float = 0.0
        self.next_interval: float = random.uniform(self.min_interval, self.max_interval)
        
        # True Round-Robin Rotation State
        self._rotation_queue: List[str] = []  # List of account aliases to rotate through
        self.account_comment_counts: Dict[str, int] = {}  # {alias: total_comments_sent}
        self.account_last_comment_time: Dict[str, float] = {}  # {alias: timestamp}
        self.recent_comments: List[str] = []
        
        self.total_sent: int = 0
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._chatroom_id: Optional[int] = 109919156 if self.target_streamer == "xyroplayzz" else None

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._loop())
            logger.info(
                f"[AutoChat] 🚀 Auto-Commenter ACTIVE for '{self.target_streamer}' "
                f"(Rate: {self.min_interval}-{self.max_interval}s, Round-Robin Rotation: ENABLED, Pool: {len(self.comments)} msgs)"
            )

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[AutoChat] 🛑 Auto-Commenter stopped")

    def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "target_streamer": self.target_streamer,
            "min_interval": self.min_interval,
            "max_interval": self.max_interval,
            "total_sent": self.total_sent,
            "pool_size": len(self.comments),
            "rotation_queue_remaining": len(self._rotation_queue),
            "unique_accounts_chatted": len(self.account_comment_counts),
            "next_in_seconds": max(0, int(self.last_comment_time + self.next_interval - time.time())) if self.last_comment_time > 0 else int(self.next_interval)
        }

    def _get_next_rotating_worker(self, active_workers: list):
        """
        Picks the next worker in true round-robin fashion.
        When all available workers have commented, reshuffles and starts the next round.
        """
        active_map = {w.alias: w for w in active_workers}
        active_aliases = list(active_map.keys())

        # Clean rotation queue: keep only currently active watching accounts
        self._rotation_queue = [a for a in self._rotation_queue if a in active_map]

        # If queue is empty, refill with all active workers sorted/shuffled
        if not self._rotation_queue:
            # Sort by comment count ascending (accounts that commented least go first), then shuffle within tiers
            shuffled = list(active_aliases)
            # Sort by least recently used
            shuffled.sort(key=lambda a: (self.account_comment_counts.get(a, 0), self.account_last_comment_time.get(a, 0)))
            self._rotation_queue = shuffled
            logger.info(f"[AutoChat] 🔄 Rotation cycle refilled with {len(self._rotation_queue)} accounts.")

        # Pop next account
        while self._rotation_queue:
            next_alias = self._rotation_queue.pop(0)
            worker = active_map.get(next_alias)
            if worker:
                return worker

        # Fallback to random if queue empty
        return random.choice(active_workers)

    async def _loop(self):
        # Quick initial warmup delay (30-60s) so accounts establish connection
        await asyncio.sleep(random.uniform(30, 60))
        self.last_comment_time = time.time()
        self.next_interval = random.uniform(self.min_interval, self.max_interval)

        while self._running:
            try:
                await asyncio.sleep(5)
                if not self.enabled:
                    continue

                now = time.time()
                if (now - self.last_comment_time) < self.next_interval:
                    continue

                target = self.target_streamer

                # Find all workers actively watching the target streamer
                active_workers = []
                for w in self.manager.workers:
                    st = w.state.streamers.get(target)
                    if st and st.is_watching and st.is_online:
                        active_workers.append(w)

                if not active_workers:
                    continue

                # Select next worker using fair round-robin rotation
                chosen_worker = self._get_next_rotating_worker(active_workers)

                # Pick comment avoiding recent duplicates
                available_comments = [
                    c for c in self.comments if c not in self.recent_comments[-12:]
                ]
                if not available_comments:
                    available_comments = self.comments

                chosen_comment = humanize_comment(random.choice(available_comments))

                # Resolve chatroom ID
                if not self._chatroom_id:
                    if target == "xyroplayzz":
                        self._chatroom_id = 109919156
                    else:
                        self._chatroom_id = await asyncio.to_thread(
                            KickChatClient.get_chatroom_id,
                            target,
                            chosen_worker.token,
                            chosen_worker.proxy
                        )

                if not self._chatroom_id:
                    logger.warning(f"[AutoChat] Could not resolve chatroom ID for {target}")
                    continue

                ok, res = await asyncio.to_thread(
                    KickChatClient.send_comment,
                    target,
                    self._chatroom_id,
                    chosen_worker.token,
                    chosen_comment,
                    chosen_worker.proxy
                )

                if ok:
                    self.total_sent += 1
                    self.account_comment_counts[chosen_worker.alias] = self.account_comment_counts.get(chosen_worker.alias, 0) + 1
                    self.account_last_comment_time[chosen_worker.alias] = time.time()
                    self.recent_comments.append(chosen_comment)
                    if len(self.recent_comments) > 20:
                        self.recent_comments.pop(0)

                    self.last_comment_time = time.time()
                    self.next_interval = random.uniform(self.min_interval, self.max_interval)
                    logger.success(
                        f"[AutoChat] 💬 [{chosen_worker.alias}] (#{self.account_comment_counts[chosen_worker.alias]}) -> {target}: "
                        f"\"{chosen_comment}\" (Next in {self.next_interval:.0f}s | Queue left: {len(self._rotation_queue)})"
                    )
                else:
                    logger.warning(f"[AutoChat] Failed to send comment from {chosen_worker.alias}: {res}")
                    # If failed, retry sooner with next account
                    self.last_comment_time = time.time() - (self.next_interval - 15)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[AutoChat] Loop exception: {e}")
                await asyncio.sleep(10)

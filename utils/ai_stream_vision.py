import asyncio
import base64
import json
import os
import random
import re
import subprocess
import time
import urllib.request
from typing import Optional, Dict, Any, List
from loguru import logger
from utils.kick_chat import KickChatClient
from curl_cffi import requests

DEFAULT_PERSONAS = {
    "aarav_patel91": "desi",
    "rohit_verma_88": "aim_analyst",
    "karan_m92": "friendly_troll",
    "dev_sharmaa7": "desi",
    "neil_kapoor23": "hype_beast",
    "tyler_brooks88": "hype_beast",
    "jordan_miller_9": "aim_analyst",
    "priya_nair_95": "setup_inquisitor",
    "ankit_raj_07": "desi",
    "mason_reed94": "hype_beast",
    "austin_blake94": "aim_analyst",
    "vikram_singh92": "desi",
    "sam_walker_88": "friendly_troll",
    "shubham_joshi7": "desi",
    "cole_henderson8": "hype_beast",
    "aditya_gupta_91": "desi",
    "chase_morgan_9": "aim_analyst"
}

def humanize_comment(text: str) -> str:
    """
    Transforms any comment into ultra-authentic, human live stream chatter:
    - Strips formal trailing periods/semicolons
    - Converts formal casing to natural stream lowercase or hype caps
    - Adds natural human letter elongation (bhai -> bhaiii, op -> oppp)
    - Adds natural gaming emojis/slang quirks
    """
    if not text:
        return ""
    
    t = text.strip().strip('"').strip("'")
    
    # Strip formal ending punctuation (periods, semicolons, trailing dashes)
    t = re.sub(r'[\.;]+$', '', t).strip()
    
    # Lowercase unless it is recognized pure hype slang
    hype_tokens = {"w", "gg", "ggs", "op", "lmao", "kekw", "nt", "xd", "l", "sheesh", "f"}
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
            r'\bare re\b': 'arre reee'
        }
        for pat, repl in stretch_map.items():
            if re.search(pat, t, re.IGNORECASE) and random.random() < 0.6:
                t = re.sub(pat, repl, t, flags=re.IGNORECASE)
                break

    # 18% chance of natural gaming emoji / emote
    if random.random() < 0.18 and not any(em in t for em in ['🔥', '💀', '😂', '🫡', '❤️']):
        emojis = ['🔥', '💀', '😂', 'gg', 'xd', 'W']
        t = f"{t} {random.choice(emojis)}"
        
    return t.strip()

VISION_PROMPT = """You are a regular 19-year-old Indian gamer hanging out in a live Kick stream chat watching your favorite Indian streamer.
You type fast in chat on mobile or PC: short, casual, authentic Hinglish / English slang.

Analyze this single live screenshot from the stream.

Answer in valid JSON with this exact schema:
{
  "is_in_game": true or false,
  "game_name": "Game name or 'Lobby/Menu' or 'Unknown'",
  "event_type": "kill" or "death" or "clutch" or "gameplay" or "lobby" or "victory" or "defeat",
  "summary": "Brief 1-sentence description of what is visible on screen",
  "reactions": {
    "desi": "1-5 words in casual Indian gamer slang / Hinglish (e.g. 'bhaiii kya mara', 'arre yaar camp kar raha tha', 'op clutch bhai', 'kya aim hai re', 'arre re unlucky tha')",
    "hype_beast": "1-4 words pure hype (e.g. 'CLEAN SHOT 🔥', 'SHEEEESH', 'W AIM BRO', 'cleaaan', 'bhai god level')",
    "aim_analyst": "2-5 words casual gamer observation (e.g. 'smooth spray bro', 'aim locked in hai aaj', 'flick dekho yaar', 'clean recoil control', 'insane reflex')",
    "friendly_troll": "2-5 words playful banter (e.g. 'bro is smurfing haha', 'bot lobby kya lol', 'hahaha kya tha ye', 'bruhh no way', 'lmao unlucky')",
    "setup_inquisitor": "2-5 words casual streamer questions (e.g. 'sens kya hai bhai?', 'konsa rank chal raha?', 'ping kitna aa raha hai?', 'bhai bgmi kab khelega?')"
  }
}

STRICT CHAT RULES:
1. NEVER sound like an esports journalist, commentator, or robot (NEVER say formal robotic phrases like 'tactical crosshair placement' or 'sens looks dialed in').
2. NO trailing periods. Write in all lowercase or natural chat caps.
3. If streamer died, lost, or spectates: MUST be condolences (e.g. 'nt nt', 'unlucky timing yaar', 'koi na next round', 'so close bro', 'peeche se mar diya'). NEVER praise aim when dead!
4. If streamer got a kill, wipe, or clutch: MUST be hype (e.g. 'cleaaan', 'op bhaiii', 'sheeeesh', 'W', 'kya shot tha').
5. Keep every comment short: between 1 and 5 words maximum. Real chatters type short bursts.
6. Return ONLY the raw JSON object, no markdown backticks or extra commentary."""

class AIStreamVision:
    """
    Multimodal AI Stream Analyzer for Kick using Gemini Flash.
    Periodically snapshots live stream via HLS feed, analyzes game state,
    and dispatches persona-aligned chat reactions through residential proxies.
    """
    def __init__(self, manager, config: Optional[dict] = None):
        self.manager = manager
        cfg = config or {}
        self.enabled: bool = cfg.get("enabled", True)
        self.api_key: str = cfg.get("api_key", "")
        self.model: str = cfg.get("model", "gemini-flash-lite-latest")
        self.target_streamer: str = cfg.get("target_streamer", "xyroplayzz").lower().strip()
        self.check_interval: int = int(cfg.get("check_interval_seconds", 35))
        self.min_cooldown: int = int(cfg.get("min_comment_cooldown_seconds", 90))
        self.max_cooldown: int = int(cfg.get("max_comment_cooldown_seconds", 180))
        self.notify_telegram: bool = cfg.get("notify_telegram", True)
        self.personas: Dict[str, str] = cfg.get("account_personas", DEFAULT_PERSONAS)

        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self.last_comment_time: float = 0.0
        self.next_comment_cooldown: float = random.uniform(self.min_cooldown, self.max_cooldown)
        self.last_detected_state: str = "IDLE"
        self.last_detected_game: str = "Unknown"
        self.last_summary: str = "Waiting for live stream..."
        self.last_comment_sent: str = ""
        self.total_analyses: int = 0
        self.total_ai_comments: int = 0
        self.recent_commenters: List[str] = []
        self._playback_url: Optional[str] = None
        self._playback_url_time: float = 0.0
        self._chatroom_id: Optional[int] = 109919156 if self.target_streamer == "xyroplayzz" else None
        
        # Temp frame path
        self._frame_path: str = "/tmp/xyro_vision_frame.jpg" if os.name != "nt" else os.path.join(os.path.expanduser("~"), "xyro_vision_frame.jpg")

    def start(self):
        if not self._running and self.enabled:
            self._running = True
            self._task = asyncio.create_task(self._main_loop())
            logger.info(
                f"[AIStreamVision] 🚀 Multimodal AI Vision Agent started for '{self.target_streamer}' "
                f"(Model: {self.model}, Check Interval: {self.check_interval}s)"
            )

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[AIStreamVision] 🛑 Multimodal AI Vision Agent stopped")

    def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "target": self.target_streamer,
            "game": self.last_detected_game,
            "state": self.last_detected_state,
            "summary": self.last_summary,
            "last_comment": self.last_comment_sent,
            "total_analyses": self.total_analyses,
            "total_ai_comments": self.total_ai_comments,
            "next_in_seconds": max(0, int(self.last_comment_time + self.next_comment_cooldown - time.time())) if self.last_comment_time > 0 else 0
        }

    def _get_playback_url(self) -> Optional[str]:
        """Fetches live AWS IVS HLS playback URL from Kick channel API."""
        now = time.time()
        if self._playback_url and (now - self._playback_url_time < 300):
            return self._playback_url

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }
        try:
            with requests.Session(impersonate="chrome120") as s:
                resp = s.get(f"https://kick.com/api/v2/channels/{self.target_streamer}", headers=headers, timeout=12)
                if resp.status_code == 200:
                    data = resp.json()
                    pb = data.get("playback_url")
                    livestream = data.get("livestream")
                    if pb and livestream:
                        self._playback_url = pb
                        self._playback_url_time = now
                        return pb
        except Exception as e:
            logger.debug(f"[AIStreamVision] Failed to fetch playback_url: {e}")
        return None

    def _capture_frame(self, stream_url: str) -> bool:
        """Captures 1 lightweight 640x360 frame using ffmpeg."""
        cmd = [
            "ffmpeg", "-y",
            "-headers", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36\r\nReferer: https://kick.com/\r\nOrigin: https://kick.com\r\n",
            "-i", stream_url,
            "-frames:v", "1",
            "-vf", "scale=640:360",
            self._frame_path
        ]
        try:
            res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
            ok = res.returncode == 0 and os.path.exists(self._frame_path) and os.path.getsize(self._frame_path) > 1000
            if not ok:
                logger.warning(f"[AIStreamVision] ffmpeg return code: {res.returncode}, exists: {os.path.exists(self._frame_path)}")
            return ok
        except Exception as e:
            logger.warning(f"[AIStreamVision] Frame capture error: {e}")
            return False

    def _call_gemini_vision(self, img_path: str) -> Optional[Dict[str, Any]]:
        """Sends frame to Gemini API and parses structured gameplay JSON."""
        try:
            with open(img_path, "rb") as f:
                b64_data = base64.b64encode(f.read()).decode("utf-8")

            payload = {
                "contents": [{
                    "parts": [
                        {"text": VISION_PROMPT},
                        {
                            "inlineData": {
                                "mimeType": "image/jpeg",
                                "data": b64_data
                            }
                        }
                    ]
                }]
            }

            models = [self.model, "gemini-flash-lite-latest", "gemini-3.8-flash"]
            for m in models:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={self.api_key}"
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                try:
                    with urllib.request.urlopen(req, timeout=25) as resp:
                        res = json.loads(resp.read().decode("utf-8"))
                        raw_text = res["candidates"][0]["content"]["parts"][0]["text"].strip()
                        # Clean markdown json blocks if wrapped
                        if raw_text.startswith("```json"):
                            raw_text = raw_text[7:]
                        if raw_text.startswith("```"):
                            raw_text = raw_text[3:]
                        if raw_text.endswith("```"):
                            raw_text = raw_text[:-3]
                        return json.loads(raw_text.strip())
                except Exception as ex_m:
                    logger.warning(f"[AIStreamVision] Gemini API error on {m}: {ex_m}")
                    continue
        except Exception as e:
            logger.warning(f"[AIStreamVision] Gemini Vision call failed: {e}")
        return None

    async def _main_loop(self):
        """Main analysis and commenting loop."""
        # Initial wait for stream check
        await asyncio.sleep(15)
        logger.info("[AIStreamVision] Loop active, waiting for stream signal...")

        while self._running:
            try:
                await asyncio.sleep(self.check_interval)
                if not self.enabled:
                    continue

                # 1. Check if streamer is online across workers or direct API
                is_live = False
                for w in self.manager.workers:
                    st = w.state.streamers.get(self.target_streamer)
                    if st and st.is_online:
                        is_live = True
                        break

                pb_url = await asyncio.to_thread(self._get_playback_url)
                if pb_url:
                    is_live = True

                if not is_live or not pb_url:
                    continue

                # 3. Capture frame
                logger.info(f"[AIStreamVision] 📸 Capturing live frame from {self.target_streamer}...")
                captured = await asyncio.to_thread(self._capture_frame, pb_url)
                if not captured:
                    continue

                # 4. Analyze with Gemini Multimodal Vision
                analysis = await asyncio.to_thread(self._call_gemini_vision, self._frame_path)
                if not analysis:
                    logger.warning("[AIStreamVision] Gemini analysis returned empty or failed to parse.")
                    continue

                self.total_analyses += 1
                self.last_detected_game = analysis.get("game_name", self.last_detected_game)
                self.last_detected_state = analysis.get("event_type", "gameplay").upper()
                self.last_summary = analysis.get("summary", "")
                reactions = analysis.get("reactions", {})

                logger.info(
                    f"[AIStreamVision] 👁️ Detected: [{self.last_detected_game}] - {self.last_detected_state} "
                    f"(\"{self.last_summary}\")"
                )

                # 5. Check cooldown before sending comment
                now = time.time()
                time_since_last = now - self.last_comment_time
                if time_since_last < self.next_comment_cooldown:
                    continue

                if not reactions:
                    continue

                # 6. Pick matching persona & account
                available_workers = [
                    w for w in self.manager.workers
                    if getattr(w, "token_valid", True) and w.alias not in self.recent_commenters[-4:]
                ]
                if not available_workers:
                    available_workers = [w for w in self.manager.workers if getattr(w, "token_valid", True)]

                if not available_workers:
                    continue

                chosen_worker = random.choice(available_workers)
                persona = self.personas.get(chosen_worker.alias, "desi")
                chosen_comment = reactions.get(persona) or reactions.get("desi") or list(reactions.values())[0]
                clean_comment = humanize_comment(str(chosen_comment))

                # 7. Resolve chatroom ID
                if not self._chatroom_id:
                    self._chatroom_id = 109919156 if self.target_streamer == "xyroplayzz" else await asyncio.to_thread(
                        KickChatClient.get_chatroom_id,
                        self.target_streamer,
                        chosen_worker.token,
                        chosen_worker.proxy
                    )

                if not self._chatroom_id:
                    continue

                # 8. Send comment
                ok, res_msg = await asyncio.to_thread(
                    KickChatClient.send_comment,
                    self.target_streamer,
                    self._chatroom_id,
                    chosen_worker.token,
                    clean_comment,
                    chosen_worker.proxy
                )

                if ok:
                    self.total_ai_comments += 1
                    self.last_comment_time = now
                    self.next_comment_cooldown = random.uniform(self.min_cooldown, self.max_cooldown)
                    self.recent_commenters.append(chosen_worker.alias)
                    self.last_comment_sent = f"[{chosen_worker.alias}] ({persona}): \"{clean_comment}\""

                    logger.success(
                        f"[AIStreamVision] 💬 [{chosen_worker.alias}] ({persona}) -> {self.target_streamer}: "
                        f"\"{clean_comment}\" (Next AI comment in {self.next_comment_cooldown:.0f}s)"
                    )

                    # Optional Telegram notification
                    if self.notify_telegram and hasattr(self.manager, "_telegram") and self.manager._telegram:
                        try:
                            tg_msg = (
                                f"👁️ <b>AI Stream Vision Reaction!</b>\n\n"
                                f"🎮 <b>Game:</b> {self.last_detected_game}\n"
                                f"⚡ <b>Event:</b> {self.last_detected_state}\n"
                                f"📝 <b>Vision Context:</b> <i>\"{self.last_summary}\"</i>\n\n"
                                f"👤 <b>Account:</b> <code>{chosen_worker.alias}</code> ({persona})\n"
                                f"💬 <b>Comment Sent:</b> <i>\"{clean_comment}\"</i>\n"
                                f"⏱ Next check in ~{int(self.next_comment_cooldown)}s"
                            )
                            asyncio.create_task(self.manager._telegram._send(tg_msg))
                        except Exception:
                            pass
                else:
                    logger.warning(f"[AIStreamVision] Failed sending AI comment from {chosen_worker.alias}: {res_msg}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[AIStreamVision] Loop exception: {e}")
                await asyncio.sleep(20)

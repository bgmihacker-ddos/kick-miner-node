import asyncio
import os
import csv
import json
import random
import time
import html
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from loguru import logger

from _websockets.ws_token import KickPoints
from _websockets.ws_connect import KickWebSocket
from utils.kick_utility import KickUtility
from utils.get_points_amount import PointsAmount
from utils.kick_prediction import KickPrediction
from utils.auto_redeem import KickAutoRedeem

if TYPE_CHECKING:
    from discord_webhook import DiscordWebhook


def clean_token(token: str) -> str:
    """Automatic token checker and cleaner: converts %7C to | and unquotes."""
    if not token:
        return ""
    t = str(token).strip().strip("'\"")
    t = t.replace("%7C", "|").replace("%7c", "|")
    t = urllib.parse.unquote(t).strip().strip("'\"")
    return t


def log_bet_to_csv(account_alias: str, streamer: str, title: str, outcome: str, odds: float, amount: int, result: str, payout: int, net_profit: int, new_balance: int, is_consensus: bool = True, is_whale: bool = False):
    try:
        csv_file = "bets_history.csv"
        file_exists = os.path.exists(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "Account", "Streamer", "Prediction", "Outcome", "Odds", "BetAmount", "Result", "Payout", "NetProfit", "NewBalance"])
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                account_alias,
                streamer,
                title,
                outcome,
                f"{odds:.2f}x",
                amount,
                result,
                payout,
                net_profit,
                new_balance
            ])
    except Exception as e:
        logger.debug(f"CSV log error: {e}")

    # Self-Learning Engine update
    try:
        from utils.adaptive_learning import AdaptiveLearningEngine
        AdaptiveLearningEngine().record_bet_result(
            streamer=streamer,
            is_consensus=is_consensus,
            did_win=bool(result == "WIN"),
            bet_amount=amount,
            net_profit=net_profit,
            odds=odds,
            is_whale=is_whale,
            prediction_title=title
        )
    except Exception as e:
        logger.debug(f"Adaptive learning update error: {e}")

    # Trigger GitHub Auto-Sync if active
    try:
        sync_inst = getattr(log_bet_to_csv, "_sync_instance", None)
        if sync_inst:
            sync_inst.sync_now()
    except Exception:
        pass


SESSION_STATE_FILE = "session_state.json"


def _load_session_state_file() -> dict:
    if os.path.exists(SESSION_STATE_FILE):
        try:
            with open(SESSION_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.debug(f"Could not load {SESSION_STATE_FILE}: {e}")
    return {}


def _save_session_state_file(data: dict):
    try:
        with open(SESSION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.debug(f"Could not save {SESSION_STATE_FILE}: {e}")


def record_consensus_outcome_to_session(streamer: str, did_consensus_win: bool):
    try:
        saved = _load_session_state_file()
        if "streamer_profiles" not in saved:
            saved["streamer_profiles"] = {}
        prof = saved["streamer_profiles"].get(streamer.lower(), {
            "consensus_wins": 0,
            "consensus_losses": 0,
            "total_tracked": 0
        })
        if did_consensus_win:
            prof["consensus_wins"] += 1
        else:
            prof["consensus_losses"] += 1
        prof["total_tracked"] += 1
        saved["streamer_profiles"][streamer.lower()] = prof
        _save_session_state_file(saved)
    except Exception as e:
        logger.debug(f"Error updating streamer profile: {e}")


def get_streamer_profile_from_session(streamer: str) -> dict:
    try:
        saved = _load_session_state_file()
        return saved.get("streamer_profiles", {}).get(streamer.lower(), {
            "consensus_wins": 0,
            "consensus_losses": 0,
            "total_tracked": 0
        })
    except Exception:
        return {"consensus_wins": 0, "consensus_losses": 0, "total_tracked": 0}


@dataclass
class StreamerState:
    name: str
    priority: int

    is_online: bool = False
    is_watching: bool = False
    points: int = 0
    session_start_points: Optional[int] = None
    last_points_update: Optional[datetime] = None
    stream_id: Optional[int] = None
    channel_id: Optional[int] = None

    ws_client: Optional[KickWebSocket] = None
    ws_task: Optional[asyncio.Task] = None
    points_task: Optional[asyncio.Task] = None
    prediction_task: Optional[asyncio.Task] = None
    active_prediction: Optional[dict] = None
    pending_bet: Optional[dict] = None

    consecutive_losses: int = 0
    consecutive_wins: int = 0
    last_bet_result: Optional[str] = None

    total_predictions_seen: int = 0
    last_prediction_time: Optional[datetime] = None
    goal_completed: bool = False

    last_error: Optional[str] = None
    error_count: int = 0


@dataclass
class AccountState:
    alias: str
    token: str
    proxy: Optional[str]
    max_concurrent: int
    streamers: Dict[str, StreamerState] = field(default_factory=dict)
    streamer_order: List[str] = field(default_factory=list)

    @property
    def active_count(self) -> int:
        return sum(1 for s in self.streamers.values() if s.is_watching)

    @property
    def active_names(self) -> List[str]:
        return [s.name for s in self.streamers.values() if s.is_watching]


class AccountWorker:
    def __init__(
        self,
        account_cfg: dict,
        global_proxy: Optional[str] = None,
        check_interval: int = 600,
        reconnect_cooldown: int = 600,
        stagger_min: float = 3.0,
        stagger_max: float = 8.0,
        manager: Optional["AccountManager"] = None,
        prediction_cfg: Optional[dict] = None,
        worker_index: int = 0,
        total_workers: int = 1,
    ):
        self.alias = account_cfg["alias"]
        self.worker_index = worker_index
        self.total_workers = total_workers
        self.token = clean_token(account_cfg.get("token", ""))
        self.proxy = account_cfg.get("proxy") or global_proxy
        self.max_concurrent = account_cfg.get("max_concurrent", 2)
        self.check_interval = check_interval
        self.reconnect_cooldown = reconnect_cooldown
        self.stagger_min = stagger_min
        self.stagger_max = stagger_max
        self.manager = manager

        pred_cfg = account_cfg.get("predictions") or prediction_cfg or {}
        self.predictions_enabled = pred_cfg.get("enabled", True)
        self.prediction_strategy = pred_cfg.get("strategy", "minimal_risk")
        self.aggressive_threshold = int(pred_cfg.get("aggressive_threshold", pred_cfg.get("risk_threshold", 15000)))
        self.moderate_threshold = int(pred_cfg.get("moderate_threshold", 30000))
        self.risk_threshold = self.aggressive_threshold

        self.aggressive_stage1_pct = int(pred_cfg.get("aggressive_stage1_percentage", pred_cfg.get("growth_stage1_percentage", 50)))
        self.aggressive_stage2_pct = int(pred_cfg.get("aggressive_stage2_percentage", pred_cfg.get("growth_stage2_percentage", 35)))
        self.aggressive_stage3_pct = int(pred_cfg.get("aggressive_stage3_percentage", pred_cfg.get("growth_stage3_percentage", 20)))

        self.moderate_stage1_pct = int(pred_cfg.get("moderate_stage1_percentage", 25))
        self.moderate_stage2_pct = int(pred_cfg.get("moderate_stage2_percentage", 18))
        self.moderate_stage3_pct = int(pred_cfg.get("moderate_stage3_percentage", 12))

        self.safe_stage1_pct = int(pred_cfg.get("safe_stage1_percentage", 12))
        self.safe_stage2_pct = int(pred_cfg.get("safe_stage2_percentage", 8))
        self.safe_stage3_pct = int(pred_cfg.get("safe_stage3_percentage", 5))

        self.growth_stage1_pct = self.aggressive_stage1_pct
        self.growth_stage2_pct = self.aggressive_stage2_pct
        self.growth_stage3_pct = self.aggressive_stage3_pct

        self.stage1_percentage = int(pred_cfg.get("stage1_percentage", self.growth_stage1_pct))
        self.stage2_percentage = int(pred_cfg.get("stage2_percentage", self.growth_stage2_pct))
        self.stage3_percentage = int(pred_cfg.get("stage3_percentage", self.growth_stage3_pct))
        self.prediction_min_odds_stage1 = float(pred_cfg.get("min_odds_stage1", 1.2))
        self.prediction_bet_percentage = self.stage2_percentage
        self.prediction_max_bet_cap = int(pred_cfg.get("max_bet_cap", 10000))
        self.prediction_seconds_before_lock = pred_cfg.get("seconds_before_lock", 5)
        self.prediction_check_interval = pred_cfg.get("check_interval", 10)
        self.prediction_min_bet = pred_cfg.get("min_bet", 10)
        self.prediction_stop_targets = {
            k.lower(): int(v) for k, v in pred_cfg.get("stop_targets", {}).items()
        }

        # Advanced Progression & Multiplier settings
        self.loss_recovery_cfg = pred_cfg.get("loss_recovery", {})
        self.loss_recovery_enabled = bool(self.loss_recovery_cfg.get("enabled", False))
        self.loss_recovery_mode = str(self.loss_recovery_cfg.get("mode", "percentage_steps"))
        self.loss_recovery_multiplier = float(self.loss_recovery_cfg.get("multiplier", 2.0))
        self.loss_recovery_steps = self.loss_recovery_cfg.get("percentage_steps", [25, 50, 85])
        self.loss_recovery_max_streak = int(self.loss_recovery_cfg.get("max_streak", 3))

        self.win_streak_cfg = pred_cfg.get("win_streak_compounding", {})
        self.win_streak_enabled = bool(self.win_streak_cfg.get("enabled", False))
        self.win_streak_steps = self.win_streak_cfg.get("percentage_steps", [15, 30, 60])
        self.win_streak_multiplier = float(self.win_streak_cfg.get("multiplier", 1.5))
        self.win_streak_max_streak = int(self.win_streak_cfg.get("max_streak", 3))

        self.kelly_cfg = pred_cfg.get("kelly_criterion", {})
        self.kelly_fraction = float(self.kelly_cfg.get("fraction", 0.75))

        self.anti_whale_cfg = pred_cfg.get("anti_whale", {})
        self.anti_whale_min_consensus = float(self.anti_whale_cfg.get("min_voter_consensus", 55.0))
        self.anti_whale_min_odds = float(self.anti_whale_cfg.get("min_odds", 1.35))

        # Bootstrap / Low-Balance Turbo Accelerator (Feature 2)
        self.bootstrap_turbo_cfg = pred_cfg.get("bootstrap_turbo", {})
        self.bootstrap_turbo_enabled = bool(self.bootstrap_turbo_cfg.get("enabled", True))
        self.bootstrap_turbo_threshold = int(self.bootstrap_turbo_cfg.get("threshold", 3000))
        self.bootstrap_turbo_pct = int(self.bootstrap_turbo_cfg.get("percentage", 90))

        # Prediction Activity Dynamic Prioritization (Feature 3)
        self.prioritize_active_predictions = bool(pred_cfg.get("prioritize_active_predictions", True))

        # Smart Odds Safety Floor (Feature 4)
        self.min_odds_floor = float(pred_cfg.get("min_odds_floor", 1.20))

        # Multi-Account Hedging & Speedrun to 500k Configurations
        self.hedging_cfg = pred_cfg.get("hedging", {})
        self.hedging_enabled = bool(self.hedging_cfg.get("enabled", True))
        self.prediction_default_stop_target = int(pred_cfg.get("default_stop_target", 100000))
        self.pool_cap_percentage = float(pred_cfg.get("max_pool_share", 0.25))
        self.min_voters_threshold = int(pred_cfg.get("min_voters_threshold", 8))

        self._prediction_client: Optional[KickPrediction] = None
        self._voted_predictions: set = set()

        raw_streamers = account_cfg.get("streamers")
        if not raw_streamers and self.manager:
            raw_streamers = self.manager.common_streamers
        streamer_names: List[str] = [
            str(s).strip().lower() for s in (raw_streamers or []) if str(s).strip()
        ]

        self.state = AccountState(
            alias=self.alias,
            token=self.token,
            proxy=self.proxy,
            max_concurrent=self.max_concurrent,
            streamer_order=streamer_names,
        )
        for idx, name in enumerate(streamer_names):
            self.state.streamers[name] = StreamerState(
                name=name, priority=idx
            )
        self._restore_session_state()

        self._utility_cache: Dict[str, KickUtility] = {}
        self._points_checker: Optional[PointsAmount] = None
        self._ws_token_getter: Optional[KickPoints] = None
        auto_redeem_cfg = account_cfg.get("auto_redeem") or (manager.auto_redeem_cfg if manager else {}) or {}
        self.auto_redeem = KickAutoRedeem(auto_redeem_cfg)
        self._discord: Optional["DiscordWebhook"] = None
        self._telegram = None

        self._rebalance_lock = asyncio.Lock()
        self._running = False

    def _restore_session_state(self):
        saved = _load_session_state_file()
        acc_saved = saved.get(self.alias, {})
        for name, st in self.state.streamers.items():
            if name in acc_saved:
                s_data = acc_saved[name]
                st.consecutive_wins = int(s_data.get("consecutive_wins", 0))
                st.consecutive_losses = int(s_data.get("consecutive_losses", 0))
                st.last_bet_result = s_data.get("last_bet_result", "")
                st.goal_completed = bool(s_data.get("goal_completed", False))
                st.total_predictions_seen = int(s_data.get("total_predictions_seen", 0))
                pb_saved = s_data.get("pending_bet")
                if pb_saved and not pb_saved.get("resolved", False):
                    st.pending_bet = pb_saved
                if st.goal_completed:
                    logger.info(f"[{self.alias}] 🎯 Restored completed goal state for {name} from {SESSION_STATE_FILE}")

    def _persist_streamer_state(self, name: str):
        st = self.state.streamers.get(name)
        if not st:
            return
        saved = _load_session_state_file()
        if self.alias not in saved:
            saved[self.alias] = {}
        pb = getattr(st, "pending_bet", None)
        pb_data = None
        if pb and not pb.get("resolved", False):
            placed_at = pb.get("placed_at")
            pb_data = {
                "prediction_id": pb.get("prediction_id"),
                "outcome_id": pb.get("outcome_id"),
                "outcome_title": pb.get("outcome_title"),
                "title": pb.get("title"),
                "amount": pb.get("amount"),
                "multiplier": pb.get("multiplier"),
                "pre_wallet": pb.get("pre_wallet"),
                "placed_at": placed_at.isoformat() if isinstance(placed_at, datetime) else str(placed_at or ""),
                "resolved": False,
                "is_consensus": pb.get("is_consensus", True),
                "is_whale": pb.get("is_whale", False),
            }
        saved[self.alias][name] = {
            "consecutive_wins": st.consecutive_wins,
            "consecutive_losses": st.consecutive_losses,
            "last_bet_result": st.last_bet_result,
            "goal_completed": st.goal_completed,
            "total_predictions_seen": st.total_predictions_seen,
            "points": st.points,
            "pending_bet": pb_data,
            "updated_at": datetime.now().isoformat(),
        }
        _save_session_state_file(saved)

    def set_discord(self, discord: "DiscordWebhook"):
        self._discord = discord

    def set_telegram(self, telegram):
        self._telegram = telegram

    def _get_utility(self, streamer: str) -> KickUtility:
        if streamer not in self._utility_cache:
            self._utility_cache[streamer] = KickUtility(
                streamer, proxy=self.proxy
            )
        return self._utility_cache[streamer]

    def _get_points_checker(self) -> PointsAmount:
        if self._points_checker is None:
            self._points_checker = PointsAmount(proxy=self.proxy)
        return self._points_checker

    def _get_ws_token_getter(self) -> KickPoints:
        if self._ws_token_getter is None:
            self._ws_token_getter = KickPoints(
                self.token, proxy=self.proxy
            )
        return self._ws_token_getter

    def _get_prediction_client(self) -> KickPrediction:
        if self._prediction_client is None:
            self._prediction_client = KickPrediction(proxy=self.proxy)
        return self._prediction_client

    async def start(self):
        self._running = True
        logger.info(
            f"[{self.alias}] Started: "
            f"{len(self.state.streamers)} streamers, "
            f"limit={self.max_concurrent}, "
            f"proxy={'yes' if self.proxy else 'no'}"
        )

        try:
            await self._check_all_online()
            await self._rebalance()

            while self._running:
                jitter = random.uniform(
                    self.check_interval * 0.95,
                    self.check_interval * 1.05,
                )
                await asyncio.sleep(jitter)
                await self._check_all_online()
                await self._rebalance()

        except asyncio.CancelledError:
            logger.info(f"[{self.alias}] Worker cancelled")
        except Exception as e:
            logger.error(f"[{self.alias}] Worker crashed: {e}")
        finally:
            await self.stop()

    async def _check_all_online(self):
        live_streamers = []
        offline_streamers = []

        for name in self.state.streamer_order:
            if not self._running:
                break
            try:
                stream_id = None
                from_cache = False
                if self.manager:
                    cached_hit, cached_sid = self.manager.get_cached_stream_id(name)
                    if cached_hit:
                        stream_id = cached_sid
                        from_cache = True

                if not from_cache:
                    utility = self._get_utility(name)
                    stream_id = utility.get_stream_id(self.token)
                    if self.manager:
                        self.manager.set_cached_stream_id(name, stream_id)

                st = self.state.streamers[name]
                was_online = st.is_online
                if stream_id is not None:
                    st.is_online = True
                    st.stream_id = stream_id
                elif st.is_watching and getattr(st, "ws_client", None) and getattr(st.ws_client, "state", None) and st.ws_client.state.is_connected:
                    st.is_online = True
                else:
                    st.is_online = False
                    st.stream_id = None

                if st.is_online:
                    live_streamers.append(f"{name} (ID: {stream_id})")
                else:
                    offline_streamers.append(name)

                if not was_online and st.is_online:
                    logger.info(
                        f"[{self.alias}] 🟢 {name} "
                        f"ONLINE (stream={stream_id})"
                    )
                    try:
                        pts = await asyncio.to_thread(self._get_points_checker().get_amount, name, self.token)
                        if pts is not None:
                            st.points = pts
                            st.last_points_update = datetime.now()
                            st.session_start_points = pts
                        elif st.points > 0:
                            st.session_start_points = st.points
                    except Exception:
                        if st.points > 0:
                            st.session_start_points = st.points

                    if self._discord:
                        self._discord.send_streamer_online(
                            self.alias, name,
                            st.priority, "online"
                        )
                    if self._telegram:
                        asyncio.create_task(
                            self._telegram.send_streamer_online(
                                self.alias, name, st.priority, "online"
                            )
                        )

                elif was_online and not st.is_online:
                    logger.info(
                        f"[{self.alias}] 🔴 {name} OFFLINE"
                    )
                    # Refresh latest points after stream ends
                    try:
                        pts = await asyncio.to_thread(self._get_points_checker().get_amount, name, self.token)
                        if pts is not None:
                            st.points = pts
                            st.last_points_update = datetime.now()
                    except Exception:
                        pass

                    if self._discord:
                        self._discord.send_streamer_online(
                            self.alias, name,
                            st.priority, "offline"
                        )
                    if self._telegram:
                        asyncio.create_task(
                            self._telegram.send_streamer_online(
                                self.alias, name, st.priority, "offline"
                            )
                        )
                    if self.manager:
                        asyncio.create_task(
                            self.manager.on_stream_ended(name, self.alias)
                        )

                elif not st.is_online:
                    logger.debug(
                        f"[{self.alias}] ⚫ {name} offline "
                        f"(stream_id=None)"
                    )

                if not from_cache:
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                else:
                    await asyncio.sleep(0.01)

            except Exception as e:
                logger.warning(
                    f"[{self.alias}] Check error for {name}: {e}"
                )

        # 10-minute streamer live-check report
        if live_streamers:
            logger.info(
                f"[{self.alias}] 📺 [10-min Live Check] Streamers LIVE ({len(live_streamers)}): {', '.join(live_streamers)} | "
                f"Offline ({len(offline_streamers)}): {', '.join(offline_streamers)}"
            )
        else:
            logger.info(
                f"[{self.alias}] ⚪ [10-min Live Check] No streamers are live right now. "
                f"All {len(offline_streamers)} offline: {', '.join(offline_streamers)}"
            )

    async def _rebalance(self):
        async with self._rebalance_lock:
            try:
                online_by_priority = [
                    name
                    for name in self.state.streamer_order
                    if self.state.streamers[name].is_online
                ]
                logger.info(f"[{self.alias}] 🔄 _rebalance: online streamers = {online_by_priority}")

                if self.prioritize_active_predictions:
                    try:
                        from utils.adaptive_learning import AdaptiveLearningEngine
                        engine = AdaptiveLearningEngine()
                        def _rank_streamer(n):
                            st_obj = self.state.streamers[n]
                            prof = engine.get_streamer_profile(n)
                            freq = prof.get("total_bets", 0) + st_obj.total_predictions_seen
                            return (
                                not getattr(st_obj, "goal_completed", False),
                                freq > 0,
                                freq,
                                -st_obj.priority,
                            )
                        online_by_priority.sort(key=_rank_streamer, reverse=True)
                    except Exception as e_sort:
                        logger.warning(f"[{self.alias}] Streamer ranking warning: {e_sort}")

                desired = set(
                    online_by_priority[: self.max_concurrent]
                )
                current = {
                    name
                    for name, s in self.state.streamers.items()
                    if s.is_watching
                }
                logger.info(f"[{self.alias}] 🔄 _rebalance: desired={desired}, current={current}")

                to_stop = current - desired
                for name in to_stop:
                    reason = (
                        "offline"
                        if not self.state.streamers[name].is_online
                        else "displaced by higher priority"
                    )
                    logger.info(
                        f"[{self.alias}] ⏹ {name} — {reason}"
                    )
                    await self._stop_streamer(name)

                    if (
                        self._discord
                        and reason == "displaced by higher priority"
                    ):
                        self._discord.send_streamer_online(
                            self.alias, name,
                            self.state.streamers[name].priority,
                            "displaced"
                        )

                    if self._telegram:
                        asyncio.create_task(
                            self._telegram.send_streamer_stopped(
                                self.alias, name, reason
                            )
                        )

                to_start = desired - current
                if to_start:
                    logger.info(f"[{self.alias}] 🚀 Starting {len(to_start)} streamers: {to_start}")
                for name in to_start:
                    pri = self.state.streamers[name].priority
                    logger.info(
                        f"[{self.alias}] ▶ Starting watcher for {name} (priority={pri})"
                    )
                    await self._start_streamer(name)

                    if self._discord:
                        self._discord.send_streamer_online(
                            self.alias, name, pri, "started"
                        )

                    if self._telegram:
                        asyncio.create_task(
                            self._telegram.send_streamer_started(
                                self.alias, name, pri
                            )
                        )

                    await asyncio.sleep(
                        random.uniform(
                            self.stagger_min, self.stagger_max
                        )
                    )

                if desired:
                    logger.info(
                        f"[{self.alias}] Active: "
                        f"{sorted(desired)} "
                        f"({len(desired)}/{self.max_concurrent})"
                    )
            except Exception as e:
                logger.error(f"[{self.alias}] ❌ _rebalance exception: {e}")


    async def _start_streamer(self, name: str):
        st = self.state.streamers[name]

        # Clean up any lingering tasks for this streamer to prevent duplicate loops
        for task in (st.ws_task, st.points_task, st.prediction_task):
            if task and not task.done():
                task.cancel()
        st.ws_task = None
        st.points_task = None
        st.prediction_task = None

        try:
            if not st.channel_id:
                utility = self._get_utility(name)
                st.channel_id = utility.get_channel_id(
                    self.token
                )
                if not st.channel_id:
                    raise RuntimeError(
                        f"Could not get channel_id for {name}"
                    )

            ws_token_getter = self._get_ws_token_getter()
            ws_token = None
            for attempt in range(1, 4):
                ws_token = ws_token_getter.get_ws_token(name)
                if ws_token:
                    break
                if attempt < 3:
                    backoff = attempt * 2 + random.uniform(0.5, 1.5)
                    logger.warning(
                        f"[{self.alias}] WS token retry {attempt}/3 for {name} in {backoff:.1f}s..."
                    )
                    await asyncio.sleep(backoff)

            if not ws_token:
                raise RuntimeError(
                    f"Could not get WS token for {name}"
                )

            async def on_disconnect():
                st.is_watching = False
                logger.warning(
                    f"[{self.alias}] WS {name} disconnected permanently"
                )
                if self._telegram:
                    asyncio.create_task(
                        self._telegram.send_streamer_warning(
                            self.alias, name, "WebSocket disconnected permanently"
                        )
                    )

            ws_client = KickWebSocket(
                data={
                    "token": ws_token,
                    "streamId": st.stream_id or 0,
                    "channelId": st.channel_id,
                },
                proxy=self.proxy,
                on_disconnect=on_disconnect,
            )

            st.ws_client = ws_client
            st.is_watching = True
            st.error_count = 0

            st.ws_task = asyncio.create_task(
                self._ws_wrapper(name, ws_client)
            )
            st.points_task = asyncio.create_task(
                self._points_loop(name)
            )
            is_worker_mode = getattr(self, "worker_mode", False) or bool(getattr(self, "config", {}).get("WorkerMode", False))
            if self.predictions_enabled and not is_worker_mode:
                st.prediction_task = asyncio.create_task(
                    self._prediction_loop(name)
                )

            try:
                pts = await asyncio.to_thread(
                    self._get_points_checker().get_amount,
                    name, self.token
                )
                if pts is not None:
                    st.points = pts
                    st.last_points_update = datetime.now()
                    if st.session_start_points is None:
                        st.session_start_points = pts
            except Exception:
                pass

            # Ensure streamer is followed
            try:
                await asyncio.to_thread(self.follow_streamer, name)
            except Exception:
                pass

            had_error = st.error_count > 0 or bool(st.last_error)
            st.error_count = 0
            st.last_error = None

            logger.success(
                f"[{self.alias}] ✅ Watching {name}"
            )
            if had_error and self._telegram:
                asyncio.create_task(
                    self._telegram.send_streamer_started(
                        self.alias, name, self.state.streamers[name].priority
                    )
                )

        except Exception as e:
            logger.error(
                f"[{self.alias}] Error starting {name}: {e}"
            )
            st.error_count += 1
            st.last_error = str(e)
            st.is_watching = False

            if self._discord:
                self._discord.send_error(
                    self.alias, name, str(e)
                )
            if self._telegram:
                asyncio.create_task(
                    self._telegram.send_streamer_error(
                        name, str(e), account_alias=self.alias
                    )
                )

    async def _stop_streamer(self, name: str):
        st = self.state.streamers[name]

        for task in (st.ws_task, st.points_task, st.prediction_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if st.ws_client:
            try:
                await st.ws_client.disconnect()
            except Exception:
                pass

        st.is_watching = False
        st.ws_client = None
        st.ws_task = None
        st.points_task = None
        st.prediction_task = None
        logger.info(f"[{self.alias}] ⏹ {name} stopped")

    async def _ws_wrapper(
        self, name: str, ws_client: KickWebSocket
    ):
        try:
            await ws_client.connect()
        except Exception as e:
            logger.error(
                f"[{self.alias}] WS {name} crashed: {e}"
            )
            if self._telegram:
                asyncio.create_task(
                    self._telegram.send_streamer_error(
                        name, f"WebSocket crashed: {e}", account_alias=self.alias
                    )
                )
        finally:
            st = self.state.streamers[name]
            st.is_watching = False
            for task in (st.points_task, st.prediction_task):
                if task and not task.done():
                    task.cancel()
            st.points_task = None
            st.prediction_task = None

            if self._running:
                logger.info(f"[{self.alias}] 🔄 Streamer {name} session dropped. Scheduling reconnect in 8s...")
                asyncio.create_task(self._delayed_rebalance(8))

    async def _delayed_rebalance(self, delay: float = 15):
        await asyncio.sleep(delay)
        if self._running:
            await self._rebalance()

    async def _points_loop(self, name: str):
        st = self.state.streamers[name]
        checker = self._get_points_checker()

        while st.is_watching and self._running:
            try:
                await asyncio.sleep(random.uniform(120, 180))
                if not st.is_watching:
                    break

                amount = await asyncio.to_thread(checker.get_amount, name, self.token)
                if amount is None:
                    continue

                old = st.points
                st.points = amount
                st.last_points_update = datetime.now()

                if st.session_start_points is None:
                    st.session_start_points = old if old > 0 else amount

                if amount > old:
                    gain = amount - old
                    logger.success(
                        f"[{self.alias}] 💰 {name}: "
                        f"+{gain} (Total: {amount})"
                    )

                    if self._discord:
                        self._discord.send_points_update(
                            self.alias, name, old, amount
                        )

                    if self._telegram:
                        total_all = self.manager.get_total_points() if self.manager else None
                        asyncio.create_task(
                            self._telegram.send_points_update(
                                streamer=name,
                                old_amount=old,
                                new_amount=amount,
                                account_alias=self.alias,
                                total_all=total_all,
                            )
                        )

                    if getattr(self, "auto_redeem", None) and self.auto_redeem.enabled:
                        await self._auto_redeem_check(name, amount)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"[{self.alias}] Points error for {name}: {e}"
                )

    async def _auto_redeem_check(self, name: str, current_points: int):
        if not getattr(self, "auto_redeem", None) or not self.auto_redeem.enabled or current_points <= 0:
            return
        try:
            redeem_results = await self.auto_redeem.check_and_redeem(
                streamer=name,
                current_points=current_points,
                token=self.token,
                account_alias=self.alias,
                proxy=self.proxy
            )
            if redeem_results and self._telegram:
                for res in redeem_results:
                    if res.get("status") == "success" and hasattr(self._telegram, "send_reward_redeemed"):
                        await self._telegram.send_reward_redeemed(
                            account_alias=self.alias,
                            streamer=name,
                            reward_title=res.get("reward", "Reward"),
                            cost=res.get("cost", 0),
                            balance_remaining=max(0, current_points - res.get("cost", 0)),
                            message_sent=res.get("message_sent", ""),
                            description=res.get("description", "")
                        )
                    elif res.get("status") == "upi_ready_alert" and hasattr(self._telegram, "send_upi_reward_ready"):
                        await self._telegram.send_upi_reward_ready(
                            account_alias=self.alias,
                            streamer=name,
                            reward_title=res.get("reward", "UPI Reward"),
                            cost=res.get("cost", 0),
                            balance=current_points,
                            upi_id=res.get("upi_id", "patelnand@fam")
                        )
        except Exception as e:
            logger.debug(f"[{self.alias}] Auto-redeem check error: {e}")

    async def _prediction_loop(self, name: str):
        st = self.state.streamers[name]
        pred_client = self._get_prediction_client()

        poll_interval = min(self.prediction_check_interval, 5)

        logger.info(
            f"[{self.alias}] 🎯 Prediction automation active for {name} "
            f"(Stages: {self.stage1_percentage}%/{self.stage2_percentage}%/{self.stage3_percentage}%, "
            f"Strategy: {self.prediction_strategy}, Poll: {poll_interval}s)"
        )

        while st.is_watching and self._running and self.predictions_enabled:
            try:
                pred_data = pred_client.get_latest_prediction(name, token=self.token)
                if not pred_data:
                    await asyncio.sleep(poll_interval)
                    continue

                pred = pred_data.get("prediction")
                if not pred or not isinstance(pred, dict):
                    st.active_prediction = None
                    await asyncio.sleep(poll_interval)
                    continue

                pred_state = pred.get("state")
                pred_id = pred.get("id")
                user_vote = pred_data.get("user_vote")

                # Self-recovery: If no active pending bet in memory, but Kick API indicates account has voted on this ACTIVE/LOCKED prediction
                if (not st.pending_bet or st.pending_bet.get("resolved")) and user_vote and pred_state in ("ACTIVE", "LOCKED"):
                    vote_oid = user_vote.get("outcome_id")
                    vote_amt = int(user_vote.get("total_vote_amount") or 0)
                    outcomes = pred.get("outcomes", [])
                    matched_out = next((o for o in outcomes if o.get("id") == vote_oid), None)
                    out_title = matched_out.get("title", "") if matched_out else ""
                    rate = float(matched_out.get("return_rate", 1.0) or 1.0) if matched_out else 1.0
                    st.pending_bet = {
                        "prediction_id": pred_id,
                        "outcome_id": vote_oid,
                        "outcome_title": out_title,
                        "title": pred.get("title", ""),
                        "amount": vote_amt,
                        "multiplier": rate,
                        "pre_wallet": (st.points or 0) + vote_amt,
                        "placed_at": datetime.now(),
                        "resolved": False,
                    }
                    st.active_prediction = {
                        "prediction": pred,
                        "voted": True,
                        "outcome": out_title,
                        "amount": vote_amt,
                    }
                    self._persist_streamer_state(name)
                    logger.info(
                        f"[{self.alias}] 🔄 Re-attached to in-flight bet for '{pred.get('title')}' in {name}: "
                        f"{vote_amt:,} pts on '{out_title}' (Odds: 1:{rate:.2f})"
                    )

                # 1. Check if a previously placed bet on this channel has resolved or paid out
                if st.pending_bet and not st.pending_bet.get("resolved"):
                    prev_pid = st.pending_bet.get("prediction_id")
                    if pred_id != prev_pid or pred_state in ("RESOLVED", "CANCELLED"):
                        st.pending_bet["resolved"] = True
                        winning_id = pred.get("winning_outcome_id")
                        did_win = bool(winning_id and winning_id == st.pending_bet.get("outcome_id"))

                        if pred_state == "CANCELLED":
                            st.last_bet_result = "CANCELLED"
                            logger.info(
                                f"[{self.alias}] 🔄 Prediction '{st.pending_bet.get('title')}' in {name} was CANCELLED. Points refunded."
                            )
                        else:
                            if pred_state == "RESOLVED" and winning_id and pred.get("outcomes"):
                                try:
                                    consensus_outcome = max(
                                        pred["outcomes"],
                                        key=lambda o: (o.get("vote_count", 0) or 0, o.get("total_vote_amount", 0) or 0)
                                    )
                                    did_consensus_win = bool(consensus_outcome.get("id") == winning_id)
                                    record_consensus_outcome_to_session(name, did_consensus_win)
                                except Exception:
                                    pass

                            if did_win:
                                st.consecutive_wins += 1
                                st.consecutive_losses = 0
                                st.last_bet_result = "WIN"
                                self._persist_streamer_state(name)
                            else:
                                st.consecutive_losses += 1
                                st.consecutive_wins = 0
                                st.last_bet_result = "LOSS"
                                self._persist_streamer_state(name)

                        # Fetch fresh points from Kick directly
                        fresh_wallet = None
                        for _ in range(3):
                            fresh_wallet = self._get_points_checker().get_amount(name, self.token)
                            if fresh_wallet is not None:
                                break
                            await asyncio.sleep(1.5)

                        if fresh_wallet is not None:
                            old_pts = st.points
                            st.points = fresh_wallet
                            st.last_points_update = datetime.now()

                            pre_wallet = st.pending_bet.get("pre_wallet", old_pts)
                            amt_bet = st.pending_bet.get("amount", 0)
                            rate = float(st.pending_bet.get("multiplier", 1.0) or 1.0)
                            net_gain = fresh_wallet - pre_wallet

                            # Robust win detection: confirmed by winning outcome ID, or wallet balance increased past pre_wallet
                            is_confirmed_win = did_win or (net_gain > 10) or (fresh_wallet > (pre_wallet - amt_bet + int(amt_bet * rate * 0.5)))

                            if pred_state == "CANCELLED" or (abs(fresh_wallet - pre_wallet) <= 10 and not did_win):
                                logger.info(
                                    f"[{self.alias}] 🔄 Prediction '{st.pending_bet.get('title')}' in {name} concluded with refund/cancellation. Wallet: {fresh_wallet:,} pts."
                                )
                            elif is_confirmed_win:
                                if st.last_bet_result != "WIN":
                                    st.consecutive_wins = 1
                                    st.consecutive_losses = 0
                                    st.last_bet_result = "WIN"
                                    self._persist_streamer_state(name)

                                payout = int(amt_bet * rate) if amt_bet > 0 else (amt_bet + max(0, net_gain))
                                profit = payout - amt_bet if amt_bet > 0 else max(0, net_gain)
                                gain = net_gain if net_gain > 0 else profit

                                logger.success(
                                    f"[{self.alias}] 🏆 Last bet coins ARRIVED for {name}! "
                                    f"New Balance: {fresh_wallet:,} pts (+{gain:,} pts). "
                                    f"Win streak: {st.consecutive_wins} 🔥"
                                )
                                if self._telegram:
                                    asyncio.create_task(self._telegram.send_prediction_won(
                                        account_alias=self.alias,
                                        streamer=name,
                                        title=st.pending_bet.get("title", ""),
                                        outcome_title=st.pending_bet.get("outcome_title", ""),
                                        amount_bet=amt_bet,
                                        payout=payout,
                                        new_balance=fresh_wallet,
                                        multiplier=rate,
                                    ))
                                if self._discord:
                                    self._discord.send_prediction_won(
                                        account_alias=self.alias,
                                        streamer=name,
                                        title=st.pending_bet.get("title", ""),
                                        outcome_title=st.pending_bet.get("outcome_title", ""),
                                        amount_bet=amt_bet,
                                        payout=payout,
                                        new_balance=fresh_wallet,
                                        multiplier=rate,
                                    )
                                log_bet_to_csv(self.alias, name, st.pending_bet.get("title", ""), st.pending_bet.get("outcome_title", ""), rate, amt_bet, "WIN", payout, profit, fresh_wallet, is_consensus=st.pending_bet.get("is_consensus", True), is_whale=st.pending_bet.get("is_whale", False))
                                self._persist_streamer_state(name)
                            else:
                                logger.warning(
                                    f"[{self.alias}] ❌ Last bet LOST for {name}. "
                                    f"Current Balance: {fresh_wallet:,} pts. "
                                    f"Loss streak: {st.consecutive_losses} ❄️"
                                )
                                log_bet_to_csv(self.alias, name, st.pending_bet.get("title", ""), st.pending_bet.get("outcome_title", ""), rate, int(amt_bet or 0), "LOSS", 0, -int(amt_bet or 0), fresh_wallet or st.points, is_consensus=st.pending_bet.get("is_consensus", True), is_whale=st.pending_bet.get("is_whale", False))
                                self._persist_streamer_state(name)
                                if self._telegram:
                                    step_idx = min(max(0, st.consecutive_losses - 1), len(self.recovery_percentage_steps) - 1)
                                    next_pct = self.recovery_percentage_steps[step_idx] if self.recovery_percentage_steps else 0
                                    asyncio.create_task(self._telegram.send_prediction_lost(
                                        account_alias=self.alias,
                                        streamer=name,
                                        title=st.pending_bet.get("title", ""),
                                        outcome_title=st.pending_bet.get("outcome_title", ""),
                                        amount_lost=int(st.pending_bet.get("amount", 0) or 0),
                                        consecutive_losses=st.consecutive_losses,
                                        new_balance=fresh_wallet or st.points,
                                        next_step_pct=next_pct
                                    ))

                # 2. If the current prediction is not ACTIVE, nothing to bet on right now
                if pred_state != "ACTIVE" or not pred_id:
                    st.active_prediction = None
                    await asyncio.sleep(poll_interval)
                    continue

                # Record prediction frequency activity for dynamic streamer rebalancing
                st.last_prediction_time = datetime.now()
                if not hasattr(st, "_seen_prediction_ids"):
                    st._seen_prediction_ids = set()
                if pred_id not in st._seen_prediction_ids:
                    st._seen_prediction_ids.add(pred_id)
                    st.total_predictions_seen += 1
                    self._persist_streamer_state(name)

                # 3. Check if we already voted on this prediction
                already_voted = (user_vote is not None or pred_id in self._voted_predictions)
                st.active_prediction = {
                    "prediction": pred,
                    "voted": already_voted,
                    "user_vote": user_vote,
                }

                if already_voted:
                    await asyncio.sleep(poll_interval)
                    continue

                # 4. Check if channel target points reached (e.g. 500k stop rule per streamer)
                stop_target = self.prediction_stop_targets.get(name.lower(), self.prediction_default_stop_target)
                if stop_target and st.points >= stop_target:
                    fresh_pts = self._get_points_checker().get_amount(name, self.token)
                    if fresh_pts is not None:
                        st.points = fresh_pts
                    if st.points >= stop_target:
                        was_completed = st.goal_completed
                        st.goal_completed = True
                        self._persist_streamer_state(name)
                        if not was_completed:
                            logger.success(
                                f"[{self.alias}] 🎯 Goal reached for {name}! Balance is {st.points:,} pts "
                                f"(Target: {stop_target:,} pts). Rotating to next streamer!"
                            )
                            if self._telegram:
                                asyncio.create_task(self._telegram.send_goal_reached(
                                    account_alias=self.alias,
                                    streamer=name,
                                    points=st.points,
                                    target=stop_target
                                ))
                            asyncio.create_task(self._rebalance())
                        else:
                            logger.info(
                                f"[{self.alias}] 🎯 Goal reached for {name}! Balance is {st.points:,} pts "
                                f"(Target: {stop_target:,} pts). Stopping bets to protect your points!"
                            )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue

                # 4. Wait for last bet coins to arrive before putting another bet
                if st.pending_bet and not st.pending_bet.get("resolved"):
                    prev_pid = st.pending_bet.get("prediction_id")
                    if prev_pid != pred_id:
                        # Previous bet was on another prediction, verify if payout landed
                        live_pts = self._get_points_checker().get_amount(name, self.token)
                        if live_pts is not None:
                            st.points = live_pts
                            st.last_points_update = datetime.now()
                        logger.info(
                            f"[{self.alias}] ⏳ Waiting for payout coins from previous bet on {name} before betting on '{pred.get('title')}'..."
                        )

                # 5. Calculate time until lock
                lock_dt = None
                if pred.get("locked_at"):
                    try:
                        lock_dt = datetime.fromisoformat(pred["locked_at"].replace("Z", "+00:00"))
                    except Exception:
                        pass

                if not lock_dt and pred.get("created_at") and pred.get("duration"):
                    try:
                        c_dt = datetime.fromisoformat(pred["created_at"].replace("Z", "+00:00"))
                        dur = int(pred["duration"])
                        lock_dt = c_dt + timedelta(seconds=dur)
                    except Exception:
                        pass

                now_dt = datetime.now(timezone.utc)
                if lock_dt:
                    time_left = (lock_dt - now_dt).total_seconds()
                else:
                    time_left = 5.0

                if time_left and time_left < -30.0:
                    logger.debug(
                        f"[{self.alias}] Prediction '{pred.get('title')}' in {name} past lock window ({time_left:.1f}s)"
                    )
                    self._voted_predictions.add(pred_id)
                    await asyncio.sleep(poll_interval)
                    continue

                # Wait until the last moment window (target: seconds_before_lock + 2s)
                target_lead = max(self.prediction_seconds_before_lock + 2, 4)

                pre_snapshot = None
                if time_left and time_left > 15:
                    wait_to_pre = time_left - 15
                    logger.info(
                        f"[{self.alias}] ⏳ Prediction '{pred.get('title')}' in {name} is ACTIVE "
                        f"({time_left:.1f}s left). Waiting {wait_to_pre:.1f}s for pre-lock snapshot..."
                    )
                    await asyncio.sleep(wait_to_pre)
                    pre_data = pred_client.get_latest_prediction(name, token=self.token)
                    if pre_data and pre_data.get("prediction"):
                        pre_snapshot = pre_data["prediction"]

                    now_dt = datetime.now(timezone.utc)
                    if lock_dt:
                        time_left = (lock_dt - now_dt).total_seconds()
                    else:
                        time_left = 15.0

                if time_left and time_left > target_lead:
                    wait_s = time_left - target_lead
                    await asyncio.sleep(wait_s)

                # Now in the final window (~5-7s before lock):
                # 6. Fetch fresh prediction data for the latest voter distribution
                fresh_data = pred_client.get_latest_prediction(name, token=self.token)
                fresh_pred = None
                if fresh_data:
                    fresh_pred = fresh_data.get("prediction")
                    if fresh_data.get("user_vote") is not None or pred_id in self._voted_predictions:
                        st.active_prediction = {
                            "prediction": fresh_pred or pred,
                            "voted": True,
                            "user_vote": fresh_data.get("user_vote"),
                        }
                        await asyncio.sleep(poll_interval)
                        continue

                target_pred = fresh_pred if (fresh_pred and isinstance(fresh_pred, dict)) else pred
                outcomes = target_pred.get("outcomes", [])
                if not outcomes:
                    logger.warning(f"[{self.alias}] No outcomes found for prediction in {name}")
                    await asyncio.sleep(poll_interval)
                    continue

                # Feature 2: Compute Late-Lock Odds Velocity & Smart Money Inflow
                smart_money_inflow = None
                if pre_snapshot and pre_snapshot.get("outcomes") and outcomes:
                    try:
                        pre_map = {o["id"]: o for o in pre_snapshot["outcomes"]}
                        total_pre_pts = sum((o.get("total_vote_amount", 0) or 0) for o in pre_snapshot["outcomes"])
                        total_late_pts = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)
                        delta_total_pts = max(1, total_late_pts - total_pre_pts)

                        for o in outcomes:
                            oid = o["id"]
                            if oid in pre_map:
                                r_pre = float(pre_map[oid].get("return_rate", 1.0) or 1.0)
                                r_late = float(o.get("return_rate", 1.0) or 1.0)
                                delta_odds = r_late - r_pre
                                delta_pts = (o.get("total_vote_amount", 0) or 0) - (pre_map[oid].get("total_vote_amount", 0) or 0)
                                vol_pct = (delta_pts / delta_total_pts) * 100.0 if delta_total_pts > 0 else 0.0

                                if delta_odds <= -0.20 and vol_pct >= 60.0 and delta_pts >= 500:
                                    smart_money_inflow = {
                                        "outcome_id": oid,
                                        "delta_odds": delta_odds,
                                        "volume_pct": vol_pct,
                                    }
                                    break
                    except Exception as e:
                        logger.debug(f"Smart money calculation error: {e}")

                # 6. Fetch real-time points balance directly from Kick before computing bets
                fresh_balance = self._get_points_checker().get_amount(name, self.token)
                if fresh_balance is not None:
                    current_balance = fresh_balance
                    st.points = fresh_balance
                    st.last_points_update = datetime.now()
                else:
                    current_balance = st.points

                if not current_balance or current_balance < self.prediction_min_bet:
                    logger.warning(
                        f"[{self.alias}] ⚠️ Insufficient points for {name} ({current_balance} pts, min {self.prediction_min_bet}). Skipping."
                    )
                    self._voted_predictions.add(pred_id)
                    await asyncio.sleep(poll_interval)
                    continue

                stop_target = self.prediction_stop_targets.get(name.lower(), self.prediction_default_stop_target)
                if stop_target and current_balance >= stop_target:
                    was_completed = st.goal_completed
                    st.goal_completed = True
                    self._persist_streamer_state(name)
                    if not was_completed:
                        logger.success(
                            f"[{self.alias}] 🎯 Goal reached for {name}! Balance is {current_balance:,} pts "
                            f"(Target: {stop_target:,} pts). Rotating to next streamer!"
                        )
                        if self._telegram:
                            asyncio.create_task(self._telegram.send_goal_reached(
                                account_alias=self.alias,
                                streamer=name,
                                points=current_balance,
                                target=stop_target
                            ))
                        asyncio.create_task(self._rebalance())
                    else:
                        logger.info(
                            f"[{self.alias}] 🎯 Goal reached for {name}! Balance is {current_balance:,} pts "
                            f"(Target: {stop_target:,} pts). Stopping bets to protect your points!"
                        )
                    self._voted_predictions.add(pred_id)
                    await asyncio.sleep(poll_interval)
                    continue

                if self.prediction_strategy == "minimal_risk":
                    best_outcome, chosen_pct, stage_info, skip_reason = KickPrediction.select_minimal_risk_outcome(
                        outcomes,
                        current_balance=current_balance,
                        aggressive_threshold=self.aggressive_threshold,
                        moderate_threshold=self.moderate_threshold,
                        aggressive_stages=(self.aggressive_stage1_pct, self.aggressive_stage2_pct, self.aggressive_stage3_pct),
                        moderate_stages=(self.moderate_stage1_pct, self.moderate_stage2_pct, self.moderate_stage3_pct),
                        safe_stages=(self.safe_stage1_pct, self.safe_stage2_pct, self.safe_stage3_pct),
                    )
                    if not best_outcome:
                        logger.info(
                            f"[{self.alias}] 🛡️ Minimal-Risk System SKIPPED '{target_pred.get('title')}' in {name}: {skip_reason}"
                        )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue
                elif self.prediction_strategy == "voter_focused_3stage":
                    best_outcome, chosen_pct, stage_info, consensus_pct = KickPrediction.select_voter_focused_outcome(
                        outcomes,
                        stage1_pct=self.stage1_percentage,
                        stage2_pct=self.stage2_percentage,
                        stage3_pct=self.stage3_percentage,
                    )
                    # --- New safety check ---
                    # If the odds (return_rate) are too low for a high‑risk bet (65%),
                    # automatically downgrade to the next stage (45%).
                    odds = best_outcome.get('return_rate', 1.0)
                    if chosen_pct == self.stage1_percentage and odds < self.prediction_min_odds_stage1:
                        # Downgrade to stage 2 (45%) for safer odds
                        chosen_pct = self.stage2_percentage
                        stage_info = f"Stage2 (45%) – downgraded due to low odds ({odds:.2f}x)"
                    else:
                        # Keep the original stage info, but include the odds for logging
                        stage_info = f"{stage_info} (Odds: {odds:.2f}x)"
                elif self.prediction_strategy == "smart_consensus":
                    best_outcome, skip_reason = KickPrediction.select_smart_outcome(
                        outcomes,
                        min_voters=10,
                        min_odds=1.15,
                        max_odds=3.0,
                        min_consensus_pct=55.0,
                    )
                    chosen_pct = 35
                    stage_info = "Smart Consensus"
                    if not best_outcome:
                        logger.warning(
                            f"[{self.alias}] 🛡️ Smart System SKIPPED '{target_pred.get('title')}' in {name}: {skip_reason}"
                        )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue
                elif self.prediction_strategy == "kelly_criterion":
                    best_outcome, chosen_pct, stage_info, skip_reason = KickPrediction.select_kelly_outcome(
                        outcomes,
                        kelly_fraction=self.kelly_fraction,
                    )
                    if not best_outcome:
                        logger.warning(
                            f"[{self.alias}] 🛡️ Kelly Criterion SKIPPED '{target_pred.get('title')}' in {name}: {skip_reason}"
                        )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue
                elif self.prediction_strategy == "anti_whale":
                    best_outcome, chosen_pct, stage_info, skip_reason = KickPrediction.select_anti_whale_outcome(
                        outcomes,
                        min_voter_consensus=self.anti_whale_min_consensus,
                        min_odds=self.anti_whale_min_odds,
                    )
                    if not best_outcome:
                        logger.warning(
                            f"[{self.alias}] 🛡️ Anti-Whale SKIPPED '{target_pred.get('title')}' in {name}: {skip_reason}"
                        )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue
                elif self.prediction_strategy in ("adaptive_learning", "self_learning", "bayesian", "brain", "speedrun_500k"):
                    from utils.adaptive_learning import AdaptiveLearningEngine
                    best_outcome, chosen_pct, stage_info, skip_reason = AdaptiveLearningEngine().evaluate_adaptive_bet(
                        streamer=name,
                        outcomes=outcomes,
                        prediction_title=target_pred.get("title", ""),
                        current_balance=current_balance,
                        aggressive_threshold=self.aggressive_threshold,
                        moderate_threshold=self.moderate_threshold,
                        aggressive_stages=(self.aggressive_stage1_pct, self.aggressive_stage2_pct, self.aggressive_stage3_pct),
                        moderate_stages=(self.moderate_stage1_pct, self.moderate_stage2_pct, self.moderate_stage3_pct),
                        safe_stages=(self.safe_stage1_pct, self.safe_stage2_pct, self.safe_stage3_pct),
                        min_odds_floor=self.min_odds_floor,
                        account_index=self.worker_index,
                        total_accounts=self.total_workers,
                        hedging_enabled=self.hedging_enabled,
                    )
                    if not best_outcome:
                        logger.warning(
                            f"[{self.alias}] 🧠 Adaptive Engine SKIPPED '{target_pred.get('title')}' in {name}: {skip_reason}"
                        )
                        self._voted_predictions.add(pred_id)
                        await asyncio.sleep(poll_interval)
                        continue
                else:
                    best_outcome = KickPrediction.select_outcome(outcomes, strategy=self.prediction_strategy)
                    chosen_pct = 35
                    stage_info = self.prediction_strategy

                if not best_outcome:
                    logger.warning(f"[{self.alias}] Could not determine best outcome for {name}")
                    await asyncio.sleep(poll_interval)
                    continue

                # Feature 4: Smart Odds Safety Floor (Never bet on low-yield favorite traps)
                odds = float(best_outcome.get("return_rate", 1.0) or 1.0)
                if self.min_odds_floor > 1.0 and odds < self.min_odds_floor:
                    logger.warning(
                        f"[{self.alias}] 🛡️ Skipping '{target_pred.get('title')}' in {name}: "
                        f"Odds {odds:.2f}x below safety floor ({self.min_odds_floor:.2f}x) - avoiding heavy favorite trap."
                    )
                    self._voted_predictions.add(pred_id)
                    await asyncio.sleep(poll_interval)
                    continue

                # Feature 2: Bootstrap / Low-Balance Turbo Accelerator (< threshold pts)
                if self.bootstrap_turbo_enabled and current_balance < self.bootstrap_turbo_threshold:
                    chosen_pct = self.bootstrap_turbo_pct
                    stage_info = f"🚀 Low-Balance Turbo ({current_balance:,} < {self.bootstrap_turbo_threshold:,} pts) -> Bet {chosen_pct}%"

                # --- Progression Modifiers (Martingale Loss Recovery & Paroli Win Streak) ---
                if self.loss_recovery_enabled and st.consecutive_losses > 0:
                    streak = min(st.consecutive_losses, self.loss_recovery_max_streak)
                    if self.loss_recovery_mode == "percentage_steps" and self.loss_recovery_steps:
                        step_idx = min(st.consecutive_losses - 1, len(self.loss_recovery_steps) - 1)
                        chosen_pct = self.loss_recovery_steps[step_idx]
                        stage_info = f"🔥 Martingale Recovery (Loss Streak: {st.consecutive_losses}, step {step_idx + 1}) -> Bet {chosen_pct}%"
                    elif self.loss_recovery_mode == "multiplier":
                        multiplier = self.loss_recovery_multiplier ** streak
                        chosen_pct = min(95, max(5, int(chosen_pct * multiplier)))
                        stage_info = f"🔥 Martingale Multiplier (Loss Streak: {st.consecutive_losses}, {multiplier:.1f}x) -> Bet {chosen_pct}%"
                elif self.win_streak_enabled and st.consecutive_wins > 0:
                    if st.consecutive_wins > self.win_streak_max_streak:
                        st.consecutive_wins = 0
                        stage_info += f" [⚡ Reverse Martingale Cycle Complete ({self.win_streak_max_streak} wins banked) -> Reset to Base]"
                    elif self.win_streak_steps:
                        step_idx = min(st.consecutive_wins - 1, len(self.win_streak_steps) - 1)
                        chosen_pct = self.win_streak_steps[step_idx]
                        stage_info = f"⚡ Reverse Martingale (Win Streak: {st.consecutive_wins}, step {step_idx + 1}) -> Bet {chosen_pct}%"

                # Compute smart bet amount with optional hard cap protection
                bet_amount = int(current_balance * (chosen_pct / 100.0))
                # For small balances (< 100 pts), never bet more than min_bet to protect remaining coins
                if current_balance < 100:
                    bet_amount = min(self.prediction_min_bet, current_balance)
                if self.prediction_max_bet_cap > 0:
                    bet_amount = min(bet_amount, self.prediction_max_bet_cap)
                bet_amount = max(self.prediction_min_bet, min(bet_amount, current_balance))

                # Slippage & Pool Dilution Defense: Never take more than self.pool_cap_percentage of the pool
                outcome_pool = best_outcome.get("total_vote_amount", 0) or 0
                total_pool_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)
                if self.pool_cap_percentage > 0 and current_balance > 3000:
                    if outcome_pool > 500:
                        max_pool_share = int(outcome_pool * self.pool_cap_percentage)
                        if bet_amount > max_pool_share:
                            capped_amt = max(self.prediction_min_bet, max_pool_share)
                            if capped_amt < bet_amount:
                                bet_amount = capped_amt
                                stage_info += f" [Slippage-Protected: {int(self.pool_cap_percentage * 100)}% of outcome pool]"
                    elif total_pool_points > 1000:
                        max_pool_share = int(total_pool_points * self.pool_cap_percentage)
                        if bet_amount > max_pool_share:
                            capped_amt = max(self.prediction_min_bet, max_pool_share)
                            if capped_amt < bet_amount:
                                bet_amount = capped_amt
                                stage_info += f" [Pool-Capped: {int(self.pool_cap_percentage * 100)}% of total pool]"

                # Feature 1: Multi-Account Fleet Hedging & Asymmetric Dutching
                is_hedging = any(h in stage_info for h in ("Brain Hedging", "Smart Hedging"))
                if is_hedging and self.manager and len(outcomes) == 2:
                    my_odds = float(best_outcome.get("return_rate", 1.0) or 1.0)
                    opp_outcome = outcomes[1] if best_outcome["id"] == outcomes[0]["id"] else outcomes[0]
                    opp_odds = float(opp_outcome.get("return_rate", 1.0) or 1.0)
                    dutch_amt = self.manager.get_dutching_hedge_stake(name, self.worker_index, my_odds, opp_odds, bet_amount)
                    if dutch_amt != bet_amount:
                        bet_amount = dutch_amt
                        stage_info += f" [Asymmetric Dutching: {bet_amount:,} pts @ {my_odds:.2f}x]"
                elif is_hedging and self.manager:
                    matched_amt = self.manager.get_matched_hedge_amount(name, bet_amount)
                    if matched_amt < bet_amount:
                        bet_amount = matched_amt
                        stage_info += f" [Hedge-Matched: {bet_amount:,} pts]"

                if pred_id in self._voted_predictions:
                    logger.debug(f"[{self.alias}] Prediction '{target_pred.get('title')}' in {name} already voted. Skipping.")
                    await asyncio.sleep(poll_interval)
                    continue

                # Mark as voted immediately before placing the vote to prevent race-condition double betting
                self._voted_predictions.add(pred_id)

                # Micro-jitter anti-burst per worker index to avoid simultaneous API bursts
                jitter = float(self.worker_index) * random.uniform(0.20, 0.40)
                if jitter > 0:
                    await asyncio.sleep(jitter)

                logger.info(
                    f"[{self.alias}] 🎲 Placing bet ({stage_info}) for '{target_pred.get('title')}' in {name}: "
                    f"{bet_amount:,} pts on '{best_outcome.get('title')}' (ID: {best_outcome.get('id')})"
                )

                success, result = pred_client.vote_prediction(
                    channel_slug=name,
                    outcome_id=best_outcome["id"],
                    amount=bet_amount,
                    token=self.token,
                )

                if success:
                    st.points = max(0, current_balance - bet_amount)
                    st.last_points_update = datetime.now()

                    rate = best_outcome.get("return_rate", 1.0)
                    total_pts = best_outcome.get("total_vote_amount", 0)
                    voters = best_outcome.get("vote_count", 0)

                    # Determine if best_outcome was consensus favorite and/or whale leader
                    is_consensus = True
                    is_whale = False
                    if outcomes:
                        try:
                            fav_cand = max(outcomes, key=lambda o: (o.get("vote_count", 0) or 0, o.get("total_vote_amount", 0) or 0))
                            is_consensus = bool(best_outcome.get("id") == fav_cand.get("id"))
                            whale_cand = max(outcomes, key=lambda o: (o.get("total_vote_amount", 0) or 0, o.get("vote_count", 0) or 0))
                            is_whale = bool(best_outcome.get("id") == whale_cand.get("id"))
                        except Exception:
                            pass

                    # Save pending bet so we wait for its resolution & payout coins before betting next time!
                    st.pending_bet = {
                        "prediction_id": pred_id,
                        "outcome_id": best_outcome["id"],
                        "outcome_title": best_outcome.get("title", ""),
                        "title": target_pred.get("title", ""),
                        "amount": bet_amount,
                        "multiplier": rate,
                        "pre_wallet": current_balance,
                        "placed_at": datetime.now(),
                        "resolved": False,
                        "is_consensus": is_consensus,
                        "is_whale": is_whale,
                    }
                    self._persist_streamer_state(name)

                    st.active_prediction = {
                        "prediction": target_pred,
                        "voted": True,
                        "outcome": best_outcome.get("title", ""),
                        "amount": bet_amount,
                    }

                    if self.manager:
                        self.manager.record_bet({
                            "timestamp": datetime.now(),
                            "account": self.alias,
                            "streamer": name,
                            "title": target_pred.get("title", ""),
                            "outcome_title": best_outcome.get("title", ""),
                            "amount": bet_amount,
                            "percentage": chosen_pct,
                            "wallet_balance": current_balance,
                            "multiplier": rate,
                        })

                    logger.success(
                        f"[{self.alias}] 🎯 Smart Bet Placed! Streamer: {name} | "
                        f"Prediction: '{target_pred.get('title')}' | Bet: {bet_amount:,} pts ({chosen_pct}% of {current_balance:,}) | "
                        f"Selected: '{best_outcome.get('title')}' (Pool: {total_pts:,} pts, Voters: {voters}) | Odds: 1:{rate:.2f}"
                    )

                    if self._telegram:
                        asyncio.create_task(self._telegram.send_prediction_placed(
                            account_alias=self.alias,
                            streamer=name,
                            title=target_pred.get("title", ""),
                            outcome_title=best_outcome.get("title", ""),
                            amount=bet_amount,
                            percentage=chosen_pct,
                            wallet_balance=current_balance,
                            multiplier=rate,
                        ))

                    if self._discord:
                        self._discord.send_prediction_placed(
                            account_alias=self.alias,
                            streamer=name,
                            title=target_pred.get("title", ""),
                            outcome_title=best_outcome.get("title", ""),
                            amount=bet_amount,
                            percentage=chosen_pct,
                            wallet_balance=current_balance,
                            multiplier=rate,
                        )
                else:
                    logger.error(
                        f"[{self.alias}] ❌ Prediction vote failed for {name}: {result}"
                    )
                    err_str = str(result).lower()
                    if not any(k in err_str for k in ("invalid_argument", "closed", "locked", "already_voted", "already voted")):
                        self._voted_predictions.discard(pred_id)

                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.alias}] Prediction loop error for {name}: {e}")
                await asyncio.sleep(poll_interval)

    async def stop(self):
        self._running = False

        for name in list(self.state.streamers):
            if self.state.streamers[name].is_watching:
                await self._stop_streamer(name)

        if self._points_checker:
            self._points_checker.close()
            self._points_checker = None

        if self._prediction_client:
            self._prediction_client.close()
            self._prediction_client = None

        if self._ws_token_getter:
            self._ws_token_getter.close()
            self._ws_token_getter = None

        for u in self._utility_cache.values():
            u.close()
        self._utility_cache.clear()

    def follow_streamer(self, streamer: str) -> bool:
        """Ensures this account follows the streamer on Kick."""
        try:
            s = self._get_points_checker().session
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}/",
            }
            resp = s.post(
                f"https://kick.com/api/v2/channels/{streamer}/follow",
                headers=headers,
                timeout=12,
            )
            if resp.status_code in (200, 422):
                return True
            return False
        except Exception as e:
            logger.debug(f"[{self.alias}] Follow {streamer} error: {e}")
            return False

    async def follow_all_streamers(self) -> int:
        """Ensures this account follows all its assigned streamers on Kick."""
        count = 0
        for streamer in self.state.streamer_order:
            ok = await asyncio.to_thread(self.follow_streamer, streamer)
            if ok:
                count += 1
            await asyncio.sleep(0.3)
        return count

    def add_streamer(self, streamer_name: str) -> bool:
        """Dynamically add a new streamer to this worker's tracking list."""
        name = streamer_name.strip().lower()
        if name in self.state.streamers:
            return False
        priority = len(self.state.streamer_order)
        self.state.streamer_order.append(name)
        self.state.streamers[name] = StreamerState(name=name, priority=priority)
        logger.info(f"[{self.alias}] ➕ Added streamer '{name}' (priority #{priority})")
        return True

    async def remove_streamer(self, streamer_name: str) -> bool:
        """Dynamically remove a streamer from this worker's tracking list."""
        name = streamer_name.strip().lower()
        if name not in self.state.streamers:
            return False

        if name in self.state.streamer_order:
            self.state.streamer_order.remove(name)

        if self.state.streamers[name].is_watching:
            await self._stop_streamer(name)
        elif name in self.state.streamers:
            st = self.state.streamers[name]
            for task in (st.ws_task, st.points_task, st.prediction_task):
                if task and not task.done():
                    task.cancel()

        del self.state.streamers[name]

        for idx, s_name in enumerate(self.state.streamer_order):
            if s_name in self.state.streamers:
                self.state.streamers[s_name].priority = idx

        if name in self._utility_cache:
            try:
                self._utility_cache[name].close()
            except Exception:
                pass
            del self._utility_cache[name]

        logger.info(f"[{self.alias}] 🗑️ Removed streamer '{name}'")
        asyncio.create_task(self._rebalance())
        return True

    def get_status(self) -> dict:
        return {
            "alias": self.alias,
            "proxy": bool(self.proxy),
            "max_concurrent": self.max_concurrent,
            "active_count": self.state.active_count,
            "active_streamers": self.state.active_names,
            "streamer_order": self.state.streamer_order,
            "streamers": {
                name: {
                    "priority": s.priority,
                    "online": s.is_online,
                    "watching": s.is_watching,
                    "points": s.points,
                    "session_start_points": s.session_start_points,
                    "target": self.prediction_stop_targets.get(name.lower(), self.prediction_default_stop_target),
                    "progress_pct": round(min(100.0, ((s.points or 0) / max(1, self.prediction_stop_targets.get(name.lower(), self.prediction_default_stop_target))) * 100), 1),
                    "consecutive_wins": s.consecutive_wins,
                    "consecutive_losses": s.consecutive_losses,
                    "last_bet_result": s.last_bet_result,
                    "total_predictions": s.total_predictions_seen,
                    "last_update": (
                        s.last_points_update.isoformat()
                        if s.last_points_update
                        else None
                    ),
                    "stream_id": s.stream_id,
                    "errors": s.error_count,
                }
                for name, s in self.state.streamers.items()
            },
        }


class AccountManager:
    def __init__(self, config: dict):
        self.config = config
        self.workers: List[AccountWorker] = []
        self._tasks: List[asyncio.Task] = []
        self._discord: Optional["DiscordWebhook"] = None
        self._telegram = None
        self._last_stream_ended_notifications: Dict[str, float] = {}
        self.bet_history: List[dict] = []
        self._live_stream_cache: Dict[str, Tuple[Optional[int], float]] = {}
        self._is_stopped = False

        proxy_cfg = config.get("Proxy", {})
        self.global_proxy = (
            proxy_cfg.get("url")
            if proxy_cfg.get("enabled") else None
        )

        self.check_interval = config.get("Check_interval", 600)
        self.reconnect_cooldown = config.get(
            "Reconnect_cooldown", 600
        )
        self.stagger_min = config.get("Connection_stagger_min", 3)
        self.stagger_max = config.get("Connection_stagger_max", 8)
        self.prediction_cfg = config.get("Predictions", {})
        self.auto_redeem_cfg = config.get("AutoRedeem", {})
        cg_cfg = config.get("CrossGamble", {})
        self.cross_gamble = None
        if cg_cfg.get("enabled", True):
            try:
                from utils.cross_gamble import CrossGambleOrchestrator
                self.cross_gamble = CrossGambleOrchestrator(
                    hero_alias=cg_cfg.get("hero_alias", "4 valo 01"),
                    hero_bet_pct=float(cg_cfg.get("hero_bet_pct", 0.30)),
                    feeder_bet_pct=float(cg_cfg.get("feeder_bet_pct", 0.50)),
                    max_hero_bet=int(cg_cfg.get("max_hero_bet", 15000)),
                    max_feeder_bet=int(cg_cfg.get("max_feeder_bet", 5000)),
                    min_consensus_pct=float(cg_cfg.get("min_consensus_pct", 55.0))
                )
                logger.info(f"🎲 CrossGamble engine active with Hero [{self.cross_gamble.hero_alias}]")
            except Exception as e_cg:
                logger.error(f"Failed to initialize CrossGamble: {e_cg}")

        # Common streamers list
        common_streamers = config.get("Streamers", [])
        if not common_streamers:
            for acc in config.get("Accounts", []):
                if acc.get("streamers"):
                    common_streamers = list(acc.get("streamers"))
                    break
        self.common_streamers: List[str] = [
            str(s).strip().lower() for s in common_streamers if str(s).strip()
        ]
        logger.info(
            f"📺 Common Streamers ({len(self.common_streamers)}): "
            f"{', '.join(self.common_streamers)}"
        )

        # Обратная совместимость
        accounts = config.get("Accounts", [])
        if not accounts:
            old_token = config.get(
                "Private", {}
            ).get("token", "")
            old_streamers = config.get("Streamers", [])
            old_max = config.get("Max_active_channels", 5)
            if old_token and old_streamers:
                accounts = [{
                    "alias": "Default",
                    "token": old_token,
                    "streamers": old_streamers,
                    "max_concurrent": old_max,
                }]
                logger.warning(
                    "⚠️ Old config format detected. "
                    "Please migrate to Accounts[]."
                )

        total_accs = len(accounts)
        for idx, acc in enumerate(accounts):
            self.workers.append(
                AccountWorker(
                    acc,
                    global_proxy=self.global_proxy,
                    check_interval=self.check_interval,
                    reconnect_cooldown=self.reconnect_cooldown,
                    stagger_min=self.stagger_min,
                    stagger_max=self.stagger_max,
                    manager=self,
                    prediction_cfg=self.prediction_cfg,
                    worker_index=idx,
                    total_workers=total_accs,
                )
            )

        logger.info(
            f"📊 Loaded accounts: {len(self.workers)}, "
            f"global proxy: "
            f"{'yes' if self.global_proxy else 'no'}"
        )

    def set_discord(self, discord: "DiscordWebhook"):
        """Connect Discord webhook to all accounts"""
        self._discord = discord
        for worker in self.workers:
            worker.set_discord(discord)
        logger.info(
            f"🟣 Discord webhook connected to "
            f"{len(self.workers)} accounts"
        )

    def set_telegram(self, telegram_bot):
        """Connect Telegram bot to all accounts"""
        self._telegram = telegram_bot
        for worker in self.workers:
            worker.set_telegram(telegram_bot)
        logger.info(
            f"🤖 Telegram bot connected to "
            f"{len(self.workers)} accounts"
        )

    def get_cached_stream_id(self, streamer: str, ttl: float = 45.0) -> Tuple[bool, Optional[int]]:
        """Return (True, stream_id) if cached within ttl seconds, else (False, None)."""
        entry = self._live_stream_cache.get(streamer.lower())
        if entry:
            sid, ts = entry
            if time.time() - ts < ttl:
                return True, sid
        return False, None

    def set_cached_stream_id(self, streamer: str, stream_id: Optional[int]):
        """Update live stream_id cache for a streamer."""
        self._live_stream_cache[streamer.lower()] = (stream_id, time.time())

    async def on_stream_ended(self, streamer: str, account_alias: str):
        """Notify via Telegram with all account/streamer points summary when a stream ends"""
        now = time.time()
        last_sent = self._last_stream_ended_notifications.get(streamer.lower(), 0)
        if now - last_sent < 120:  # 2-minute debounce across accounts
            return
        self._last_stream_ended_notifications[streamer.lower()] = now

        # Refresh latest points for this streamer across all accounts
        for worker in self.workers:
            for s_name, s_state in worker.state.streamers.items():
                if s_name.lower() == streamer.lower():
                    try:
                        pts = await asyncio.to_thread(worker._get_points_checker().get_amount, s_name, worker.token)
                        if pts is not None:
                            s_state.points = pts
                            s_state.last_points_update = datetime.now()
                    except Exception:
                        pass

        if self._telegram:
            logger.info(f"📢 Sending stream ended points summary for {streamer} to Telegram")
            await self._telegram.send_stream_ended_summary(streamer, ended_account=account_alias)

        # Reset session_start_points for this streamer so the next session starts fresh
        for worker in self.workers:
            for s_name, s_state in worker.state.streamers.items():
                if s_name.lower() == streamer.lower():
                    s_state.session_start_points = None

    def record_bet(self, bet_info: dict):
        self.bet_history.append(bet_info)
        if len(self.bet_history) > 50:
            self.bet_history.pop(0)

    def get_past_bets(self, limit: int = 10) -> List[dict]:
        return list(reversed(self.bet_history))[:limit]

    def get_active_predictions(self) -> List[dict]:
        active = []
        seen = set()
        for worker in self.workers:
            for s in worker.state.streamers.values():
                if s.active_prediction and s.active_prediction.get("prediction"):
                    p = s.active_prediction["prediction"]
                    pid = p.get("id")
                    key = (s.name, pid)
                    if key not in seen:
                        seen.add(key)
                        active.append({
                            "streamer": s.name,
                            "account": worker.alias,
                            "prediction": p,
                            "voted": s.active_prediction.get("voted", False),
                            "outcome": s.active_prediction.get("outcome"),
                            "amount": s.active_prediction.get("amount"),
                            "user_vote": s.active_prediction.get("user_vote"),
                        })
        return active

    def get_live_pending_bets(self) -> List[dict]:
        pending = []
        for worker in self.workers:
            for s in worker.state.streamers.values():
                pb = getattr(s, "pending_bet", None)
                if pb and not pb.get("resolved", False):
                    placed_at = pb.get("placed_at")
                    ts_str = placed_at.strftime("%Y-%m-%d %H:%M:%S") if isinstance(placed_at, datetime) else str(placed_at or "")
                    amt = int(pb.get("amount", 0) or 0)
                    mult = float(pb.get("multiplier", 1.0) or 1.0)
                    est_gain = int(amt * (mult - 1.0)) if mult > 1.0 else 0
                    pending.append({
                        "account": worker.alias,
                        "streamer": s.name,
                        "prediction": pb.get("title", ""),
                        "outcome": pb.get("outcome_title", ""),
                        "amount": amt,
                        "odds": f"{mult:.2f}x",
                        "est_gain": est_gain,
                        "timestamp": ts_str,
                        "status": "In Play",
                    })
        return pending

    async def start_all(self):
        self._is_stopped = False

        # Launch central background services immediately
        if getattr(self, "cross_gamble", None):
            self._tasks.append(asyncio.create_task(self._cross_gamble_loop()))
        self._tasks.append(asyncio.create_task(self._auto_redeem_scanner_loop()))
        self._tasks.append(asyncio.create_task(self._memory_trim_loop()))

        delay_min, delay_max = (0.5, 1.2)
        logger.info(f"🚀 Launching all {len(self.workers)} accounts (stagger {delay_min}-{delay_max}s)...")
        for i, worker in enumerate(self.workers):
            if i > 0:
                delay = random.uniform(delay_min, delay_max)
                await asyncio.sleep(delay)

            task = asyncio.create_task(worker.start())
            self._tasks.append(task)

        if not self.workers:
            logger.warning(
                "⚠️ No accounts loaded! Miner running in standby mode. "
                "Web Dashboard and Telegram Bot are active. "
                "Use Telegram (/addaccount) to connect accounts."
            )

        while not self._is_stopped:
            running_tasks = [t for t in self._tasks if not t.done()]
            if running_tasks:
                await asyncio.gather(*running_tasks, return_exceptions=True)
            await asyncio.sleep(2)

    async def stop_all(self):
        self._is_stopped = True
        for task in self._tasks:
            task.cancel()
        for w in self.workers:
            await w.stop()

    async def _cross_gamble_loop(self):
        """High-performance central loop monitoring live predictions across streamers to execute cross-gamble pool funneling."""
        logger.info(f"🎲 Central Cross-Gamble Funnel active: funneling pool into [{self.cross_gamble.hero_alias}]")
        await asyncio.sleep(5)
        pred_client = KickPrediction()

        while not self._is_stopped:
            try:
                # Concurrent check across all monitored streamers
                async def _poll_streamer_pred(s_name: str):
                    try:
                        p_data = await asyncio.to_thread(pred_client.get_latest_prediction, s_name)
                        if not p_data:
                            return None
                        p_obj = self.cross_gamble.parse_prediction(p_data)
                        if not p_obj:
                            return None
                        if p_obj["id"] in self.cross_gamble.processed_predictions:
                            return None
                        return s_name, p_obj
                    except Exception:
                        return None

                poll_tasks = [_poll_streamer_pred(s) for s in list(self.common_streamers)]
                active_predictions = await asyncio.gather(*poll_tasks)

                for item in active_predictions:
                    if not item:
                        continue
                    streamer, pred_obj = item
                    pred_id = pred_obj["id"]

                    remaining = pred_obj.get("remaining_seconds", 30)
                    is_controlled = (streamer.lower() == "xyroplayzz")

                    # Timing window:
                    # Controlled test channel (xyroplayzz): execute immediately if remaining >= 2
                    # Public stream: execute when remaining <= 30 and remaining >= 3
                    if is_controlled:
                        if remaining < 2:
                            continue
                    else:
                        if remaining > 30 or remaining < 3:
                            continue

                    analysis = self.cross_gamble.analyze_prediction(pred_obj, streamer)
                    if not analysis:
                        continue

                    # Ensure Hero account points are freshly known
                    hero_w = next((w for w in self.workers if w.alias == self.cross_gamble.hero_alias), None)
                    if hero_w:
                        h_st = hero_w.state.streamers.get(streamer)
                        if not h_st or (h_st.points or 0) < 10:
                            try:
                                h_pts = await asyncio.to_thread(hero_w._get_points_checker().get_amount, streamer, hero_w.token)
                                if h_pts and h_st:
                                    h_st.points = h_pts
                            except Exception:
                                pass

                    # Refresh points for zero-point workers concurrently
                    zero_pt_workers = [
                        w for w in self.workers
                        if w.token_valid and ((w.state.streamers.get(streamer).points if w.state.streamers.get(streamer) else 0) < 10)
                    ][:35]
                    if zero_pt_workers:
                        async def _refresh_w_pts(worker):
                            try:
                                await asyncio.sleep(random.uniform(0.05, 0.4))
                                p = await asyncio.to_thread(worker._get_points_checker().get_amount, streamer, worker.token)
                                if p and worker.state.streamers.get(streamer):
                                    worker.state.streamers[streamer].points = p
                            except Exception:
                                pass
                        await asyncio.gather(*[_refresh_w_pts(w) for w in zero_pt_workers])

                    # Collect eligible accounts
                    accounts_data = []
                    for w in self.workers:
                        if not w.token_valid:
                            continue
                        st = w.state.streamers.get(streamer)
                        pts = st.points if st else 0
                        if pts >= 10:
                            accounts_data.append({
                                "alias": w.alias,
                                "points": pts,
                                "token": w.token,
                                "worker": w
                            })

                    if not accounts_data:
                        logger.debug(f"[{streamer}] No accounts with >= 10 points for cross-gamble")
                        continue

                    bets = self.cross_gamble.compute_bets(analysis, accounts_data)
                    logger.info(f"⚡ Cross-gamble dispatching {len(bets)} bets concurrently on {streamer} for '{analysis['title']}'...")

                    # Mark processed right away so concurrent ticks do not duplicate
                    self.cross_gamble.processed_predictions.add(pred_id)

                    # Priority execution: Hero bets FIRST, Feeders follow with paced stagger to prevent 429
                    async def _place_single_bet(b, max_retries=3):
                        acc = b["account"]
                        amt = b["amount"]
                        out_id = b["outcome_id"]
                        out_title = b["outcome_title"]
                        role = b["role"]
                        for attempt in range(max_retries):
                            try:
                                ok, res = await asyncio.to_thread(
                                    pred_client.vote_prediction,
                                    streamer, out_id, amt, acc["token"]
                                )
                                if ok:
                                    odds_val = analysis.get("hero_odds" if b["is_hero"] else "feeder_odds", 1.0)
                                    logger.success(f"[{acc['alias']}] 🎯 [{role}] Voted {amt:,} pts on '{out_title}' (Odds: {odds_val:.2f}x)")
                                    if acc.get("worker") and acc["worker"].state.streamers.get(streamer):
                                        acc["worker"].state.streamers[streamer].points = max(0, acc["points"] - amt)
                                    return True, b
                                else:
                                    res_str = str(res)
                                    if "429" in res_str or "TOO_MANY_REQUESTS" in res_str:
                                        logger.warning(f"[{acc['alias']}] ⏳ Rate limited (429) on vote, backing off {0.5 * (attempt + 1)}s (attempt {attempt+1}/{max_retries})")
                                        await asyncio.sleep(0.5 * (attempt + 1))
                                        continue
                                    logger.warning(f"[{acc['alias']}] ⚠️ Cross-gamble vote: {res}")
                                    return False, b
                            except Exception as e_vote:
                                logger.error(f"[{acc['alias']}] Vote error: {e_vote}")
                                await asyncio.sleep(0.3)
                        return False, b

                    hero_b = next((b for b in bets if b.get("is_hero")), None)
                    feeder_bets = [b for b in bets if not b.get("is_hero")]
                    executed_bets = []

                    # Step 1: Place HERO bet FIRST
                    if hero_b:
                        logger.info(f"👑 Executing Hero [{self.cross_gamble.hero_alias}] bet FIRST on '{hero_b['outcome_title']}' ({hero_b['amount']:,} pts)...")
                        ok_hero, _ = await _place_single_bet(hero_b, max_retries=5)
                        if ok_hero:
                            executed_bets.append(hero_b)
                        else:
                            logger.error(f"🚨 Failed to place Hero bet for [{self.cross_gamble.hero_alias}]!")

                    # Step 2: Place Feeder bets with controlled 0.18s spacing (no 429)
                    for fb in feeder_bets:
                        await asyncio.sleep(random.uniform(0.15, 0.25))
                        ok_f, _ = await _place_single_bet(fb, max_retries=2)
                        if ok_f:
                            executed_bets.append(fb)

                    # Telegram notification
                    if self._telegram and hasattr(self._telegram, "_get_recipients") and executed_bets:
                        hero_b = next((b for b in executed_bets if b.get("is_hero")), None)
                        hero_str = f"{hero_b['amount']:,} pts on <b>{analysis['hero_outcome_title']}</b>" if hero_b else "None"
                        feeder_total = sum(b["amount"] for b in executed_bets if not b.get("is_hero"))
                        feeder_count = len([b for b in executed_bets if not b.get("is_hero")])
                        msg = (
                            f"🎲 <b>CROSS-GAMBLE EXECUTED!</b>\n\n"
                            f"👤 <b>Streamer:</b> <code>{html.escape(streamer)}</code>\n"
                            f"🎯 <b>Prediction:</b> <i>{html.escape(analysis['title'])}</i>\n"
                            f"👑 <b>Hero [{self.cross_gamble.hero_alias}]:</b> {hero_str}\n"
                            f"👥 <b>Feeders ({feeder_count}):</b> {feeder_total:,} pts into <b>{analysis['feeder_outcome_title']}</b>\n"
                            f"📊 <b>Strategy:</b> <code>{analysis['strategy'].upper()}</code>\n"
                            f"⚡ <i>Pool liquidity pumped & odds optimized!</i>"
                        )
                        for uid in self._telegram._get_recipients():
                            await self._telegram._send(uid, msg)

            except asyncio.CancelledError:
                break
            except Exception as e_main:
                logger.debug(f"Cross-gamble loop tick notice: {e_main}")

            await asyncio.sleep(4)

    async def _memory_trim_loop(self):
        """Active memory optimizer for 512MB RAM environments. Frees unmapped heap pages to OS."""
        import gc
        import ctypes
        libc = None
        try:
            libc = ctypes.CDLL("libc.so.6")
        except Exception:
            pass
        while not self._is_stopped:
            await asyncio.sleep(45)
            gc.collect()
            if libc and hasattr(libc, "malloc_trim"):
                try:
                    libc.malloc_trim(0)
                except Exception:
                    pass

    async def _auto_redeem_scanner_loop(self):
        """Periodic background scanner checking target rewards across all streamers."""
        logger.info("🎁 Background Auto-Redeem Scanner active: checking target rewards every 10m")
        await asyncio.sleep(20)
        while not self._is_stopped:
            try:
                for worker in list(self.workers):
                    if not worker.token_valid or not getattr(worker, "auto_redeem", None) or not worker.auto_redeem.enabled:
                        continue
                    for streamer in list(self.common_streamers):
                        try:
                            pts = await asyncio.to_thread(worker._get_points_checker().get_amount, streamer, worker.token)
                            if pts and pts > 0:
                                st = worker.state.streamers.get(streamer)
                                if st:
                                    st.points = pts
                                await worker._auto_redeem_check(streamer, pts)
                        except Exception as e_s:
                            logger.debug(f"[{worker.alias}] Scanner check for {streamer} skipped: {e_s}")
                        await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Auto-redeem scanner loop error: {e}")
            await asyncio.sleep(600)

    def get_all_status(self) -> List[dict]:
        return [w.get_status() for w in self.workers]

    def get_total_points(self) -> int:
        total = 0
        for w in self.workers:
            for s in w.state.streamers.values():
                total += (s.points or 0)
        return total

    def get_all_streamers_flat(self) -> List[str]:
        seen = set()
        result = []
        for w in self.workers:
            for s in w.state.streamer_order:
                if s not in seen:
                    seen.add(s)
                    result.append(s)
        return result

    def is_any_streamer_live(self) -> bool:
        for worker in self.workers:
            for s in worker.state.streamers.values():
                if s.is_online or s.is_watching:
                    return True
        return False

    def get_matched_hedge_amount(self, streamer: str, target_bet: int) -> int:
        """
        Cross-account hedging stabilizer:
        Finds the balanced stake across all active accounts watching this streamer
        so that Group A and Group B bet amounts are balanced, preventing one account
        from suffering a mismatched loss.
        """
        active_balances = []
        for w in self.workers:
            st = w.state.streamers.get(streamer)
            if st and st.is_watching and (st.points or 0) > 0:
                active_balances.append(st.points)

        if not active_balances or len(active_balances) < 2:
            return target_bet

        min_balance = min(active_balances)
        safe_max_hedge = int(min_balance * 0.90)
        return max(10, min(target_bet, safe_max_hedge)) if safe_max_hedge > 10 else target_bet

    def get_dutching_hedge_stake(
        self,
        streamer: str,
        worker_index: int,
        my_odds: float,
        opponent_odds: float,
        target_bet: int
    ) -> int:
        """
        True Asymmetric Dutching Calculator:
        Proportionally sizes stakes so that gross payouts are identical on both sides:
        Stake_my * Odds_my == Stake_opp * Odds_opp
        Neutralizes downside variance from mismatched odds.
        """
        active_balances = []
        for w in self.workers:
            st = w.state.streamers.get(streamer)
            if st and st.is_watching and (st.points or 0) > 0:
                active_balances.append(st.points)

        base_stake = target_bet
        if active_balances and len(active_balances) >= 2:
            min_balance = min(active_balances)
            safe_max_hedge = int(min_balance * 0.90)
            if safe_max_hedge > 10:
                base_stake = min(target_bet, safe_max_hedge)

        if my_odds > 1.0 and opponent_odds > 1.0:
            scale = (2.0 * opponent_odds) / (my_odds + opponent_odds)
            dutch_bet = int(base_stake * scale)
            return max(10, dutch_bet)
        return max(10, base_stake)

    def record_consensus_outcome(self, streamer: str, did_consensus_win: bool):
        record_consensus_outcome_to_session(streamer, did_consensus_win)

    def get_streamer_profile(self, streamer: str) -> dict:
        return get_streamer_profile_from_session(streamer)

    def get_live_streamers_summary(self) -> dict:
        live = []
        offline = []
        for worker in self.workers:
            for name, s in worker.state.streamers.items():
                if s.is_online or s.is_watching:
                    live.append({
                        "name": name,
                        "account": worker.alias,
                        "stream_id": s.stream_id,
                        "points": s.points
                    })
                else:
                    offline.append({
                        "name": name,
                        "account": worker.alias
                    })
        return {
            "any_live": len(live) > 0,
            "live_count": len(live),
            "live": live,
            "offline": offline
        }

    async def follow_all(self) -> dict:
        """Ensures all accounts follow all their assigned streamers on Kick."""
        summary = {}
        for worker in self.workers:
            summary[worker.alias] = await worker.follow_all_streamers()
        return summary

    @staticmethod
    def _validate_kick_token(token: str, streamer: str = "binks69") -> tuple[bool, str]:
        """Automatic token checker that validates token against Kick API."""
        try:
            token = clean_token(token)
            if not token:
                return False, "Token is empty."

            from curl_cffi import requests
            s = requests.Session(impersonate="chrome120")
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}/",
            }
            resp = s.get(
                f"https://kick.com/api/v2/channels/{streamer}/points",
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 200:
                return True, "OK"
            if resp.status_code == 401:
                return False, "401 Unauthenticated (Invalid or expired Kick token)."
            if resp.status_code == 403:
                logger.warning(f"Validation encountered 403 for {streamer}, accepting token anyway.")
                return True, "Cloudflare challenge encountered (accepted)."
            return True, f"HTTP {resp.status_code}"
        except Exception as e:
            logger.warning(f"Kick token verification error: {e}")
            return True, "Check skipped due to network error"

    @staticmethod
    def _validate_streamer_exists(streamer: str) -> tuple[bool, str]:
        """Validates that a streamer channel exists on Kick."""
        try:
            from curl_cffi import requests
            s = requests.Session(impersonate="chrome120")
            resp = s.get(
                f"https://kick.com/api/v2/channels/{streamer}",
                timeout=15,
            )
            if resp.status_code == 200:
                return True, "OK"
            if resp.status_code == 404:
                return False, f"Channel '{streamer}' does not exist on Kick."
            return True, f"HTTP {resp.status_code}"
        except Exception as e:
            logger.warning(f"Kick streamer validation warning for {streamer}: {e}")
            return True, "Network check skipped"

    async def add_streamer(self, streamer_name: str) -> tuple[bool, str]:
        """Dynamically add a streamer to the common list and assign to all account workers."""
        clean = str(streamer_name).strip().lower().lstrip("@")
        clean = clean.rstrip("/").split("/")[-1]

        if not clean:
            return False, "Streamer name cannot be empty."

        if clean in self.common_streamers:
            return False, f"Streamer '{clean}' is already in the common streamer list."

        valid, msg = await asyncio.to_thread(self._validate_streamer_exists, clean)
        if not valid:
            return False, msg

        # Add to common list
        self.common_streamers.append(clean)
        self.config["Streamers"] = list(self.common_streamers)

        # Update persistent storage
        try:
            config_path = "config.json"
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg_data = json.load(f)
                cfg_data["Streamers"] = list(self.common_streamers)
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(cfg_data, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Persisted streamer '{clean}' to config.json")
            if os.environ.get("CONFIG_JSON"):
                try:
                    cfg_env = json.loads(os.environ["CONFIG_JSON"])
                    cfg_env["Streamers"] = list(self.common_streamers)
                    os.environ["CONFIG_JSON"] = json.dumps(cfg_env)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error persisting streamer '{clean}': {e}")

        # Dynamically add to all workers and follow channel on all accounts
        for worker in self.workers:
            worker.add_streamer(clean)
            asyncio.create_task(asyncio.to_thread(worker.follow_streamer, clean))

        # Check online status across workers
        for worker in self.workers:
            asyncio.create_task(worker._check_all_online())

        logger.info(f"➕ Streamer '{clean}' added to all {len(self.workers)} accounts.")
        return True, f"Streamer '{clean}' successfully added to all accounts and followed!"

    async def remove_streamer(self, streamer_name: str) -> tuple[bool, str]:
        """Dynamically remove a streamer from the common list and all account workers."""
        clean = str(streamer_name).strip().lower().lstrip("@")
        clean = clean.rstrip("/").split("/")[-1]

        if not clean:
            return False, "Streamer name cannot be empty."

        if clean not in self.common_streamers:
            # Check if any worker is tracking it anyway
            tracked_anywhere = any(clean in w.state.streamers for w in self.workers)
            if not tracked_anywhere:
                return False, f"Streamer '{clean}' is not in the streamer list."

        # Remove from common list
        if clean in self.common_streamers:
            self.common_streamers.remove(clean)
        self.config["Streamers"] = list(self.common_streamers)

        # Update persistent storage
        try:
            config_path = "config.json"
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg_data = json.load(f)
                cfg_data["Streamers"] = list(self.common_streamers)
                if "Predictions" in cfg_data and "stop_targets" in cfg_data["Predictions"]:
                    cfg_data["Predictions"]["stop_targets"].pop(clean, None)
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(cfg_data, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Removed streamer '{clean}' from config.json")
            if os.environ.get("CONFIG_JSON"):
                try:
                    cfg_env = json.loads(os.environ["CONFIG_JSON"])
                    cfg_env["Streamers"] = list(self.common_streamers)
                    if "Predictions" in cfg_env and "stop_targets" in cfg_env["Predictions"]:
                        cfg_env["Predictions"]["stop_targets"].pop(clean, None)
                    os.environ["CONFIG_JSON"] = json.dumps(cfg_env)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error removing streamer '{clean}' from persistent config: {e}")

        # Remove from session_state.json if present
        try:
            saved_state = _load_session_state_file()
            changed = False
            for alias_key in list(saved_state.keys()):
                if isinstance(saved_state[alias_key], dict) and clean in saved_state[alias_key]:
                    del saved_state[alias_key][clean]
                    changed = True
            if "streamer_profiles" in saved_state and clean in saved_state["streamer_profiles"]:
                del saved_state["streamer_profiles"][clean]
                changed = True
            if changed:
                _save_session_state_file(saved_state)
        except Exception as e:
            logger.warning(f"Error cleaning session state for {clean}: {e}")

        # Invalidate live stream cache
        self._live_stream_cache.pop(clean, None)

        # Remove from all workers
        for worker in self.workers:
            await worker.remove_streamer(clean)

        logger.info(f"🗑️ Streamer '{clean}' removed from all {len(self.workers)} accounts.")
        return True, f"Streamer '{clean}' successfully removed from all accounts!"

    async def add_account(
        self,
        alias: str,
        token: str,
        proxy: Optional[str] = None,
        max_concurrent: int = 10,
        streamers: Optional[List[str]] = None,
    ) -> tuple[bool, str]:
        """Dynamically add an account, validate it, persist it, and start mining."""
        clean_alias = str(alias).strip()
        clean_token_str = clean_token(token)

        if not clean_alias or not clean_token_str:
            return False, "Alias and Token cannot be empty."

        for w in self.workers:
            if w.alias.strip().lower() == clean_alias.lower():
                return False, f"Account with alias '{clean_alias}' already exists."

        # Fetch streamers from common list if not explicitly provided
        target_streamers = list(streamers) if streamers else list(self.common_streamers)
        if not target_streamers:
            target_streamers = self.config.get("Streamers") or [
                "soulaman", "klurge", "binks69", "harshkhelraay",
                "hydraflick", "soulzeref", "pinkcess", "s8ul_sid", "nonstopgaming"
            ]
            if not self.common_streamers:
                self.common_streamers = [s.strip().lower() for s in target_streamers if s.strip()]

        test_channel = target_streamers[0] if target_streamers else "binks69"
        valid, val_msg = await asyncio.to_thread(self._validate_kick_token, clean_token_str, test_channel)
        if not valid:
            return False, val_msg

        new_account_cfg = {
            "alias": clean_alias,
            "token": clean_token_str,
            "proxy": proxy,
            "max_concurrent": max_concurrent,
        }
        if streamers and streamers != self.common_streamers:
            new_account_cfg["streamers"] = streamers

        # Update in-memory config
        self.config.setdefault("Accounts", []).append(new_account_cfg)

        # Update persistent storage
        try:
            config_path = "config.json"
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg_data = json.load(f)
                cfg_data.setdefault("Accounts", []).append(new_account_cfg)
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(cfg_data, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Persisted new account [{clean_alias}] to config.json")
            if os.environ.get("CONFIG_JSON"):
                try:
                    cfg_env = json.loads(os.environ["CONFIG_JSON"])
                    cfg_env.setdefault("Accounts", []).append(new_account_cfg)
                    os.environ["CONFIG_JSON"] = json.dumps(cfg_env)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error persisting account [{clean_alias}]: {e}")

        # Instantiate worker
        total_w = len(self.workers) + 1
        worker = AccountWorker(
            new_account_cfg,
            global_proxy=self.global_proxy,
            check_interval=self.check_interval,
            reconnect_cooldown=self.reconnect_cooldown,
            stagger_min=self.stagger_min,
            stagger_max=self.stagger_max,
            manager=self,
            prediction_cfg=self.prediction_cfg,
            worker_index=len(self.workers),
            total_workers=total_w,
        )

        if self._discord:
            worker.set_discord(self._discord)
        if self._telegram:
            worker.set_telegram(self._telegram)

        self.workers.append(worker)

        # Launch worker
        task = asyncio.create_task(worker.start())
        self._tasks.append(task)

        # Launch background streamer following
        asyncio.create_task(worker.follow_all_streamers())

        logger.info(f"➕ New account [{clean_alias}] added and worker started!")
        return True, "Account added and worker started successfully."
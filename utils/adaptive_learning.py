import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
try:
    from loguru import logger
except ImportError:
    import logging
    import sys
    logger = logging.getLogger("adaptive_learning")
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

LEARNING_PROFILE_FILE = "learning_profile.json"


class AdaptiveLearningEngine:
    """
    High-Profit Self-Learning Prediction Engine for Kick Miner.
    Features:
    - Normal full-sized compounding bets on every prediction (no pauses, no micro-bets).
    - Smart Money vs. Crowd Divergence tracking per streamer.
    - Fade-The-Public (Jinx / Meme Chat) contrarian exploitation.
    - Odds bracket sweet-spot optimization (maximizing point gains).
    - Multi-account hedging synergy when applicable.
    - Win-streak momentum compounding.
    """

    _instance = None
    _lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(AdaptiveLearningEngine, cls).__new__(cls)
                    cls._instance._init_engine()
        return cls._instance

    def _init_engine(self):
        self.file_path = LEARNING_PROFILE_FILE
        self.profiles: Dict[str, Any] = {
            "version": 3,
            "global": {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "net_profit": 0,
                "win_rate": 0.0,
            },
            "streamers": {},
        }
        self.load_profiles()

    def load_profiles(self):
        with self._lock:
            if os.path.exists(self.file_path):
                try:
                    with open(self.file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, dict) and "streamers" in data:
                            self.profiles = data
                            logger.info(
                                f"🧠 Self-Learning Engine: Loaded knowledge base for "
                                f"{len(self.profiles['streamers'])} streamer(s)"
                            )
                except Exception as e:
                    logger.warning(f"🧠 Could not load {self.file_path}: {e}")

    def save_profiles(self):
        with self._lock:
            try:
                with open(self.file_path, "w", encoding="utf-8") as f:
                    json.dump(self.profiles, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"🧠 Failed to save {self.file_path}: {e}")

    @staticmethod
    def classify_title(title: str) -> str:
        t = (title or "").lower()
        if any(k in t for k in ['kill', 'kills']):
            return 'kills'
        if any(k in t for k in ['round', 'rounds', 'half', '7-5', '8-4', 'first to', 'ot', 'point', 'opps get', 'opp get']):
            return 'rounds'
        if any(k in t for k in ['top', 'position', 'posi', 'finish']):
            return 'placement'
        if any(k in t for k in ['win', 'winner', 'jeet', 'booyah', 'chicken', 'who wins']):
            return 'match_win'
        return 'general'

    def get_streamer_profile(self, streamer: str) -> Dict[str, Any]:
        with self._lock:
            return self.profiles["streamers"].get(streamer.lower(), {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "consensus_wins": 0,
                "consensus_losses": 0,
                "whale_wins": 0,
                "whale_losses": 0,
                "contrarian_wins": 0,
                "contrarian_losses": 0,
                "consecutive_wins": 0,
                "consecutive_losses": 0,
                "net_profit": 0,
                "win_rate": 0.0,
                "consensus_accuracy": 0.70,
                "whale_accuracy": 0.65,
                "odds_brackets": {
                    "low": {"wins": 0, "losses": 0},   # < 1.35x
                    "mid": {"wins": 0, "losses": 0},   # 1.35x - 2.10x (sweet spot)
                    "high": {"wins": 0, "losses": 0},  # > 2.10x
                },
                "categories": {},
            })

    def record_bet_result(
        self,
        streamer: str,
        is_consensus: bool,
        did_win: bool,
        bet_amount: int,
        net_profit: int,
        odds: float = 1.0,
        is_whale: bool = False,
        prediction_title: str = "",
    ):
        """Updates comprehensive learning profile when a prediction resolves."""
        streamer_key = streamer.lower()
        now_str = datetime.now().isoformat()
        category = self.classify_title(prediction_title)

        with self._lock:
            st_prof = self.profiles["streamers"].setdefault(streamer_key, {
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "consensus_wins": 0,
                "consensus_losses": 0,
                "whale_wins": 0,
                "whale_losses": 0,
                "contrarian_wins": 0,
                "contrarian_losses": 0,
                "consecutive_wins": 0,
                "consecutive_losses": 0,
                "net_profit": 0,
                "win_rate": 0.0,
                "consensus_accuracy": 0.70,
                "whale_accuracy": 0.65,
                "odds_brackets": {
                    "low": {"wins": 0, "losses": 0},
                    "mid": {"wins": 0, "losses": 0},
                    "high": {"wins": 0, "losses": 0},
                },
                "categories": {},
                "last_updated": now_str,
            })

            st_prof["total_bets"] += 1
            st_prof["net_profit"] += net_profit
            st_prof["last_updated"] = now_str

            # Odds bracket tracking
            bracket = "low" if odds < 1.35 else ("mid" if odds <= 2.10 else "high")
            st_prof.setdefault("odds_brackets", {
                "low": {"wins": 0, "losses": 0},
                "mid": {"wins": 0, "losses": 0},
                "high": {"wins": 0, "losses": 0},
            })

            # Category tracking
            st_prof.setdefault("categories", {})
            cat_data = st_prof["categories"].setdefault(category, {
                "wins": 0,
                "losses": 0,
                "net_profit": 0,
                "accuracy": 0.70,
            })
            cat_data["net_profit"] += net_profit

            if did_win:
                st_prof["wins"] += 1
                st_prof["consecutive_wins"] = st_prof.get("consecutive_wins", 0) + 1
                st_prof["consecutive_losses"] = 0
                st_prof["odds_brackets"][bracket]["wins"] += 1
                cat_data["wins"] += 1

                if is_consensus:
                    st_prof["consensus_wins"] += 1
                else:
                    st_prof["contrarian_wins"] += 1
                if is_whale:
                    st_prof["whale_wins"] = st_prof.get("whale_wins", 0) + 1
            else:
                st_prof["losses"] += 1
                st_prof["consecutive_losses"] = st_prof.get("consecutive_losses", 0) + 1
                st_prof["consecutive_wins"] = 0
                st_prof["odds_brackets"][bracket]["losses"] += 1
                cat_data["losses"] += 1

                if is_consensus:
                    st_prof["consensus_losses"] += 1
                else:
                    st_prof["contrarian_losses"] += 1
                if is_whale:
                    st_prof["whale_losses"] = st_prof.get("whale_losses", 0) + 1

            # Update category Bayesian accuracy (Prior: 5 wins / 2 losses)
            c_tot = cat_data["wins"] + cat_data["losses"]
            cat_data["accuracy"] = round((5.0 + cat_data["wins"]) / (7.0 + c_tot), 3)

            # Update overall win rate
            st_total = st_prof["wins"] + st_prof["losses"]
            st_prof["win_rate"] = round(st_prof["wins"] / max(1, st_total), 3)

            # Bayesian update for consensus accuracy (Prior: 7 wins / 3 losses)
            c_wins = st_prof["consensus_wins"]
            c_losses = st_prof["consensus_losses"]
            st_prof["consensus_accuracy"] = round((7.0 + c_wins) / (10.0 + c_wins + c_losses), 3)

            # Bayesian update for whale / smart money accuracy (Prior: 6.5 wins / 3.5 losses)
            w_wins = st_prof.get("whale_wins", 0)
            w_losses = st_prof.get("whale_losses", 0)
            st_prof["whale_accuracy"] = round((6.5 + w_wins) / (10.0 + w_wins + w_losses), 3)

            # Global stats
            gl = self.profiles["global"]
            gl["total_bets"] += 1
            gl["net_profit"] += net_profit
            if did_win:
                gl["wins"] += 1
            else:
                gl["losses"] += 1
            gl_total = gl["wins"] + gl["losses"]
            gl["win_rate"] = round(gl["wins"] / max(1, gl_total), 3)

            # Global category stats
            gl_cats = gl.setdefault("categories", {})
            gl_c = gl_cats.setdefault(category, {"wins": 0, "losses": 0, "net_profit": 0, "accuracy": 0.70})
            gl_c["net_profit"] += net_profit
            if did_win:
                gl_c["wins"] += 1
            else:
                gl_c["losses"] += 1
            gl_c["accuracy"] = round((5.0 + gl_c["wins"]) / (7.0 + gl_c["wins"] + gl_c["losses"]), 3)

        self.save_profiles()
        logger.info(
            f"🧠 [Self-Learning] {streamer} [{category}]: {st_prof['wins']}W/{st_prof['losses']}L "
            f"(Consensus: {st_prof['consensus_accuracy']*100:.1f}%, Cat: {cat_data['accuracy']*100:.1f}%, "
            f"Net: {st_prof['net_profit']:+,} pts)"
        )

    def evaluate_adaptive_bet(
        self,
        streamer: str,
        outcomes: List[Dict[str, Any]],
        prediction_title: str = "",
        current_balance: int = 0,
        aggressive_threshold: int = 15000,
        moderate_threshold: int = 30000,
        aggressive_stages: Tuple[int, int, int] = (50, 35, 20),
        moderate_stages: Tuple[int, int, int] = (25, 18, 12),
        safe_stages: Tuple[int, int, int] = (14, 10, 6),
        min_odds_floor: float = 1.15,
        account_index: int = 0,
        total_accounts: int = 1,
        hedging_enabled: bool = True,
        **kwargs,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, Optional[str]]:
        """
        High-profit adaptive strategy selector:
        - NEVER pauses and NEVER micro-bets.
        - Sizes bets using standard stage percentages (e.g. 50% / 35% / 25%).
        - Selects outcomes via Category Intelligence (kills, placement, match_win, rounds),
          Smart Money (whales), Fade-The-Public, or Odds Sweet-Spot optimization.
        """
        if not outcomes:
            return None, 0, "", "No outcomes available"

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        total_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)

        prof = self.get_streamer_profile(streamer)
        p_consensus = prof.get("consensus_accuracy", 0.70)
        p_whale = prof.get("whale_accuracy", 0.65)
        tracked_bets = prof.get("total_bets", 0)
        consec_wins = prof.get("consecutive_wins", 0)

        # Category intelligence
        category = self.classify_title(prediction_title)
        cat_stats = prof.get("categories", {}).get(category, {})
        cat_total = cat_stats.get("wins", 0) + cat_stats.get("losses", 0)
        cat_acc = cat_stats.get("accuracy", p_consensus)

        # 1. Determine Normal Bet Sizing (Always normal, never micro)
        if current_balance < aggressive_threshold:
            # Aggressive Phase: fast compounding
            stage_p = aggressive_stages
            phase_label = "🔥 Aggressive"
        elif current_balance < moderate_threshold:
            # Moderate Phase: balanced growth
            stage_p = moderate_stages
            phase_label = "⚡ Moderate"
        else:
            # Safe Phase: fortress capital growth
            stage_p = safe_stages
            phase_label = "🛡️ Fortress"

        # Win Streak Accelerator: if on a 2+ streak, take the top stage!
        if consec_wins >= 2:
            normal_pct = stage_p[0]
            streak_note = f" [🔥 Win Streak {consec_wins}x Accelerated]"
        else:
            normal_pct = stage_p[0] if (p_consensus >= 0.68 or cat_acc >= 0.70) else stage_p[1]
            streak_note = ""

        # Identify crowd leader (by voters) and whale leader (by points)
        crowd_leader = max(outcomes, key=lambda o: (o.get("vote_count", 0) or 0, o.get("total_vote_amount", 0) or 0))
        whale_leader = max(outcomes, key=lambda o: (o.get("total_vote_amount", 0) or 0, o.get("vote_count", 0) or 0))
        underdog = min(outcomes, key=lambda o: (o.get("vote_count", 0) or 0, o.get("total_vote_amount", 0) or 0))

        crowd_rate = float(crowd_leader.get("return_rate", 1.0) or 1.0)
        whale_rate = float(whale_leader.get("return_rate", 1.0) or 1.0)
        underdog_rate = float(underdog.get("return_rate", 1.0) or 1.0)

        c_voters = crowd_leader.get("vote_count", 0) or 0
        consensus_pct = (c_voters / total_voters * 100.0) if total_voters > 0 else 50.0

        w_pts = whale_leader.get("total_vote_amount", 0) or 0
        whale_share = (w_pts / total_points * 100.0) if total_points > 0 else 50.0

        # --- STRATEGY 1: Category Master Lock (High Conviction) ---
        # When channel has proven category dominance (e.g. placement @ 85%+, kills @ 65%+)
        if cat_total >= 3 and cat_acc >= 0.70 and crowd_rate >= min_odds_floor:
            cat_pct = stage_p[0]
            info = (
                f"🧠 [Category Master: {category.upper()}] Channel accuracy {cat_acc*100:.0f}% "
                f"({cat_stats.get('wins',0)}W/{cat_stats.get('losses',0)}L) -> "
                f"Locking '{crowd_leader.get('title')}' @ {crowd_rate:.2f}x ({phase_label} Top {cat_pct}%){streak_note}"
            )
            return crowd_leader, cat_pct, info, None

        # --- STRATEGY 2: Category Fade-The-Public (Contrarian Trap Avoidance) ---
        # If crowd consensus in this specific category is historically terrible (< 44%)
        if cat_total >= 3 and cat_acc < 0.44 and len(outcomes) == 2:
            if underdog_rate >= min_odds_floor:
                info = (
                    f"🧠 [Category Contrarian: {category.upper()}] Crowd accuracy in {category} is {cat_acc*100:.0f}% -> "
                    f"Fading Crowd & Backing Underdog '{underdog.get('title')}' @ {underdog_rate:.2f}x "
                    f"({phase_label} Normal {normal_pct}%){streak_note}"
                )
                return underdog, normal_pct, info, None

        # --- STRATEGY 1: Multi-Account Hedging (Balanced 2x Market) ---
        if hedging_enabled and total_accounts >= 2 and len(outcomes) == 2:
            r1 = float(outcomes[0].get("return_rate", 1.0) or 1.0)
            r2 = float(outcomes[1].get("return_rate", 1.0) or 1.0)
            if 1.65 <= r1 <= 2.65 and 1.65 <= r2 <= 2.65:
                chosen = outcomes[0] if (account_index % 2 == 0) else outcomes[1]
                rate = float(chosen.get("return_rate", 1.0) or 1.0)
                info = (
                    f"🧠 [Smart Hedging] 2x Market ({r1:.2f}x vs {r2:.2f}x) -> "
                    f"Account {account_index+1} backing '{chosen.get('title')}' @ {rate:.2f}x (Normal {normal_pct}%)"
                )
                return chosen, normal_pct, info, None

        # --- STRATEGY 2: Fade-The-Public (Contrarian Jinx Exploiter) ---
        # If chat accuracy is proven bad (< 42%) across 4+ bets, bet normal size on underdog!
        if tracked_bets >= 4 and p_consensus < 0.42 and len(outcomes) == 2:
            if underdog_rate >= min_odds_floor:
                info = (
                    f"🧠 [Fade-The-Public] Chat Delusion ({p_consensus*100:.0f}% acc) -> "
                    f"High-Yield Bet on Underdog '{underdog.get('title')}' @ {underdog_rate:.2f}x "
                    f"({phase_label} Normal {normal_pct}%){streak_note}"
                )
                return underdog, normal_pct, info, None

        # --- STRATEGY 3: Smart Money (Whale Divergence) ---
        # When points volume heavily diverges from voter count, and whale accuracy is higher
        if (
            whale_leader.get("id") != crowd_leader.get("id")
            and whale_share >= 60.0
            and p_whale >= p_consensus
            and whale_rate >= min_odds_floor
        ):
            info = (
                f"🧠 [Smart Money Whale] Whales backing '{whale_leader.get('title')}' ({whale_share:.0f}% volume) @ {whale_rate:.2f}x -> "
                f"Follow Smart Money ({phase_label} Normal {normal_pct}%){streak_note}"
            )
            return whale_leader, normal_pct, info, None

        # --- STRATEGY 4: Odds Sweet-Spot Optimizer ---
        # Check if mid-odds bracket (1.35x - 2.10x) has proven superior win rate for this streamer
        brackets = prof.get("odds_brackets", {})
        mid_b = brackets.get("mid", {"wins": 0, "losses": 0})
        mid_total = mid_b["wins"] + mid_b["losses"]
        mid_acc = (mid_b["wins"] / mid_total) if mid_total >= 3 else 0.70

        if 1.35 <= crowd_rate <= 2.10 and mid_acc >= 0.65:
            info = (
                f"🧠 [Golden Sweet-Spot] Channel Sweet Spot ({mid_acc*100:.0f}% win rate @ {crowd_rate:.2f}x) -> "
                f"'{crowd_leader.get('title')}' ({phase_label} Normal {normal_pct}%){streak_note}"
            )
            return crowd_leader, normal_pct, info, None

        # --- STRATEGY 5: Super-Consensus Crowd Lock ---
        # If crowd agreement is solid (>= 60%)
        if consensus_pct >= 60.0:
            info = (
                f"🧠 [Consensus Lock] {consensus_pct:.1f}% Crowd agreement on '{crowd_leader.get('title')}' @ {crowd_rate:.2f}x -> "
                f"({phase_label} Normal {normal_pct}%){streak_note}"
            )
            return crowd_leader, normal_pct, info, None

        # --- Default: Best voter backed outcome ---
        info = (
            f"🧠 [Adaptive Standard] Backing '{crowd_leader.get('title')}' @ {crowd_rate:.2f}x "
            f"({phase_label} Normal {normal_pct}%){streak_note}"
        )
        return crowd_leader, normal_pct, info, None

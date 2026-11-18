from curl_cffi import requests
import json
import time
import random
from typing import Optional, Dict, Any, Tuple
from loguru import logger
from datetime import datetime, timezone


class KickPrediction:
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy
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
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://kick.com",
            "Sec-Ch-Ua": (
                '"Chromium";v="120", "Google Chrome";v="120", '
                '"Not?A_Brand";v="99"'
            ),
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Connection": "keep-alive",
        })
        self._initialized = False

    def _ensure_session(self):
        if self._initialized:
            return
        try:
            resp = self.session.get("https://kick.com", timeout=15)
            for name, value in {
                "showMatureContent": "true",
                "USER_LOCALE": "en",
            }.items():
                self.session.cookies.set(name, value, domain="kick.com")
            time.sleep(random.uniform(0.3, 0.8))
            self._initialized = True
        except Exception as e:
            logger.error(f"Prediction session init error: {e}")

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def get_latest_prediction(self, channel_slug: str, token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest prediction for the given channel.
        Returns a dict containing 'prediction' and optional 'user_vote', or None if failed/none.
        """
        self._ensure_session()
        headers = {
            "Referer": f"https://kick.com/{channel_slug}/",
            "Origin": "https://kick.com",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            resp = self.session.get(
                f"https://kick.com/api/v2/channels/{channel_slug}/predictions/latest",
                headers=headers,
                timeout=12,
            )

            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.debug(f"Predictions latest [{channel_slug}] HTTP {resp.status_code}")
                return None

            data = json.loads(resp.content.decode("utf-8", errors="ignore"))
            inner = data.get("data")
            if isinstance(inner, dict):
                return inner
            return None

        except Exception as e:
            logger.error(f"Error getting latest prediction for {channel_slug}: {e}")
            return None

    def vote_prediction(
        self,
        channel_slug: str,
        outcome_id: str,
        amount: int,
        token: str,
    ) -> Tuple[bool, Any]:
        """
        Submits a prediction bet.
        """
        self._ensure_session()
        headers = {
            "Referer": f"https://kick.com/{channel_slug}/",
            "Origin": "https://kick.com",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "amount": amount,
            "outcome_id": outcome_id,
        }

        try:
            resp = self.session.post(
                f"https://kick.com/api/v2/channels/{channel_slug}/predictions/vote",
                headers=headers,
                json=payload,
                timeout=15,
            )

            if resp.status_code in (200, 201):
                try:
                    res_data = json.loads(resp.content.decode("utf-8", errors="ignore"))
                    return True, res_data
                except Exception:
                    return True, resp.text

            raw_err = resp.content.decode("utf-8", errors="ignore")
            logger.warning(
                f"Prediction vote failed for {channel_slug} (HTTP {resp.status_code}): {raw_err}"
            )
            return False, raw_err

        except Exception as e:
            logger.error(f"Exception placing vote for {channel_slug}: {e}")
            return False, str(e)

    @staticmethod
    def select_smart_outcome(
        outcomes: list,
        min_voters: int = 10,
        min_odds: float = 1.15,
        max_odds: float = 3.0,
        min_consensus_pct: float = 55.0,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Smart filter for minimal chance of loss:
        1. Checks total liquidity/voters: Requires at least min_voters.
        2. Selects the outcome with highest unique voters (crowd consensus).
        3. Validates consensus dominance: Must have at least min_consensus_pct of total voters.
        4. Validates odds safety: Return rate must be between min_odds and max_odds.
        Returns: (best_outcome, skip_reason)
        """
        if not outcomes:
            return None, "No outcomes available"

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        if total_voters < min_voters:
            return None, f"Too few voters ({total_voters}/{min_voters} required) - ghost prediction"

        best = max(
            outcomes,
            key=lambda o: (
                o.get("vote_count", 0) or 0,
                o.get("total_vote_amount", 0) or 0,
            ),
        )

        best_voters = best.get("vote_count", 0) or 0
        consensus_pct = (best_voters / total_voters * 100.0) if total_voters > 0 else 0

        if consensus_pct < min_consensus_pct:
            return None, f"No clear crowd consensus ({consensus_pct:.1f}% on '{best.get('title')}', min {min_consensus_pct}%) - 50/50 coinflip"

        rate = best.get("return_rate", 1.0) or 1.0
        if rate < min_odds:
            return None, f"Odds too low ({rate:.2f}x < {min_odds:.2f}x min) - reward too small for risk"

        if rate > max_odds:
            return None, f"Odds too high ({rate:.2f}x > {max_odds:.2f}x max) - high risk longshot"

        return best, None

    @staticmethod
    def select_voter_focused_outcome(
        outcomes: list,
        stage1_pct: int = 65,
        stage2_pct: int = 45,
        stage3_pct: int = 30,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, float]:
        """
        Pure voter-focused system (3 stages, NEVER skips):
        Always bets on whichever outcome has the higher number of unique voters.
        - Stage 1 (>= 70% of voters): Bets stage1_pct (default 65%) for aggressive compound growth.
        - Stage 2 (55% - 69.9% of voters): Bets stage2_pct (default 45%) for strong compounding.
        - Stage 3 (< 55% of voters): Bets stage3_pct (default 30%) on the higher voter side.
        """
        if not outcomes:
            return None, stage3_pct, "No outcomes", 0.0

        # Always pick the outcome with the highest number of unique voters (points as tiebreaker)
        best = max(
            outcomes,
            key=lambda o: (
                o.get("vote_count", 0) or 0,
                o.get("total_vote_amount", 0) or 0,
            ),
        )

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        best_voters = best.get("vote_count", 0) or 0
        consensus_pct = (best_voters / total_voters * 100.0) if total_voters > 0 else 50.0

        if consensus_pct >= 70.0:
            pct = stage1_pct
            stage = f"Stage 1 (Super Majority {consensus_pct:.1f}% voters) -> Bet {pct}%"
        elif consensus_pct >= 55.0:
            pct = stage2_pct
            stage = f"Stage 2 (Solid Majority {consensus_pct:.1f}% voters) -> Bet {pct}%"
        else:
            pct = stage3_pct
            stage = f"Stage 3 (Narrow Majority {consensus_pct:.1f}% voters) -> Defensive Bet {pct}%"

        return best, pct, stage, consensus_pct

    @staticmethod
    def select_minimal_risk_outcome(
        outcomes: list,
        current_balance: int = 0,
        aggressive_threshold: int = 15000,
        moderate_threshold: int = 30000,
        **kwargs,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, Optional[str]]:
        """
        Odds-Aware High-Profit Engine (NO SKIPS):
        1. 💎 Golden Sweet Spot (58%-74% consensus + 1.40x-2.25x odds): Max high-yield compounding (45%).
        2. 🐢 Heavy Favorite Trap (>=75% consensus but <1.35x odds): Sized down (22%) to prevent risking the farm for pennies.
        3. 🚀 High Confidence Lock (>=75% consensus + >=1.35x odds): Heavy bet (50%).
        4. 🎲 Coinflip Defense (<58% consensus): Cushion bet (15%) so 50/50 losses never wipe you out.
        - Automatically transitions between Aggressive (<15k), Moderate (15k-30k), and Fortress Safe (>=30k).
        - Always bets on majority unique voters (points as tiebreaker).
        """
        if not outcomes:
            return None, 0, "", "No outcomes available"

        # Always select the outcome with the highest number of unique voters (points as tiebreaker)
        best = max(
            outcomes,
            key=lambda o: (
                o.get("vote_count", 0) or 0,
                o.get("total_vote_amount", 0) or 0,
            ),
        )

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        best_voters = best.get("vote_count", 0) or 0
        consensus_pct = (best_voters / total_voters * 100.0) if total_voters > 0 else 50.0
        rate = best.get("return_rate", 1.0) or 1.0

        # Phase base percentages (Odds-Aware Matrix)
        if current_balance < aggressive_threshold:
            phase_label = f"🔥 Aggressive (<{aggressive_threshold:,} pts)"
            sweet_spot_pct = 45   # 58%-74% majority + 1.40x-2.25x odds (THE SWEET SPOT)
            super_lock_pct = 50   # >=75% majority + good odds (>=1.35x)
            heavy_fav_pct = 22    # >=75% majority but low odds (<1.35x) -> avoid 1.20x trap!
            coinflip_pct = 15     # <58% close match / coinflip cushion
            underdog_pct = 20     # High odds (>2.25x) with majority backing
        elif current_balance < moderate_threshold:
            phase_label = f"⚡ Moderate ({aggressive_threshold:,}-{moderate_threshold:,} pts)"
            sweet_spot_pct = 25
            super_lock_pct = 28
            heavy_fav_pct = 14
            coinflip_pct = 10
            underdog_pct = 12
        else:
            phase_label = f"🛡️ Fortress Safe (≥{moderate_threshold:,} pts)"
            sweet_spot_pct = 12
            super_lock_pct = 14
            heavy_fav_pct = 7
            coinflip_pct = 5
            underdog_pct = 6

        # --- Odds-Aware Value Evaluation ---
        if consensus_pct >= 75.0:
            if rate < 1.35:
                # Heavy favorite but low odds: don't risk the farm for pennies!
                bet_pct = heavy_fav_pct
                scenario_label = f"🐢 Heavy Fav Low-Odds Trap ({consensus_pct:.1f}% voters, {rate:.2f}x) -> Sized Down"
            else:
                # Super majority with good odds (rare whale divergence)
                bet_pct = super_lock_pct
                scenario_label = f"🚀 Super Majority Lock ({consensus_pct:.1f}% voters, {rate:.2f}x) -> Heavy Bet"
        elif consensus_pct >= 58.0:
            if 1.40 <= rate <= 2.25:
                # THE SWEET SPOT: Crowd backing + juicy payout
                bet_pct = sweet_spot_pct
                scenario_label = f"💎 High-Yield Sweet Spot ({consensus_pct:.1f}% voters, {rate:.2f}x) -> Max Profit"
            elif rate < 1.40:
                bet_pct = heavy_fav_pct
                scenario_label = f"📈 Solid Majority Low-Odds ({consensus_pct:.1f}% voters, {rate:.2f}x)"
            else:
                bet_pct = underdog_pct
                scenario_label = f"🎯 High-Odds Value ({consensus_pct:.1f}% voters, {rate:.2f}x)"
        else:
            # Tight 50/50 Coinflip (<58% consensus)
            bet_pct = coinflip_pct
            scenario_label = f"🎲 Coinflip Defense ({consensus_pct:.1f}% voters, {rate:.2f}x) -> Cushioned"

        info = f"{phase_label} {scenario_label} -> Bet {bet_pct}%"
        return best, bet_pct, info, None

    @staticmethod
    def calculate_kelly_bet(consensus_pct: float, return_rate: float, kelly_fraction: float = 0.75, total_voters: int = 0) -> float:
        """
        Calculates optimal bet percentage using the Kelly Criterion formula:
        f* = (b * p - q) / b
        Scales fraction dynamically with crowd sample size if total_voters is provided:
        - N < 15: 0.25x (noise reduction / small sample caution)
        - 15 <= N < 50: 0.50x (balanced edge)
        - N >= 50: 0.85x (high statistical confidence)
        """
        if return_rate <= 1.0 or consensus_pct <= 0:
            return 0.0
        b = return_rate - 1.0
        p = consensus_pct / 100.0
        q = 1.0 - p
        edge = (b * p - q) / b
        if edge <= 0:
            return 0.0
        if total_voters > 0:
            effective_fraction = 0.25 if total_voters < 15 else (0.50 if total_voters < 50 else 0.85)
        else:
            effective_fraction = kelly_fraction
        bet_pct = edge * effective_fraction * 100.0
        return max(5.0, min(bet_pct, 85.0))

    @staticmethod
    def select_kelly_outcome(
        outcomes: list,
        kelly_fraction: float = 0.75,
        min_voters: int = 5,
        **kwargs,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, Optional[str]]:
        """
        Mathematical Kelly Criterion Engine:
        Evaluates each outcome for positive expected value edge and dynamically sizes the bet.
        """
        if not outcomes:
            return None, 0, "", "No outcomes available"

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        if total_voters < min_voters:
            best = max(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0,
                ),
            )
            return best, 15, f"Low liquidity ({total_voters} voters) -> Safety Bet 15%", None

        best_candidate = None
        best_edge = -999.0
        best_pct = 0.0

        for o in outcomes:
            v_count = o.get("vote_count", 0) or 0
            cons = (v_count / total_voters * 100.0) if total_voters > 0 else 50.0
            rate = o.get("return_rate", 1.0) or 1.0
            if rate <= 1.0:
                continue
            b = rate - 1.0
            p = cons / 100.0
            q = 1.0 - p
            edge = (b * p - q) / b
            if edge > best_edge:
                best_edge = edge
                best_candidate = o
                raw_pct = edge * kelly_fraction * 100.0
                best_pct = max(10.0, min(raw_pct, 80.0))

        if not best_candidate or best_edge <= 0:
            best = max(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0,
                ),
            )
            return best, 10, "No positive Kelly edge -> Defensive Bet 10%", None

        cons_pct = ((best_candidate.get("vote_count", 0) or 0) / total_voters * 100.0) if total_voters > 0 else 50.0
        rate = best_candidate.get("return_rate", 1.0) or 1.0
        info = f"📊 Kelly Optimal ({cons_pct:.1f}% consensus, {rate:.2f}x odds, edge={best_edge:.2f}) -> Bet {int(best_pct)}%"
        return best_candidate, int(best_pct), info, None

    @staticmethod
    def select_anti_whale_outcome(
        outcomes: list,
        min_voters: int = 8,
        min_voter_consensus: float = 55.0,
        min_odds: float = 1.35,
        **kwargs,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, Optional[str]]:
        """
        Anti-Whale Value Hunter:
        Identifies outcomes backed by majority of chatters but paying inflated odds due to points distorted by whales.
        """
        if not outcomes:
            return None, 0, "", "No outcomes available"

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        total_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)

        for o in outcomes:
            v_count = o.get("vote_count", 0) or 0
            v_pct = (v_count / total_voters * 100.0) if total_voters > 0 else 0.0
            rate = o.get("return_rate", 1.0) or 1.0
            pts = o.get("total_vote_amount", 0) or 0
            pts_pct = (pts / total_points * 100.0) if total_points > 0 else 0.0

            if total_voters >= min_voters and v_pct >= min_voter_consensus and rate >= min_odds:
                info = f"🐋 Anti-Whale Skew Found! ({v_pct:.1f}% crowd vs {pts_pct:.1f}% pts, Odds {rate:.2f}x) -> Heavy Bet 50%"
                return o, 50, info, None

        best = max(
            outcomes,
            key=lambda o: (
                o.get("vote_count", 0) or 0,
                o.get("total_vote_amount", 0) or 0,
            ),
        )
        return best, 35, "No whale skew detected -> Standard Consensus Bet 35%", None

    @staticmethod
    def select_brain_outcome(
        outcomes: list,
        current_balance: int = 0,
        account_index: int = 0,
        total_accounts: int = 1,
        hedging_enabled: bool = True,
        min_odds_floor: float = 1.20,
        turbo_threshold: int = 3000,
        turbo_pct: int = 90,
        min_voters_threshold: int = 8,
        **kwargs,
    ) -> Tuple[Optional[Dict[str, Any]], int, str, Optional[str]]:
        """
        AI Prediction Brain:
        Coordinates multi-account hedging at ~2x odds, anti-whale exploitation,
        crowd consensus locks, Kelly dynamic edge sizing, and speedrun staging.
        """
        if not outcomes:
            return None, 0, "", "No outcomes available"

        total_voters = sum((o.get("vote_count", 0) or 0) for o in outcomes)
        total_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)

        # Ghost Prediction / Low Liquidity Defense:
        if total_voters < min_voters_threshold:
            best_cand = max(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0,
                ),
            )
            return best_cand, 5, f"👻 Low Liquidity ({total_voters}/{min_voters_threshold} voters) -> Safety Probe Bet 5%", None

        # Dynamic Kelly Fraction calculation based on crowd sample size
        dynamic_kelly_fraction = 0.25 if total_voters < 15 else (0.50 if total_voters < 50 else 0.85)

        # 1. Feature: Late-Lock Odds Velocity & Smart Money Tracker
        smart_money = kwargs.get("smart_money_inflow")
        if smart_money and smart_money.get("outcome_id"):
            sm_id = smart_money["outcome_id"]
            sm_cand = next((o for o in outcomes if o.get("id") == sm_id), None)
            if sm_cand:
                sm_rate = float(sm_cand.get("return_rate", 1.0) or 1.0)
                if sm_rate >= min_odds_floor:
                    sm_pct = turbo_pct if current_balance < turbo_threshold else 45
                    d_odds = smart_money.get("delta_odds", 0.0)
                    v_pct = smart_money.get("volume_pct", 0)
                    info = (
                        f"🌊 [Smart Money Inflow] Late-odds collapse detected ({d_odds:+.2f}x shift, "
                        f"{v_pct:.0f}% late vol) -> Follow Smart Money on '{sm_cand.get('title')}' @ {sm_rate:.2f}x (Bet {sm_pct}%)"
                    )
                    return sm_cand, sm_pct, info, None

        # 2. Feature: Self-Learning Streamer Profiler (Fade-the-Public / Channel Bias Memory)
        channel_profile = kwargs.get("channel_profile")
        if channel_profile and channel_profile.get("total_tracked", 0) >= 5 and len(outcomes) == 2:
            acc = channel_profile.get("consensus_wins", 0) / max(1, channel_profile["total_tracked"])
            if acc < 0.40:
                # Chat suffers from severe delusion bias (<40% win rate) -> Fade the public!
                contrarian_cand = min(outcomes, key=lambda o: (o.get("vote_count", 0) or 0))
                c_rate = float(contrarian_cand.get("return_rate", 1.0) or 1.0)
                if c_rate >= min_odds_floor:
                    fade_pct = turbo_pct if current_balance < turbo_threshold else 35
                    info = (
                        f"🧠 [Brain Fade-The-Public] Delusion Bias on channel (Chat accuracy {acc*100:.0f}% < 40%) -> "
                        f"Contrarian Bet on '{contrarian_cand.get('title')}' @ {c_rate:.2f}x (Bet {fade_pct}%)"
                    )
                    return contrarian_cand, fade_pct, info, None

        # 3. Mode A: Multi-Account Hedging (Dutching / Arbitrage at balanced ~2x odds)
        if hedging_enabled and total_accounts >= 2 and len(outcomes) == 2:
            o1, o2 = outcomes[0], outcomes[1]
            r1 = float(o1.get("return_rate", 1.0) or 1.0)
            r2 = float(o2.get("return_rate", 1.0) or 1.0)

            # Check if both outcomes offer balanced returns (~1.60x to 2.65x)
            if 1.60 <= r1 <= 2.65 and 1.60 <= r2 <= 2.65:
                assigned_outcome = o1 if (account_index % 2 == 0) else o2
                assigned_group = "Group A" if (account_index % 2 == 0) else "Group B"
                target_rate = float(assigned_outcome.get("return_rate", 1.0) or 1.0)

                if current_balance < turbo_threshold:
                    hedge_pct = turbo_pct
                elif current_balance < 50000:
                    hedge_pct = 50
                else:
                    hedge_pct = 30

                info = (
                    f"🧠 [Brain Hedging] Balanced 2x Market ({r1:.2f}x vs {r2:.2f}x) -> "
                    f"{assigned_group} on '{assigned_outcome.get('title')}' @ {target_rate:.2f}x, Bet {hedge_pct}%"
                )
                return assigned_outcome, hedge_pct, info, None

        # 4. Mode B: Anti-Whale Skew Exploitation
        for o in outcomes:
            v_count = o.get("vote_count", 0) or 0
            v_pct = (v_count / total_voters * 100.0) if total_voters > 0 else 0.0
            pts = o.get("total_vote_amount", 0) or 0
            pts_pct = (pts / total_points * 100.0) if total_points > 0 else 0.0
            rate = float(o.get("return_rate", 1.0) or 1.0)

            if total_voters >= 8 and v_pct >= 58.0 and rate >= 1.35 and pts_pct <= 45.0:
                if current_balance < turbo_threshold:
                    pct = turbo_pct
                elif current_balance < 60000:
                    pct = 50
                else:
                    pct = 25
                info = (
                    f"🧠 [Brain Anti-Whale] Skew Exploit! ({v_pct:.1f}% crowd vs {pts_pct:.1f}% pts, "
                    f"Odds {rate:.2f}x) -> Bet {pct}% on '{o.get('title')}'"
                )
                return o, pct, info, None

        # 5. Mode C: Crowd Consensus Super-Lock (>=68% voter agreement)
        best_voter_outcome = max(
            outcomes,
            key=lambda o: (
                o.get("vote_count", 0) or 0,
                o.get("total_vote_amount", 0) or 0,
            ),
        )
        best_voters = best_voter_outcome.get("vote_count", 0) or 0
        consensus_pct = (best_voters / total_voters * 100.0) if total_voters > 0 else 50.0
        best_rate = float(best_voter_outcome.get("return_rate", 1.0) or 1.0)

        # Smart Odds Floor
        if min_odds_floor > 1.0 and best_rate < min_odds_floor:
            return None, 0, "", f"Odds {best_rate:.2f}x below safety floor ({min_odds_floor:.2f}x) - avoiding trap"

        if consensus_pct >= 68.0:
            if current_balance < turbo_threshold:
                pct = turbo_pct
                stage = f"Rocket Booster (<{turbo_threshold:,} pts)"
            elif current_balance < 60000:
                pct = 45
                stage = "Compound Acceleration (3k-60k pts)"
            else:
                pct = 25
                stage = "Pool-Preserving Cruise (60k-500k pts)"

            info = f"🧠 [Brain Consensus] {stage} ({consensus_pct:.1f}% crowd, {best_rate:.2f}x) -> Bet {pct}%"
            return best_voter_outcome, pct, info, None

        # 6. Mode D: Kelly Criterion Evaluation with Dynamic Sample Sizing
        best_candidate = None
        best_edge = -999.0
        for o in outcomes:
            v_count = o.get("vote_count", 0) or 0
            cons = (v_count / total_voters * 100.0) if total_voters > 0 else 50.0
            rate = float(o.get("return_rate", 1.0) or 1.0)
            if rate <= 1.0:
                continue
            b = rate - 1.0
            p = cons / 100.0
            q = 1.0 - p
            edge = (b * p - q) / b
            if edge > best_edge:
                best_edge = edge
                best_candidate = o

        if best_candidate and best_edge > 0.05:
            rate = float(best_candidate.get("return_rate", 1.0) or 1.0)
            if current_balance < turbo_threshold:
                pct = turbo_pct
            else:
                pct = max(15, min(int(best_edge * dynamic_kelly_fraction * 100.0), 55))
            info = f"🧠 [Brain Kelly] Edge={best_edge:.2f} (Odds {rate:.2f}x, Dynamic f*={dynamic_kelly_fraction:.2f}) -> Bet {pct}%"
            return best_candidate, pct, info, None

        # 7. Default Fallback
        if current_balance < turbo_threshold:
            fallback_pct = turbo_pct
        else:
            fallback_pct = 25
        info = f"🧠 [Brain Default] Majority crowd ({consensus_pct:.1f}%, {best_rate:.2f}x) -> Bet {fallback_pct}%"
        return best_voter_outcome, fallback_pct, info, None

    @staticmethod
    def select_outcome(outcomes: list, strategy: str = "brain", **kwargs) -> Optional[Dict[str, Any]]:
        """
        Chooses the target outcome from the prediction options based on strategy.
        'brain': master AI prediction brain with multi-account hedging & speedrun staging.
        'minimal_risk': minimal risk engine with liquidity, consensus, odds filters.
        'voter_focused_3stage': 3-stage dynamic voter-focused betting (no skips).
        'smart_consensus': crowd consensus with liquidity and odds safety filters.
        'kelly_criterion': mathematically optimal growth allocation based on edge.
        'anti_whale': targets crowd favorites with whale-distorted payout odds.
        'most_voters': selects outcome with the highest number of unique voters.
        'most_popular': selects outcome with the highest points pooled.
        """
        if not outcomes:
            return None

        if strategy in ("brain", "speedrun_500k"):
            best, pct, info, reason = KickPrediction.select_brain_outcome(outcomes, **kwargs)
            return best
        elif strategy == "minimal_risk":
            best, pct, info, reason = KickPrediction.select_minimal_risk_outcome(outcomes, **kwargs)
            return best
        elif strategy == "voter_focused_3stage":
            best, pct, stage, cons = KickPrediction.select_voter_focused_outcome(outcomes, **kwargs)
            return best
        elif strategy == "smart_consensus":
            best, reason = KickPrediction.select_smart_outcome(outcomes, **kwargs)
            return best
        elif strategy == "kelly_criterion":
            best, pct, info, reason = KickPrediction.select_kelly_outcome(outcomes, **kwargs)
            return best
        elif strategy == "anti_whale":
            best, pct, info, reason = KickPrediction.select_anti_whale_outcome(outcomes, **kwargs)
            return best
        elif strategy == "most_voters":
            return max(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0,
                ),
            )
        else:
            return max(
                outcomes,
                key=lambda o: (
                    o.get("total_vote_amount", 0) or 0,
                    o.get("vote_count", 0) or 0,
                ),
            )

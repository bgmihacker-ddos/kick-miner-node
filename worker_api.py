"""
Pure Standard-Library Async Worker REST API for Kick Channel Points Miner
Requires ZERO third-party web frameworks (no aiohttp, no Flask).
Runs on Python's built-in asyncio with near-zero memory footprint (< 100 KB).
"""

import os
import gc
import json
import random
import asyncio
from typing import Optional, TYPE_CHECKING
from loguru import logger

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

if TYPE_CHECKING:
    from account_manager import AccountManager


class WorkerAPI:
    def __init__(self, account_manager: "AccountManager", port: int = 9645):
        self.account_manager = account_manager
        self.port = port
        self.server: Optional[asyncio.Server] = None

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                return

            req_line = line.decode("utf-8", errors="ignore").strip()
            parts = req_line.split()
            if len(parts) < 2:
                writer.close()
                return

            method, path = parts[0].upper(), parts[1].split("?")[0]

            # Parse HTTP Headers
            headers = {}
            content_length = 0
            while True:
                h_line = await reader.readline()
                if not h_line or h_line in (b"\r\n", b"\n"):
                    break
                h_str = h_line.decode("utf-8", errors="ignore").strip()
                if ":" in h_str:
                    k, v = h_str.split(":", 1)
                    k_clean = k.strip().lower()
                    v_clean = v.strip()
                    headers[k_clean] = v_clean
                    if k_clean == "content-length":
                        try:
                            content_length = int(v_clean)
                        except ValueError:
                            content_length = 0

            # Read Body if POST
            body_bytes = b""
            if content_length > 0:
                body_bytes = await reader.readexactly(content_length)

            # Route Endpoints
            if method == "GET" and path == "/health":
                self._send_text_response(writer, 200, "OK")
                return

            if method == "GET" and (path == "/" or path == "/status"):
                data = self._get_status_data()
                self._send_json_response(writer, 200, data)
                return

            if method == "GET" and path == "/accounts":
                data = self._get_accounts_data()
                self._send_json_response(writer, 200, data)
                return

            if method == "POST" and (path == "/bet" or path == "/api/bet"):
                data = await self._handle_bet_dispatch(body_bytes)
                code = 200 if data.get("success") else 400
                self._send_json_response(writer, code, data)
                return

            if method == "GET" and path == "/api/accounts":
                data = self._get_accounts_data()
                self._send_json_response(writer, 200, data.get("accounts", []))
                return

            if method == "POST" and path == "/chat":
                data = await self._handle_chat_dispatch(body_bytes)
                code = 200 if data.get("success") else 400
                self._send_json_response(writer, code, data)
                return

            if method == "POST" and path == "/gc":
                collected = gc.collect()
                self._send_json_response(writer, 200, {"success": True, "collected_objects": collected})
                return

            # Default 404
            self._send_text_response(writer, 404, "Not Found")

        except Exception as e:
            logger.debug(f"[WorkerAPI] Error handling request: {e}")
            try:
                self._send_text_response(writer, 500, f"Internal Error: {e}")
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _get_status_data(self) -> dict:
        mem_mb = 0.0
        if HAS_PSUTIL:
            try:
                process = psutil.Process(os.getpid())
                mem_mb = round(process.memory_info().rss / (1024 * 1024), 1)
            except Exception:
                pass

        all_streamers = self.account_manager.get_all_streamers_flat()
        total_accounts = len(self.account_manager.workers)
        active_viewers = 0
        total_points = 0

        streamer_statuses = {}
        for s in all_streamers:
            is_live = False
            for w in self.account_manager.workers:
                st = w.state.streamers.get(s)
                if st and st.is_watching:
                    is_live = True
                    break
            streamer_statuses[s] = {"online": is_live}

        for w in self.account_manager.workers:
            is_watching_any = any(st.is_watching for st in w.state.streamers.values())
            if is_watching_any:
                active_viewers += 1
            for st in w.state.streamers.values():
                total_points += (st.points or 0)

        return {
            "status": "online",
            "memory_mb": mem_mb,
            "total_accounts": total_accounts,
            "active_viewers": active_viewers,
            "total_points": total_points,
            "streamers": streamer_statuses
        }

    def _get_accounts_data(self) -> dict:
        accounts_data = []
        for w in self.account_manager.workers:
            total_acc_points = sum(st.points or 0 for st in w.state.streamers.values())
            watching = [name for name, st in w.state.streamers.items() if st.is_watching]
            accounts_data.append({
                "alias": w.alias,
                "token_prefix": (w.token[:12] + "...") if w.token else "None",
                "proxy": w.proxy or "direct",
                "points": total_acc_points,
                "is_watching": len(watching) > 0,
                "watching_channels": watching
            })
        return {"accounts": accounts_data}

    async def _handle_bet_dispatch(self, body_bytes: bytes) -> dict:
        try:
            body = json.loads(body_bytes.decode("utf-8", errors="ignore")) if body_bytes else {}
        except Exception:
            return {"success": False, "error": "Invalid JSON body"}

        alias = body.get("alias") or body.get("account")
        streamer = body.get("streamer")
        outcome_id = body.get("outcome_id")
        amount = body.get("amount", 0)

        if not alias or not streamer or not outcome_id or amount <= 0:
            return {"success": False, "error": "Missing required fields"}

        target_worker = next((w for w in self.account_manager.workers if w.alias == alias), None)
        if not target_worker:
            return {"success": False, "error": f"Account '{alias}' not found"}

        try:
            from utils.kick_prediction import KickPrediction
            pred_client = KickPrediction()
            ok, res = await asyncio.to_thread(
                pred_client.vote_prediction,
                streamer, outcome_id, amount, target_worker.token
            )
            if ok:
                st = target_worker.state.streamers.get(streamer)
                if st and st.points:
                    st.points = max(0, st.points - amount)
                logger.success(f"[{alias}] 🎯 WorkerAPI Bet: Voted {amount:,} pts on {streamer} outcome {outcome_id}")
                return {"success": True, "result": res}
            else:
                return {"success": False, "error": str(res)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _handle_chat_dispatch(self, body_bytes: bytes) -> dict:
        try:
            body = json.loads(body_bytes.decode("utf-8", errors="ignore")) if body_bytes else {}
        except Exception:
            return {"success": False, "error": "Invalid JSON body"}

        alias = body.get("account")
        message = body.get("message")
        streamer = body.get("streamer", "xyroplayzz")

        if not message:
            return {"success": False, "error": "Message is empty"}

        # Select worker
        target_worker = None
        if alias:
            target_worker = next((w for w in self.account_manager.workers if w.alias == alias), None)
        if not target_worker and self.account_manager.workers:
            valid_workers = [w for w in self.account_manager.workers if w.token]
            if valid_workers:
                target_worker = random.choice(valid_workers)

        if not target_worker:
            return {"success": False, "error": "No available account found"}

        try:
            from utils.kick_chat import KickChatClient
            token = getattr(target_worker, "token", "")
            proxy = getattr(target_worker, "proxy", None)

            chatroom_id = KickChatClient.get_chatroom_id(streamer, token, proxy)
            if not chatroom_id:
                return {"success": False, "error": f"Could not find chatroom for {streamer}"}

            success, status_msg = KickChatClient.send_comment(streamer, chatroom_id, token, message, proxy)
            return {
                "success": success,
                "account": target_worker.alias,
                "message": message,
                "status": status_msg
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _send_json_response(self, writer: asyncio.StreamWriter, status: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        headers = (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(headers + body)

    def _send_text_response(self, writer: asyncio.StreamWriter, status: int, text: str):
        body = text.encode("utf-8")
        headers = (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: text/plain; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(headers + body)

    async def start(self):
        self.server = await asyncio.start_server(self.handle_request, "0.0.0.0", self.port)
        logger.success(f"⚡ Pure Asyncio Worker API listening on 0.0.0.0:{self.port} (Zero external dependencies!)")

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


async def memory_scrubber_task(interval_minutes: int = 15):
    """Periodically cleans up memory and trims log files to stay under 512MB."""
    while True:
        try:
            await asyncio.sleep(interval_minutes * 60)
            collected = gc.collect()
            logger.debug(f"[MemoryScrubber] Garbage collector freed {collected} objects.")

            for log_file in ["miner.log", "watchdog.log"]:
                if os.path.exists(log_file) and os.path.getsize(log_file) > 5 * 1024 * 1024:
                    try:
                        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                            lines = f.readlines()
                        with open(log_file, "w", encoding="utf-8") as f:
                            f.writelines(lines[-5000:])
                        logger.info(f"[MemoryScrubber] Trimmed {log_file} down to last 5000 lines.")
                    except Exception as e:
                        logger.warning(f"Could not trim {log_file}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[MemoryScrubber] Notice: {e}")

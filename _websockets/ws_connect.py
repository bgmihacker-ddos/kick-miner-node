import json
import asyncio
import traceback
import random
from typing import Dict, Optional, Callable, Awaitable
from dataclasses import dataclass

try:
    import rnet
    from rnet import WebSocket, Message
    HAS_RNET = True
except ImportError:
    try:
        import websockets
    except ImportError:
        websockets = None
    WebSocket = None
    Message = None
    HAS_RNET = False

from loguru import logger
from localization import t


@dataclass
class ConnectionState:
    is_connected: bool = False
    reconnect_attempts: int = 0
    max_reconnect_attempts: int = 3


class KickWebSocket:
    def __init__(
        self,
        data: Dict[str, str],
        proxy: Optional[str] = None,
        on_disconnect: Optional[Callable[[], Awaitable]] = None,
    ):
        self.ws: Optional[WebSocket] = None
        self.data = data
        self.proxy = proxy
        self.on_disconnect = on_disconnect
        self.state = ConnectionState()
        self.handshake_task: Optional[asyncio.Task] = None
        self.tracking_task: Optional[asyncio.Task] = None
        self._running = False

    async def connect(self) -> bool:
        if not self.data.get("token"):
            logger.error(t("token_must_not_be_empty"))
            return False

        try:
            ch = self.data.get("channelId", "?")
            logger.info(t(
                "websocket_connecting", channel_id=ch
            ))

            ws_url = (
                "wss://websockets.kick.com/viewer/v1/connect"
                f"?token={self.data['token']}"
            )

            ws_kwargs = dict(
                url=ws_url,
                read_buffer_size=4096,
                write_buffer_size=4096,
                max_message_size=4096,
            )

            if HAS_RNET and rnet is not None:
                if self.proxy:
                    ws_kwargs["proxy"] = self.proxy
                    try:
                        self.ws = await asyncio.wait_for(rnet.websocket(**ws_kwargs), timeout=4.0)
                    except Exception as proxy_err:
                        logger.warning(
                            f"⚠️ Proxy connection failed ({proxy_err}), failing over to direct..."
                        )
                        ws_kwargs.pop("proxy", None)
                        self.ws = await asyncio.wait_for(rnet.websocket(**ws_kwargs), timeout=5.0)
                else:
                    self.ws = await asyncio.wait_for(rnet.websocket(**ws_kwargs), timeout=5.0)
            else:
                if self.proxy:
                    try:
                        from websockets_proxy import Proxy, proxy_connect
                        proxy_obj = Proxy.from_url(self.proxy)
                        self.ws = await asyncio.wait_for(
                            proxy_connect(ws_url, proxy=proxy_obj),
                            timeout=4.0
                        )
                    except Exception as pe:
                        logger.warning(f"⚠️ Proxy failed ({pe}), falling back to direct...")
                        self.ws = await asyncio.wait_for(
                            websockets.connect(ws_url),
                            timeout=5.0
                        )
                else:
                    self.ws = await asyncio.wait_for(
                        websockets.connect(ws_url),
                        timeout=5.0
                    )

            logger.success(t("websocket_connected"))
            self.state.is_connected = True
            self.state.reconnect_attempts = 0

            await self._send_initial_messages()
            await self._start_background_tasks()
            await self._listen_for_messages()

            return True

        except Exception as e:
            logger.error(t(
                "websocket_connection_failed", error=str(e)
            ))
            logger.debug(t(
                "connection_traceback",
                traceback=traceback.format_exc(),
            ))
            self.state.is_connected = False
            await self._handle_reconnection()
            return False

    async def _send_initial_messages(self):
        await self._send_handshake()
        await self._send_ping()

    async def _start_background_tasks(self):
        self._running = True
        self.handshake_task = asyncio.create_task(
            self._handshake_loop()
        )
        self.tracking_task = asyncio.create_task(
            self._tracking_loop()
        )

    async def _handshake_loop(self):
        while self._running and self.state.is_connected:
            try:
                await asyncio.sleep(random.uniform(25, 35))
                if self.state.is_connected:
                    await self._send_handshake()
                    await self._send_ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(t(
                    "handshake_loop_error", error=str(e)
                ))
                break

    async def _tracking_loop(self):
        while self._running and self.state.is_connected:
            try:
                await asyncio.sleep(random.uniform(9.5, 12.5))
                if self.state.is_connected:
                    await self._send_user_event()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(t(
                    "tracking_loop_error", error=str(e)
                ))
                break

    async def _listen_for_messages(self):
        try:
            while self.state.is_connected and self._running:
                message = await self.ws.recv()
                await self._handle_message(message)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(t(
                "message_listening_error", error=str(e)
            ))
            self.state.is_connected = False
            await self._handle_reconnection()

    async def _handle_message(self, message):
        try:
            if hasattr(message, "text"):
                msg = message.text
            elif isinstance(message, bytes):
                msg = message.decode("utf-8", errors="ignore")
            else:
                msg = str(message)

            if not msg or msg.strip() == "":
                return

            if msg.strip() == "ping":
                await self._send_pong()
                return

            try:
                parsed = json.loads(msg)
            except json.JSONDecodeError:
                return

            msg_type = parsed.get("type", "unknown")
            logger.debug(t(
                "received_message_type", type=msg_type
            ))

            if msg_type == "channel_handshake":
                ch_id = None
                data_msg = parsed.get("data", {})
                if isinstance(data_msg, dict):
                    message_inner = data_msg.get("message", {})
                    if isinstance(message_inner, dict):
                        ch_id = message_inner.get("channelId")
                if ch_id:
                    logger.info(t(
                        "channel_handshake_received",
                        channel_id=ch_id,
                    ))

            elif msg_type == "ping":
                await self._send_pong()

            elif msg_type == "pong":
                pass

            elif msg_type == "error":
                err_data = parsed.get("data", {})
                err = (
                    err_data.get("message", "Unknown")
                    if isinstance(err_data, dict)
                    else "Unknown"
                )
                logger.error(t("websocket_error", error=err))

            elif msg_type == "user_event":
                pass

        except Exception as e:
            logger.error(t(
                "message_handling_error", error=str(e)
            ))

    async def _handle_reconnection(self):
        if (
            self.state.reconnect_attempts
            < self.state.max_reconnect_attempts
        ):
            self.state.reconnect_attempts += 1
            logger.info(t(
                "attempting_reconnect",
                attempt=self.state.reconnect_attempts,
                max=self.state.max_reconnect_attempts,
            ))
            await self._cleanup_tasks()

            delay = random.uniform(3.0, 7.0)
            logger.info(f"Reconnect delay: {delay:.1f}s")
            await asyncio.sleep(delay)
            await self.connect()
        else:
            logger.error(t("max_reconnection_attempts"))
            await self.disconnect()
            if self.on_disconnect:
                await self.on_disconnect()

    async def _cleanup_tasks(self):
        self._running = False
        for task in (self.handshake_task, self.tracking_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def disconnect(self):
        self.state.is_connected = False
        await self._cleanup_tasks()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        logger.info(t("websocket_closed"))

    async def _send_msg(self, payload: dict) -> bool:
        if not self.state.is_connected or not self.ws:
            return False
        raw_json = json.dumps(payload)
        try:
            if HAS_RNET and Message is not None:
                await self.ws.send(Message.from_text(raw_json))
            else:
                await self.ws.send(raw_json)
            return True
        except Exception as e:
            logger.error(f"WebSocket send error: {e}")
            self.state.is_connected = False
            return False

    async def _send_handshake(self):
        if not self.state.is_connected:
            return
        payload = {
            "type": "channel_handshake",
            "data": {
                "message": {
                    "channelId": int(
                        self.data.get("channelId", 0)
                    ),
                }
            },
        }
        await self._send_msg(payload)
        logger.debug(t(
            "sent_handshake",
            channel_id=self.data.get("channelId", "?"),
        ))

    async def _send_ping(self):
        if not self.state.is_connected:
            return
        await self._send_msg({"type": "ping"})
        logger.debug(t("sent_ping"))

    async def _send_pong(self):
        if not self.state.is_connected:
            return
        await self._send_msg({"type": "pong"})

    async def _send_user_event(self):
        if not self.state.is_connected:
            return
        payload = {
            "type": "user_event",
            "data": {
                "message": {
                    "name": "tracking.user.watch.livestream",
                    "channel_id": int(
                        self.data.get("channelId", 0)
                    ),
                    "livestream_id": int(
                        self.data.get("streamId", 0)
                    ),
                }
            },
        }
        await self._send_msg(payload)
        logger.debug(t(
            "sent_user_event",
            channel_id=self.data.get("channelId", "?"),
            stream_id=self.data.get("streamId", "?"),
        ))

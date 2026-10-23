from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable

import websockets
from dhanhq import MarketFeed

logger = logging.getLogger("dhan.stream")


class WebSocketStreamEngine:
    """High-capacity WebSocket streaming engine for DhanHQ Live Market Feed v2.

    Engineered to handle both small sets (1-100 stocks) and full market universes
    (8,000+ stocks across NSE and BSE) concurrently over a single high-throughput
    WebSocket connection without buffer overflow or frame drops.

    Features:
        - Asynchronous decoupled reader and sender loops.
        - High-throughput buffering with `max_queue=None` and `max_size=None`.
        - Paced non-blocking batch subscriptions (batches of 100 instruments with 10ms pacing).
        - Binary packet decoding via DhanHQ's native protocol parser.
        - Support for dynamic subscriptions and unsubscriptions during live streaming.
    """

    WSS_URL = "wss://api-feed.dhan.co"

    def __init__(
        self,
        client_id: str,
        access_token: str,
        on_message: Callable[[dict], None] | None = None,
        on_connect: Callable[[], None] | None = None,
        on_close: Callable[[], None] | None = None,
        on_error: Callable[[Any], None] | None = None,
    ):
        self.client_id = client_id
        self.access_token = access_token
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_close = on_close
        self.on_error = on_error

        self._parser = MarketFeed.__new__(MarketFeed)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._is_running = False
        self._is_connected = False
        self._sub_queue: asyncio.Queue | None = None
        self._stop_event = threading.Event()

    @staticmethod
    def get_exchange_segment_str(code: int) -> str:
        mapping = {
            0: "IDX_I",
            1: "NSE_EQ",
            2: "NSE_FNO",
            3: "NSE_CURRENCY",
            4: "BSE_EQ",
            5: "MCX_COMM",
            7: "BSE_CURRENCY",
            8: "BSE_FNO",
        }
        return mapping.get(code, "NSE_EQ")

    def start(self, initial_subscriptions: list[tuple[int, str, int]] | None = None) -> threading.Thread:
        """Start the streaming engine in a background daemon thread."""
        if self._is_running:
            return self._thread

        self._is_running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_thread,
            args=(initial_subscriptions or [],),
            daemon=True,
            name="DhanStreamEngineThread",
        )
        self._thread.start()
        return self._thread

    def stop(self) -> None:
        """Stop streaming and disconnect WebSocket."""
        self._is_running = False
        self._stop_event.set()
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._close_ws(), self._loop)

    def subscribe_dynamic(self, items: list[tuple[int, str, int]]) -> None:
        """Send dynamic subscription while streaming."""
        if self._loop and self._sub_queue and self._is_connected:
            asyncio.run_coroutine_threadsafe(self._sub_queue.put(("sub", items)), self._loop)

    def unsubscribe_dynamic(self, items: list[tuple[int, str, int]]) -> None:
        """Send dynamic unsubscription while streaming."""
        if self._loop and self._sub_queue and self._is_connected:
            asyncio.run_coroutine_threadsafe(self._sub_queue.put(("unsub", items)), self._loop)

    def _run_thread(self, initial_subscriptions: list[tuple[int, str, int]]) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main_async(initial_subscriptions))
        except Exception as e:
            logger.error("Error in stream engine thread: %s", e)
        finally:
            self._loop.close()

    async def _close_ws(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
            self._is_connected = False

    async def _main_async(self, initial_subscriptions: list[tuple[int, str, int]]) -> None:
        url = (
            f"{self.WSS_URL}?version=2"
            f"&token={self.access_token}"
            f"&clientId={self.client_id}"
            f"&authType=2"
        )
        self._sub_queue = asyncio.Queue()

        if initial_subscriptions:
            await self._sub_queue.put(("sub", initial_subscriptions))

        backoff = 30
        while self._is_running and not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    url,
                    max_queue=None,
                    max_size=None,
                ) as ws:
                    self._ws = ws
                    self._is_connected = True
                    backoff = 30

                    logger.info("Connected to DhanHQ Live Market Feed WebSocket.")
                    if self.on_connect:
                        try:
                            self.on_connect()
                        except Exception as e:
                            logger.error("Error in on_connect: %s", e)

                    reader_task = asyncio.create_task(self._reader_loop(ws))
                    sender_task = asyncio.create_task(self._sender_loop(ws))

                    done, pending = await asyncio.wait(
                        [reader_task, sender_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in pending:
                        task.cancel()

            except Exception as e:
                self._is_connected = False
                if self._stop_event.is_set() or not self._is_running:
                    break
                logger.warning("WebSocket disconnected: %s. Reconnecting in %ds...", e, backoff)
                if self.on_error:
                    try:
                        self.on_error(e)
                    except Exception:
                        pass
                try:
                    await asyncio.sleep(backoff)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    break
                backoff = min(backoff * 2, 60)

        self._is_connected = False
        if self.on_close:
            try:
                self.on_close()
            except Exception:
                pass

    async def _reader_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        """High-speed consumer reading incoming binary market feed frames."""
        while self._is_running and not self._stop_event.is_set():
            try:
                raw_bytes = await ws.recv()
                if not raw_bytes or not isinstance(raw_bytes, (bytes, bytearray)):
                    continue

                parsed = self._parser.process_data(raw_bytes)
                if parsed and isinstance(parsed, dict) and self.on_message:
                    try:
                        self.on_message(parsed)
                    except Exception as e:
                        logger.error("Error in on_message: %s", e)

            except websockets.exceptions.ConnectionClosed:
                break
            except Exception as e:
                logger.error("Error in reader loop: %s", e)
                break

    async def _sender_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Sender loop consuming subscription requests and pacing them."""
        while self._is_running and not self._stop_event.is_set():
            try:
                action, items = await self._sub_queue.get()
                if not items:
                    continue

                by_code: dict[int, list[tuple[int, str]]] = {}
                for seg, sec_id, code in items:
                    by_code.setdefault(code, []).append((seg, sec_id))

                for req_code, instruments in by_code.items():
                    actual_code = req_code + 1 if action == "unsub" else req_code

                    for i in range(0, len(instruments), 100):
                        batch = instruments[i : i + 100]
                        packet = {
                            "RequestCode": actual_code,
                            "InstrumentCount": len(batch),
                            "InstrumentList": [
                                {
                                    "ExchangeSegment": self.get_exchange_segment_str(seg),
                                    "SecurityId": str(sec_id),
                                }
                                for seg, sec_id in batch
                            ],
                        }
                        await ws.send(json.dumps(packet))
                        # 10ms pacing allows event loop to interleave incoming frame reads
                        await asyncio.sleep(0.010)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in sender loop: %s", e)
                break

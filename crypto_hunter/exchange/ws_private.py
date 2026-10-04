"""Binance USDⓈ-M Futures User Data Stream client.

The signed REST client creates and renews a listenKey. Account position updates,
order fills, and algo-order state changes are normalized for the engine and the
connection automatically reconnects with a fresh listenKey when needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, Awaitable, Callable, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .rest import BinanceFuturesREST

log = logging.getLogger("ch.ws.private")
EventCB = Callable[[str, Dict[str, Any]], Awaitable[None] | None]


class PrivateWS:
    def __init__(self, url: str, rest: BinanceFuturesREST, on_event: EventCB):
        self.url = url.rstrip("/")
        self.rest = rest
        self.on_event = on_event
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._stopping = False
        self.connected = False
        self.logged_in = False
        self.last_message_ts = 0.0
        self.reconnects = 0
        self.listen_key: Optional[str] = None

    @staticmethod
    def stream_url(base_url: str, listen_key: str) -> str:
        return f"{base_url.rstrip('/')}/ws/{listen_key}"

    def start(self) -> None:
        if not self._task or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="binance-private-ws")

    async def stop(self) -> None:
        self._stopping = True
        for task in (self._keepalive_task, self._task):
            if task and not task.done():
                task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self.listen_key:
            try:
                await self.rest.close_listen_key(self.listen_key)
            except Exception as exc:
                log.debug("closing Binance listenKey failed: %s", exc)
        self.listen_key = None
        self.connected = self.logged_in = False

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping:
            listen_key: Optional[str] = None
            try:
                listen_key = await self.rest.create_listen_key()
                self.listen_key = listen_key
                url = self.stream_url(self.url, listen_key)
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024,
                    open_timeout=10, close_timeout=3,
                ) as ws:
                    self._ws = ws
                    self.connected = self.logged_in = True
                    self.last_message_ts = time.time()
                    backoff = 1.0
                    log.info("Binance user-data WS connected")
                    self._keepalive_task = asyncio.create_task(self._keepalive(), name="binance-listenkey-keepalive")
                    async for raw in ws:
                        self.last_message_ts = time.time()
                        try:
                            await self._handle(raw)
                        except Exception:
                            log.exception("Binance private WS handler failed")
            except asyncio.CancelledError:
                break
            except (ConnectionClosed, OSError, asyncio.TimeoutError, Exception) as exc:
                if self._stopping:
                    break
                self.reconnects += 1
                log.warning("Binance user-data WS disconnected (%s) – reconnecting in %.1fs", exc, backoff)
            finally:
                self.connected = self.logged_in = False
                self._ws = None
                if self._keepalive_task:
                    self._keepalive_task.cancel()
                    self._keepalive_task = None
                if listen_key:
                    try:
                        await self.rest.close_listen_key(listen_key)
                    except Exception as exc:
                        log.debug("closing expired Binance listenKey failed: %s", exc)
                self.listen_key = None
            if self._stopping:
                break
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 30.0)

    async def _keepalive(self) -> None:
        try:
            while not self._stopping and self.listen_key:
                await asyncio.sleep(30 * 60)
                await self.rest.keepalive_listen_key(self.listen_key)
                log.debug("Binance user-data listenKey renewed")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.warning("Binance listenKey renewal failed: %s – reconnecting", exc)
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:
                    pass

    async def _emit(self, kind: str, data: Dict[str, Any]) -> None:
        result = self.on_event(kind, data)
        if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
            await result

    async def _handle(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        event = str(message.get("e", ""))
        if event == "listenKeyExpired":
            log.warning("Binance user-data listenKey expired; reconnecting")
            if self._ws:
                await self._ws.close()
            return
        if event == "ACCOUNT_UPDATE":
            account = message.get("a") or {}
            # Balance payloads omit the account's true available margin; the REST account
            # endpoint remains the source of truth for equity/free balance.
            for position in account.get("P", []) or []:
                await self._emit("position", {
                    "symbol": position.get("s"),
                    "positionSide": position.get("ps", "BOTH"),
                    "positionAmt": position.get("pa", 0),
                    "entryPrice": position.get("ep", 0),
                    "unRealizedPnl": position.get("up", 0),
                    "marginType": position.get("mt", "isolated"),
                    "isolatedMargin": position.get("iw", 0),
                    "cr": position.get("cr", 0),
                })
            await self._emit("account", {"reason": account.get("m"), "eventTime": message.get("E")})
        elif event == "ORDER_TRADE_UPDATE":
            await self._emit("order", message.get("o") or {})
        elif event == "ALGO_UPDATE":
            await self._emit("algo.order", message.get("o") or {})
        elif event == "MARGIN_CALL":
            await self._emit("account", {"marginCall": message.get("p") or [], "eventTime": message.get("E")})
        elif event:
            log.debug("unhandled Binance private event: %s", event)

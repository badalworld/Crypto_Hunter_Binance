"""Binance USDⓈ-M public websocket market-data client.

The client dynamically subscribes to 5m/1h kline streams for the watch-list and
24h ticker, mark-price, and book-ticker streams for managed positions. Binance stream
names use lowercase symbols (for example ``btcusdt@markPrice@1s``).
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

from .rest import KLINE_INTERVALS

log = logging.getLogger("ch.ws.public")

TickerCB = Callable[[Dict[str, Any]], Awaitable[None] | None]
KlineCB = Callable[[str, str, Dict[str, Any]], Awaitable[None] | None]


async def _maybe_await(value: Any) -> None:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        await value


class PublicWS:
    def __init__(self, url: str, on_ticker: TickerCB, on_kline: KlineCB, interval: str = "Min5"):
        self.url = url.rstrip("/")
        self.on_ticker = on_ticker
        self.on_kline = on_kline
        self.interval = interval
        self.binance_interval = KLINE_INTERVALS.get(interval, interval)
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = False
        self.connected = False
        self.last_message_ts = 0.0
        self.reconnects = 0
        self._kline_symbols: Set[str] = set()
        self._ticker_symbols: Set[str] = set()
        self._next_id = 1

    def start(self) -> None:
        if not self._task or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run(), name="binance-public-ws")

    async def stop(self) -> None:
        self._stopping = True
        if self._task and not self._task.done():
            self._task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self.connected = False

    async def set_kline_symbols(self, symbols: Set[str]) -> None:
        added = symbols - self._kline_symbols
        removed = self._kline_symbols - symbols
        self._kline_symbols = set(symbols)
        streams = [f"{s.lower()}@kline_{self.binance_interval}" for s in removed]
        await self._send_streams("UNSUBSCRIBE", streams)
        streams = [f"{s.lower()}@kline_{self.binance_interval}" for s in added]
        await self._send_streams("SUBSCRIBE", streams)

    async def set_ticker_symbols(self, symbols: Set[str]) -> None:
        added = symbols - self._ticker_symbols
        removed = self._ticker_symbols - symbols
        self._ticker_symbols = set(symbols)
        streams = [stream for symbol in removed for stream in self._ticker_streams_for(symbol)]
        await self._send_streams("UNSUBSCRIBE", streams)
        streams = [stream for symbol in added for stream in self._ticker_streams_for(symbol)]
        await self._send_streams("SUBSCRIBE", streams)

    @staticmethod
    def _ticker_streams_for(symbol: str) -> list[str]:
        s = symbol.lower()
        # 24hr ticker includes best bid/ask; @bookTicker is routed to Binance's
        # separate /public endpoint, while these feeds belong on /market.
        return [f"{s}@ticker", f"{s}@markPrice@1s"]

    async def _send_streams(self, method: str, streams: list[str]) -> None:
        if not streams or not self.connected or not self._ws:
            return
        # Binance allows up to 200 params per subscription request; keep a safe batch.
        for offset in range(0, len(streams), 100):
            try:
                await self._ws.send(json.dumps({
                    "method": method,
                    "params": streams[offset:offset + 100],
                    "id": self._next_id,
                }))
                self._next_id += 1
            except Exception as exc:
                log.debug("Binance WS subscription update failed: %s", exc)
                return

    async def _resubscribe(self) -> None:
        streams = [f"{s.lower()}@kline_{self.binance_interval}" for s in self._kline_symbols]
        streams.extend(stream for symbol in self._ticker_symbols for stream in self._ticker_streams_for(symbol))
        await self._send_streams("SUBSCRIBE", streams)

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024,
                    open_timeout=10, close_timeout=3,
                ) as ws:
                    self._ws = ws
                    self.connected = True
                    backoff = 1.0
                    log.info("Binance market-data WS connected")
                    await self._resubscribe()
                    async for raw in ws:
                        self.last_message_ts = time.time()
                        try:
                            await self._handle(raw)
                        except Exception:
                            log.exception("Binance public WS handler failed")
            except asyncio.CancelledError:
                break
            except (ConnectionClosed, OSError, asyncio.TimeoutError, Exception) as exc:
                if self._stopping:
                    break
                self.reconnects += 1
                log.warning("Binance market WS disconnected (%s) – reconnecting in %.1fs", exc, backoff)
            finally:
                self.connected = False
                self._ws = None
            if self._stopping:
                break
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 30.0)

    async def _handle(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        # Combined streams wrap the event in {stream, data}; raw /ws streams do not.
        data = message.get("data", message)
        event = data.get("e") if isinstance(data, dict) else None
        if event == "24hrTicker":
            await _maybe_await(self.on_ticker({
                "symbol": data.get("s"), "lastPrice": data.get("c"),
                "bid1": data.get("b"), "ask1": data.get("a"),
                "volume24": data.get("v"), "amount24": data.get("q"),
                "riseFallRate": float(data.get("P", 0) or 0) / 100.0,
                "timestamp": data.get("E", int(time.time() * 1000)),
            }))
        elif event == "markPriceUpdate":
            await _maybe_await(self.on_ticker({
                "symbol": data.get("s"), "fairPrice": data.get("p"), "indexPrice": data.get("i"),
                "timestamp": data.get("E", int(time.time() * 1000)),
            }))
        elif event == "bookTicker":
            await _maybe_await(self.on_ticker({
                "symbol": data.get("s"), "bid1": data.get("b"), "ask1": data.get("a"),
                "timestamp": data.get("T", data.get("E", int(time.time() * 1000))),
            }))
        elif event == "kline":
            candle = data.get("k") or {}
            symbol = str(data.get("s") or candle.get("s") or "")
            if not symbol:
                return
            normalized = {
                "t": int(float(candle.get("t", 0)) / 1000),
                "o": candle.get("o"), "h": candle.get("h"), "l": candle.get("l"),
                "c": candle.get("c"), "v": candle.get("v", 0), "q": candle.get("q", 0),
            }
            await _maybe_await(self.on_kline(symbol, self.interval, normalized))
        elif "code" in message and message.get("code") not in (None, 0):
            log.warning("Binance WS response: %s", message)

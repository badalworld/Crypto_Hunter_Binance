"""Conservative Binance Futures rate limiter.

The main bucket tracks Binance's USDⓈ-M request-weight budget, while the endpoint
windows damp repeated calls to the same route. Exchange documentation limits are
occasionally revised; this client deliberately reserves headroom rather than trying
to run at the published maximum.
"""
from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Tuple


@dataclass
class _Window:
    limit: int
    period: float
    hits: Deque[float] = field(default_factory=deque)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# Request counts (not weights) per endpoint. The weighted IP bucket below is the
# authoritative global guard; route buckets prevent accidental hot loops.
BINANCE_FUTURES_LIMITS: Dict[str, Tuple[int, float]] = {
    "fapi/v1/ping": (120, 60.0),
    "fapi/v1/time": (120, 60.0),
    "fapi/v1/exchangeInfo": (30, 60.0),
    "fapi/v1/ticker/24hr": (30, 60.0),
    "fapi/v1/ticker/bookTicker": (60, 60.0),
    "fapi/v1/premiumIndex": (60, 60.0),
    "fapi/v1/klines": (600, 60.0),
    "fapi/v3/account": (120, 60.0),
    "fapi/v3/positionRisk": (120, 60.0),
    "fapi/v1/positionSide/dual": (120, 60.0),
    "fapi/v1/leverage": (60, 60.0),
    "fapi/v1/marginType": (60, 60.0),
    "fapi/v1/order": (240, 60.0),
    "fapi/v1/algoOrder": (240, 60.0),
    "fapi/v1/openAlgoOrders": (120, 60.0),
    "fapi/v1/algoOpenOrders": (120, 60.0),
    "fapi/v1/income": (120, 60.0),
    "fapi/v1/userTrades": (120, 60.0),
    "fapi/v1/listenKey": (120, 60.0),
}
DEFAULT_LIMIT: Tuple[int, float] = (600, 60.0)


class RateLimiter:
    def __init__(self, fraction: float = 0.95, limits: Dict[str, Tuple[int, float]] | None = None):
        self.fraction = fraction
        self._limits = dict(limits or BINANCE_FUTURES_LIMITS)
        # Binance documents 2400 request-weight units per minute per IP.
        self._global = _Window(limit=max(1, math.floor(2400 * fraction)), period=60.0)
        self._windows: Dict[str, _Window] = {}
        self.stats: Dict[str, int] = {}

    def _window(self, key: str) -> _Window:
        window = self._windows.get(key)
        if window is None:
            count, period = self._limits.get(key, DEFAULT_LIMIT)
            window = _Window(limit=max(1, math.floor(count * self.fraction)), period=period)
            self._windows[key] = window
        return window

    @staticmethod
    def key_for(path: str) -> str:
        path = path.split("?")[0].strip("/")
        if path.startswith("fapi/"):
            return path
        return path

    async def acquire(self, path: str, weight: int = 1) -> None:
        key = self.key_for(path)
        weight = max(1, int(weight))
        await self._wait(self._global, weight)
        await self._wait(self._window(key))
        self.stats[key] = self.stats.get(key, 0) + 1

    async def _wait(self, window: _Window, amount: int = 1) -> None:
        amount = min(max(1, amount), window.limit)
        async with window.lock:
            while True:
                now = time.monotonic()
                while window.hits and now - window.hits[0] >= window.period:
                    window.hits.popleft()
                if len(window.hits) + amount <= window.limit:
                    window.hits.extend([now] * amount)
                    return
                oldest = window.hits[0] if window.hits else now
                await asyncio.sleep(max(0.005, window.period - (now - oldest) + 0.002))

    def usage(self) -> Dict[str, Dict[str, float]]:
        now = time.monotonic()
        out: Dict[str, Dict[str, float]] = {}
        for key, window in self._windows.items():
            used = sum(1 for timestamp in window.hits if now - timestamp < window.period)
            out[key] = {
                "used": used,
                "budget": window.limit,
                "period": window.period,
                "pct": round(100 * used / window.limit, 1),
                "requests": self.stats.get(key, 0),
            }
        used_global = sum(1 for timestamp in self._global.hits if now - timestamp < self._global.period)
        out["request_weight_1m"] = {
            "used": used_global,
            "budget": self._global.limit,
            "period": self._global.period,
            "pct": round(100 * used_global / self._global.limit, 1),
            "requests": sum(self.stats.values()),
        }
        return out

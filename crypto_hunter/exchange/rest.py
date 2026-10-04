"""Async Binance USDⓈ-M Futures REST client.

All signed requests use Binance's HMAC-SHA256 query-string signature and the
``X-MBX-APIKEY`` header. Conditional TP/SL orders use the Algo Service endpoints
(``/fapi/v1/algoOrder``), as required by Binance for USDⓈ-M Futures.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

import aiohttp

from ..security import Credentials
from .models import AccountAsset, Contract, ExchangeAPIError, ExchangePosition, Ticker
from .rate_limiter import RateLimiter

log = logging.getLogger("ch.rest")

KLINE_INTERVALS = {
    "Min1": "1m", "Min3": "3m", "Min5": "5m", "Min15": "15m", "Min30": "30m",
    "Min60": "1h", "Hour1": "1h", "Hour2": "2h", "Hour4": "4h", "Hour6": "6h",
    "Hour8": "8h", "Hour12": "12h", "Day1": "1d", "Day3": "3d", "Week1": "1w", "Month1": "1M",
}


def _value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return format(v, ".15g")
    return str(v)


def _encode_params(params: Dict[str, Any]) -> str:
    pairs = [(key, _value(value)) for key, value in params.items() if value is not None]
    return urlencode(pairs, doseq=True, quote_via=quote, safe="-_.~")


class BinanceFuturesREST:
    def __init__(
        self,
        base_url: str,
        rate_limiter: RateLimiter,
        credentials: Optional[Credentials] = None,
        legacy_base_url: Optional[str] = None,
        timeout_sec: float = 10.0,
        max_retries: int = 4,
        backoff_base: float = 0.25,
        pool_size: int = 32,
        recv_window_sec: int = 10,
    ):
        # ``legacy_base_url`` is accepted for call-site compatibility; Binance's USD-M
        # Futures API has a single configured REST origin, so it is intentionally unused.
        del legacy_base_url
        self.base_url = base_url.rstrip("/")
        self.rl = rate_limiter
        self.creds = credentials
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(5.0, timeout_sec))
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.pool_size = pool_size
        self.recv_window_ms = max(1000, int(recv_window_sec * 1000))
        self._session: Optional[aiohttp.ClientSession] = None
        self._time_offset_ms = 0
        self.last_latency_ms: float = 0.0

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.pool_size, limit_per_host=self.pool_size, ttl_dns_cache=300,
                enable_cleanup_closed=True, keepalive_timeout=30,
            )
            self._session = aiohttp.ClientSession(connector=connector, timeout=self.timeout)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def set_credentials(self, creds: Optional[Credentials]) -> None:
        self.creds = creds

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None, "REST client not started"
        return self._session

    def _now_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    def _signed_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if self.creds is None:
            raise ExchangeAPIError(401, "API credentials not configured")
        signed = dict(params)
        signed["timestamp"] = self._now_ms()
        signed["recvWindow"] = self.recv_window_ms
        query = _encode_params(signed)
        signature = hmac.new(self.creds.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        signed["signature"] = signature
        return signed

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        private: bool = False,
        api_key_only: bool = False,
        weight: int = 1,
        retry_safe: Optional[bool] = None,
    ) -> Any:
        if (private or api_key_only) and self.creds is None:
            raise ExchangeAPIError(401, "API credentials not configured", path)
        request_params = {k: v for k, v in (params or {}).items() if v is not None}
        if private:
            request_params = self._signed_params(request_params)
        headers: Dict[str, str] = {}
        if private or api_key_only:
            headers["X-MBX-APIKEY"] = self.creds.api_key  # type: ignore[union-attr]
        if method in {"POST", "PUT"}:
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        safe_to_retry = retry_safe if retry_safe is not None else method in {"GET", "HEAD"}
        attempt = 0
        while True:
            await self.rl.acquire(path, weight=weight)
            query = _encode_params(request_params)
            url = f"{self.base_url}{path}"
            data = None
            if method in {"GET", "DELETE", "HEAD"}:
                if query:
                    url = f"{url}?{query}"
            elif query:
                data = query

            t0 = time.perf_counter()
            try:
                async with self.session.request(method, url, headers=headers, data=data) as resp:
                    text = await resp.text()
                    self.last_latency_ms = (time.perf_counter() - t0) * 1000
                    try:
                        payload = json.loads(text) if text else {}
                    except json.JSONDecodeError:
                        payload = {"msg": text[:300]}

                    if resp.status >= 400:
                        code = resp.status
                        error_message = payload.get("msg") if isinstance(payload, dict) else None
                        msg = f"HTTP {resp.status}: {str(error_message or text)[:300]}"
                        if isinstance(payload, dict):
                            try:
                                code = int(payload.get("code", resp.status))
                            except (TypeError, ValueError):
                                pass
                            msg = str(payload.get("msg") or msg)
                        exc = ExchangeAPIError(code, msg, path, payload)
                        if safe_to_retry and exc.retryable and attempt < self.max_retries:
                            attempt += 1
                            delay = self.backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    delay = max(delay, float(retry_after))
                                except ValueError:
                                    pass
                            log.warning("%s %s failed (%s) – retry %d/%d in %.2fs", method, path, exc.message,
                                        attempt, self.max_retries, delay)
                            if code == -1021:
                                await self._sync_time_offset()
                            await asyncio.sleep(delay)
                            continue
                        raise exc
                    if isinstance(payload, dict) and "code" in payload and isinstance(payload.get("code"), (int, float)) and payload["code"] < 0:
                        raise ExchangeAPIError(int(payload["code"]), str(payload.get("msg", "API error")), path, payload)
                    return payload
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if not safe_to_retry or attempt >= self.max_retries:
                    raise ExchangeAPIError(0, f"network error: {type(exc).__name__}: {exc}", path) from exc
                attempt += 1
                delay = self.backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                log.warning("%s %s network error (%s) – retry %d/%d in %.2fs", method, path, exc,
                            attempt, self.max_retries, delay)
                await asyncio.sleep(delay)

    async def _sync_time_offset(self) -> None:
        try:
            server_ms = await self._request("GET", "/fapi/v1/time", weight=1)
            self._time_offset_ms = int(server_ms["serverTime"]) - int(time.time() * 1000)
        except Exception as exc:  # A later normal request can still succeed with local time.
            log.debug("Binance time sync failed: %s", exc)

    # ================================================================== public data
    async def ping(self) -> int:
        await self._request("GET", "/fapi/v1/ping", weight=1)
        data = await self._request("GET", "/fapi/v1/time", weight=1)
        server_ms = int(data["serverTime"])
        self._time_offset_ms = server_ms - int(time.time() * 1000)
        return server_ms

    async def get_contracts(self) -> List[Contract]:
        data = await self._request("GET", "/fapi/v1/exchangeInfo", weight=1)
        return [Contract.from_api(d) for d in (data.get("symbols") or [])]

    async def get_tickers(self) -> List[Ticker]:
        tickers, books, marks = await asyncio.gather(
            self._request("GET", "/fapi/v1/ticker/24hr", weight=40),
            self._request("GET", "/fapi/v1/ticker/bookTicker", weight=5),
            self._request("GET", "/fapi/v1/premiumIndex", weight=10),
        )
        book_by_symbol = {str(x.get("symbol")): x for x in (books if isinstance(books, list) else [books])}
        mark_by_symbol = {str(x.get("symbol")): x for x in (marks if isinstance(marks, list) else [marks])}
        out: List[Ticker] = []
        for row in (tickers if isinstance(tickers, list) else [tickers]):
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            merged = dict(row)
            merged.update(book_by_symbol.get(symbol, {}))
            merged.update(mark_by_symbol.get(symbol, {}))
            out.append(Ticker.from_api(merged))
        return out

    async def get_ticker(self, symbol: str) -> Ticker:
        row = await self._request("GET", "/fapi/v1/ticker/24hr", {"symbol": symbol}, weight=1)
        book, mark = await asyncio.gather(
            self._request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol}, weight=1),
            self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, weight=1),
        )
        return Ticker.from_api({**row, **book, **mark})

    async def get_kline(self, symbol: str, interval: str = "Min5", start: Optional[int] = None,
                        end: Optional[int] = None) -> Dict[str, List[float]]:
        """Fetch Binance klines and normalize timestamps to seconds for ``KlineStore``."""
        binance_interval = KLINE_INTERVALS.get(interval, interval)
        params: Dict[str, Any] = {"symbol": symbol, "interval": binance_interval}
        if start is not None:
            params["startTime"] = int(start * 1000 if start < 10_000_000_000 else start)
        if end is not None:
            params["endTime"] = int(end * 1000 if end < 10_000_000_000 else end)
        if start is not None and end is not None:
            span = max(1, int((end - start) / 60))
            # Use the caller's interval-aware range when possible; Binance caps standard klines at 1500.
            params["limit"] = min(1500, max(1, span + 5))
        else:
            params["limit"] = 500
        limit = int(params["limit"])
        weight = 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
        rows = await self._request("GET", "/fapi/v1/klines", params, weight=weight)
        out: Dict[str, List[float]] = {k: [] for k in ("time", "open", "close", "high", "low", "vol", "amount")}
        for row in rows or []:
            if not isinstance(row, (list, tuple)) or len(row) < 8:
                continue
            out["time"].append(float(row[0]) / 1000.0)
            out["open"].append(float(row[1]))
            out["high"].append(float(row[2]))
            out["low"].append(float(row[3]))
            out["close"].append(float(row[4]))
            out["vol"].append(float(row[5]))
            out["amount"].append(float(row[7]))
        return out

    # ==================================================================== account
    async def get_account(self) -> Dict[str, Any]:
        return await self._request("GET", "/fapi/v3/account", private=True, weight=5)

    async def get_assets(self) -> List[AccountAsset]:
        account = await self.get_account()
        rows = account.get("assets") or []
        out = []
        for row in rows:
            normalized = dict(row)
            normalized["canTrade"] = account.get("canTrade", True)
            out.append(AccountAsset.from_api(normalized))
        return out

    async def get_asset(self, currency: str = "USDT") -> AccountAsset:
        account = await self.get_account()
        rows = account.get("assets") or []
        row = next((x for x in rows if x.get("asset") == currency), None)
        if row is None:
            return AccountAsset(currency, 0, 0, 0, 0, 0, 0, bool(account.get("canTrade", False)))
        normalized = dict(row)
        # The account-level totals are the effective single-asset/multi-assets margin balance.
        normalized.update({
            "equity": account.get("totalMarginBalance", row.get("marginBalance", row.get("walletBalance", 0))),
            "availableBalance": account.get("availableBalance", row.get("availableBalance", 0)),
            "cashBalance": account.get("totalWalletBalance", row.get("walletBalance", 0)),
            "positionInitialMargin": account.get("totalPositionInitialMargin", row.get("positionInitialMargin", 0)),
            "unrealized": account.get("totalUnrealizedProfit", row.get("unrealizedProfit", 0)),
            "canTrade": account.get("canTrade", False),
        })
        return AccountAsset.from_api(normalized)

    async def get_open_positions(self, symbol: Optional[str] = None) -> List[ExchangePosition]:
        params = {"symbol": symbol} if symbol else None
        rows = await self._request("GET", "/fapi/v3/positionRisk", params, private=True, weight=5)
        out = []
        for row in rows or []:
            pos = ExchangePosition.from_api(row)
            if pos.hold_vol > 0 and pos.symbol:
                out.append(pos)
        return out

    async def get_position_mode(self) -> int:
        data = await self._request("GET", "/fapi/v1/positionSide/dual", private=True, weight=30)
        dual_side = data.get("dualSidePosition")
        is_hedge = dual_side is True or str(dual_side).lower() == "true"
        return 1 if is_hedge else 2

    async def change_margin_type(self, symbol: str, margin_type: str) -> Any:
        mode = "ISOLATED" if margin_type.lower() == "isolated" else "CROSSED"
        try:
            return await self._request(
                "POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": mode},
                private=True, weight=1, retry_safe=True,
            )
        except ExchangeAPIError as exc:
            # Binance returns -4046 when the symbol is already in the requested mode.
            if exc.code == -4046:
                return {"code": 200, "msg": "No need to change margin type."}
            raise

    async def change_leverage(self, symbol: str, leverage: int, open_type: int = 1,
                             position_type: int = 1, position_id: Optional[int] = None) -> Any:
        del open_type, position_type, position_id
        return await self._request(
            "POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": int(leverage)},
            private=True, weight=1, retry_safe=True,
        )

    # ====================================================================== orders
    async def create_order(
        self,
        symbol: str,
        vol: float,
        side: int,
        order_type: int,
        open_type: int = 1,
        price: Optional[float] = None,
        leverage: Optional[int] = None,
        position_id: Optional[int] = None,
        external_oid: Optional[str] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        loss_trend: Optional[int] = None,
        profit_trend: Optional[int] = None,
        position_mode: Optional[int] = None,
        reduce_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
        # Adapter compatibility parameters are accepted but unused by Binance.
        del open_type, leverage, position_id, stop_loss_price, take_profit_price, loss_trend, profit_trend
        side_name = {1: "BUY", 3: "SELL", 4: "SELL", 2: "BUY"}.get(int(side))
        if side_name is None:
            raise ValueError(f"unsupported order side: {side}")
        order_name = "MARKET" if int(order_type) == 5 else "LIMIT"
        params: Dict[str, Any] = {"symbol": symbol, "side": side_name, "type": order_name, "quantity": vol}
        if order_name == "LIMIT":
            params.update({"price": price, "timeInForce": "IOC"})
        if position_mode == 1:
            params["positionSide"] = "LONG" if side in (1, 4) else "SHORT"
        elif position_mode == 2:
            params["positionSide"] = "BOTH"
        if reduce_only and position_mode != 1:
            params["reduceOnly"] = True
        if external_oid:
            params["newClientOrderId"] = external_oid
        params["newOrderRespType"] = "RESULT"
        data = await self._request("POST", "/fapi/v1/order", params, private=True, weight=1, retry_safe=False)
        return data if isinstance(data, dict) else {}

    async def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return await self._request(
            "GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, private=True, weight=1,
        ) or {}

    async def get_order_by_client_id(self, symbol: str, client_order_id: str) -> Dict[str, Any]:
        return await self._request(
            "GET", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_order_id},
            private=True, weight=1,
        ) or {}

    async def cancel_orders(self, symbol: str, order_ids: List[str | int]) -> Any:
        responses = []
        for order_id in order_ids:
            responses.append(await self._request(
                "DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id},
                private=True, weight=1, retry_safe=True,
            ))
        return responses

    async def cancel_all_orders(self, symbol: str) -> Any:
        return await self._request(
            "DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, private=True, weight=1, retry_safe=True,
        )

    # --------------------------------------------- Algo Service TP / SL orders
    async def get_stop_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"algoType": "CONDITIONAL"}
        if symbol:
            params["symbol"] = symbol
        data = await self._request("GET", "/fapi/v1/openAlgoOrders", params, private=True, weight=1)
        return list(data or [])

    async def place_algo_order(self, symbol: str, side: str, position_mode: int, order_type: str,
                               trigger_price: float, working_type: str, client_algo_id: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type,
            "triggerPrice": trigger_price,
            "workingType": working_type,
            "closePosition": True,
            "priceProtect": False,
            "newOrderRespType": "RESULT",
        }
        params["positionSide"] = "LONG" if side.upper() == "SELL" and position_mode == 1 else (
            "SHORT" if side.upper() == "BUY" and position_mode == 1 else "BOTH"
        )
        if client_algo_id:
            params["clientAlgoId"] = client_algo_id
        data = await self._request("POST", "/fapi/v1/algoOrder", params, private=True, weight=1, retry_safe=False)
        return data if isinstance(data, dict) else {}

    async def cancel_algo_order(self, algo_id: str | int) -> Any:
        return await self._request(
            "DELETE", "/fapi/v1/algoOrder", {"algoid": algo_id}, private=True, weight=1, retry_safe=True,
        )

    async def cancel_stop_orders(self, ids: List[str | int]) -> Any:
        out = []
        for algo_id in dict.fromkeys(str(x) for x in ids if x):
            try:
                out.append(await self.cancel_algo_order(algo_id))
            except ExchangeAPIError as exc:
                # Already-triggered/canceled protection is expected during close/reconcile.
                if exc.code not in {-2011, -2013, -2021}:
                    raise
        return out

    async def cancel_all_stop_orders(self, symbol: str, position_id: Optional[int] = None) -> Any:
        del position_id
        return await self._request(
            "DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol}, private=True, weight=1, retry_safe=True,
        )

    # ==================================================================== PnL ledger
    async def get_income_history(self, symbol: str, start_time: int, end_time: Optional[int] = None,
                                 limit: int = 1000) -> List[Dict[str, Any]]:
        now = self._now_ms()
        # Binance user income queries should use a bounded time window; closed trade history is
        # available for three months, while the user-trades endpoint is limited to recent ranges.
        start_time = max(int(start_time), now - 89 * 86400_000)
        params: Dict[str, Any] = {"symbol": symbol, "startTime": start_time,
                                  "endTime": min(int(end_time or now), now), "limit": min(1000, limit)}
        data = await self._request("GET", "/fapi/v1/income", params, private=True, weight=30)
        return list(data or [])

    async def get_user_trades(self, symbol: str, start_time: int, end_time: Optional[int] = None,
                              limit: int = 1000) -> List[Dict[str, Any]]:
        now = self._now_ms()
        # When querying by time Binance limits userTrades to a seven-day window.
        start_time = max(int(start_time), now - 6 * 86400_000)
        params: Dict[str, Any] = {"symbol": symbol, "startTime": start_time,
                                  "endTime": min(int(end_time or now), now), "limit": min(1000, limit)}
        data = await self._request("GET", "/fapi/v1/userTrades", params, private=True, weight=5)
        return list(data or [])

    async def get_history_positions(
        self,
        symbol: Optional[str] = None,
        page_size: int = 1000,
        position_type: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        side: Optional[str] = None,
        entry_order_id: Optional[str] = None,
        position_margin: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Build a position ledger from Binance income + user-trade history.

        Binance reports REALIZED_PNL, COMMISSION and FUNDING_FEE as separate income
        records, not as one closed-position record. For the bot's one-position-per-symbol
        policy, summing records since the position opened provides the equivalent ledger.
        """
        del position_type, entry_order_id
        if not symbol:
            return []
        now = end_time or self._now_ms()
        opened = start_time or (now - 6 * 86400_000)
        try:
            incomes, trades = await asyncio.gather(
                self.get_income_history(symbol, opened, now, page_size),
                self.get_user_trades(symbol, opened, now, page_size),
            )
        except ExchangeAPIError:
            raise
        except Exception as exc:
            raise ExchangeAPIError(0, f"could not read Binance PnL history: {exc}", "/fapi/v1/income") from exc

        gross = 0.0
        commission_signed = 0.0
        fee_total = 0.0
        funding = 0.0
        have_realized_income = False
        have_commission_income = False
        for row in incomes:
            kind = row.get("incomeType")
            amount = float(row.get("income", 0) or 0)
            if kind == "REALIZED_PNL":
                gross += amount
                have_realized_income = True
            elif kind == "COMMISSION":
                # Only the USDT-denominated fees can be added directly to quote-currency PnL.
                asset = str(row.get("asset", "USDT"))
                if asset == "USDT":
                    commission_signed += amount
                    fee_total += abs(amount)
                    have_commission_income = True
            elif kind == "FUNDING_FEE":
                if str(row.get("asset", "USDT")) == "USDT":
                    funding += amount

        # Income history is authoritative per component. Fall back to executed fills for
        # realized PnL and commissions if that part of the ledger has not caught up yet.
        if not have_realized_income:
            gross = sum(float(t.get("realizedPnl", 0) or 0) for t in trades)
        if not have_commission_income:
            fee_total = sum(abs(float(t.get("commission", 0) or 0)) for t in trades
                            if str(t.get("commissionAsset", "USDT")) == "USDT")
            commission_signed = -fee_total
        has_history = bool(incomes or trades)

        close_side = "SELL" if side == "long" else "BUY" if side == "short" else None
        position_side = "LONG" if side == "long" else "SHORT" if side == "short" else None
        close_trades = [
            t for t in trades
            if (close_side is None or str(t.get("side", "")).upper() == close_side)
            and (position_side is None or str(t.get("positionSide", "BOTH")).upper() in {position_side, "BOTH"})
        ]
        close_qty = sum(float(t.get("qty", 0) or 0) for t in close_trades)
        close_avg = (
            sum(float(t.get("price", 0) or 0) * float(t.get("qty", 0) or 0) for t in close_trades) / close_qty
            if close_qty > 0 else 0.0
        )
        net = gross + commission_signed + funding
        if not has_history:
            return []
        return [{
            "positionId": None,
            "closeAvgPrice": close_avg,
            "closeVol": close_qty,
            "closeProfitLoss": gross,
            "fee": commission_signed,
            "totalFee": fee_total,
            "holdFee": funding,
            "realised": net,
            "profitRatio": (net / position_margin * 100.0) if position_margin > 0 else 0.0,
            "state": 3,
        }]

    # ============================================================= user-data stream
    async def create_listen_key(self) -> str:
        data = await self._request("POST", "/fapi/v1/listenKey", api_key_only=True, weight=1, retry_safe=True)
        key = data.get("listenKey") if isinstance(data, dict) else None
        if not key:
            raise ExchangeAPIError(0, "Binance did not return a user-stream listenKey", "/fapi/v1/listenKey", data)
        return str(key)

    async def keepalive_listen_key(self, listen_key: str) -> None:
        del listen_key  # Binance renews the single active listenKey associated with this API key.
        await self._request("PUT", "/fapi/v1/listenKey", api_key_only=True, weight=1, retry_safe=True)

    async def close_listen_key(self, listen_key: str) -> None:
        del listen_key
        await self._request("DELETE", "/fapi/v1/listenKey", api_key_only=True, weight=1, retry_safe=True)

    # ================================================================== IP helper
    async def public_ip(self) -> Dict[str, Any]:
        """Return the server's public egress IP for the Binance API-key IP allowlist."""
        providers = ["https://api.ipify.org?format=json", "https://api.ip.sb/jsonip",
                     "https://ipinfo.io/json", "https://checkip.amazonaws.com"]
        errors = []
        for url in providers:
            try:
                async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=4),
                                            headers={"User-Agent": "CryptoHunter/1.0"}) as response:
                    text = (await response.text()).strip()
                    try:
                        body = json.loads(text)
                        ip = body.get("ip") or body.get("ip_addr")
                    except json.JSONDecodeError:
                        ip = text if text.count(".") == 3 and len(text) <= 15 else None
                    if ip:
                        return {"ip": ip, "source": url.split("/")[2], "ts": time.time()}
            except Exception as exc:
                errors.append(f"{url.split('/')[2]}: {type(exc).__name__}")
        return {"ip": None, "error": "; ".join(errors)[:300], "ts": time.time()}

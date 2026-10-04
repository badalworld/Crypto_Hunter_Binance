"""Signature-checking in-process Binance USDⓈ-M Futures emulator for tests only."""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs

from aiohttp import web

API_KEY = "binance-test-key-123456"
API_SECRET = "binance-test-secret-1234567890"
TEST_SYMBOL = "TESTUSDT"


class FakeBinance:
    FEE = 0.0005
    FUNDING = -0.0123
    STEP = 0.001

    def __init__(self) -> None:
        self.price = 100.0
        self.wallet_balance = 1000.0
        self.hedge_mode = False
        self.leverage_by_symbol: Dict[str, int] = {}
        self.margin_type_by_symbol: Dict[str, str] = {}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.positions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.algo_orders: Dict[str, Dict[str, Any]] = {}
        self.income: List[Dict[str, Any]] = []
        self.trades: List[Dict[str, Any]] = []
        self.calls: List[str] = []
        self.order_requests: List[Dict[str, str]] = []
        self.leverage_calls: List[Dict[str, Any]] = []
        self.plan_changes: List[Dict[str, Any]] = []
        self._order_id = 700000
        self._algo_id = 800000
        self._trade_id = 900000

    # ------------------------------------------------------------ request/auth helpers
    @staticmethod
    def _params(raw: str) -> Dict[str, str]:
        return {key: values[-1] for key, values in parse_qs(raw, keep_blank_values=True).items()}

    def _auth(self, request: web.Request, raw_params: str) -> Optional[web.Response]:
        path = request.path
        key_only = path == "/fapi/v1/listenKey"
        private = path.startswith("/fapi/v3/") or path in {
            "/fapi/v1/positionSide/dual", "/fapi/v1/marginType", "/fapi/v1/leverage",
            "/fapi/v1/order", "/fapi/v1/allOpenOrders", "/fapi/v1/openAlgoOrders",
            "/fapi/v1/algoOrder", "/fapi/v1/algoOpenOrders", "/fapi/v1/income",
            "/fapi/v1/userTrades",
        }
        if not key_only and not private:
            return None
        if request.headers.get("X-MBX-APIKEY") != API_KEY:
            return web.json_response({"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."}, status=401)
        if key_only:
            return None
        params = self._params(raw_params)
        signature = params.get("signature", "")
        unsigned = "&".join(part for part in raw_params.split("&") if not part.startswith("signature="))
        expected = hmac.new(API_SECRET.encode(), unsigned.encode(), hashlib.sha256).hexdigest()
        if not signature or not hmac.compare_digest(signature, expected):
            return web.json_response({"code": -1022, "msg": "Signature for this request is not valid."}, status=400)
        try:
            timestamp = int(params.get("timestamp", "0"))
        except ValueError:
            timestamp = 0
        if abs(timestamp - int(time.time() * 1000)) > int(params.get("recvWindow", "10000")):
            return web.json_response({"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."}, status=400)
        return None

    @staticmethod
    def _error(code: int, message: str, status: int = 400) -> web.Response:
        return web.json_response({"code": code, "msg": message}, status=status)

    @staticmethod
    def _json_num(value: float) -> str:
        return format(float(value), ".15g")

    def _new_order_id(self) -> str:
        self._order_id += 1
        return str(self._order_id)

    def _new_trade_id(self) -> int:
        self._trade_id += 1
        return self._trade_id

    def _record_income(self, symbol: str, kind: str, amount: float) -> None:
        self.income.append({
            "symbol": symbol, "incomeType": kind, "income": self._json_num(amount),
            "asset": "USDT", "time": int(time.time() * 1000), "tranId": self._trade_id + 1,
        })

    def _position_key(self, symbol: str, position_side: str = "BOTH") -> Tuple[str, str]:
        return symbol, position_side if self.hedge_mode else "BOTH"

    def _open_position(self, symbol: str, side: str, quantity: float, leverage: int,
                       position_side: Optional[str] = None, client_order_id: str = "") -> Dict[str, Any]:
        side = side.lower()
        position_side = (position_side or ("LONG" if side == "long" else "SHORT")) if self.hedge_mode else "BOTH"
        key = self._position_key(symbol, position_side)
        now_ms = int(time.time() * 1000)
        fee = quantity * self.price * self.FEE
        self.wallet_balance -= fee
        position = {
            "symbol": symbol, "positionSide": position_side, "side": side,
            "quantity": quantity, "entryPrice": self.price, "leverage": int(leverage),
            "marginType": self.margin_type_by_symbol.get(symbol, "isolated").lower(),
            "openedAt": now_ms, "openFee": fee, "key": key,
        }
        existing = self.positions.get(key)
        if existing and existing["side"] == side:
            total = existing["quantity"] + quantity
            existing["entryPrice"] = (existing["entryPrice"] * existing["quantity"] + self.price * quantity) / total
            existing["quantity"] = total
            existing["openFee"] += fee
            position = existing
        else:
            self.positions[key] = position

        entry_trade_id = self._new_trade_id()
        self.trades.append({
            "symbol": symbol, "id": entry_trade_id, "orderId": int(self._order_id),
            "side": "BUY" if side == "long" else "SELL", "positionSide": position_side,
            "price": self._json_num(self.price), "qty": self._json_num(quantity),
            "quoteQty": self._json_num(quantity * self.price), "realizedPnl": "0",
            "commission": self._json_num(fee), "commissionAsset": "USDT", "time": now_ms,
            "buyer": side == "long", "maker": False,
        })
        self._record_income(symbol, "COMMISSION", -fee)
        # One simulated funding settlement keeps the integration test's accounting path
        # representative of Binance's separate FUNDING_FEE income records.
        self.wallet_balance += self.FUNDING
        self._record_income(symbol, "FUNDING_FEE", self.FUNDING)
        return position

    def _close_position(self, position: Dict[str, Any], price: float, quantity: Optional[float] = None,
                        close_order_id: Optional[str] = None) -> float:
        key = position["key"]
        qty = min(position["quantity"], quantity if quantity is not None else position["quantity"])
        qty = max(0.0, qty)
        if qty <= 0:
            return 0.0
        gross = ((price - position["entryPrice"]) if position["side"] == "long"
                 else (position["entryPrice"] - price)) * qty
        fee = qty * price * self.FEE
        self.wallet_balance += gross - fee
        order_id = close_order_id or self._new_order_id()
        trade_id = self._new_trade_id()
        close_side = "SELL" if position["side"] == "long" else "BUY"
        self.trades.append({
            "symbol": position["symbol"], "id": trade_id, "orderId": int(order_id),
            "side": close_side, "positionSide": position["positionSide"],
            "price": self._json_num(price), "qty": self._json_num(qty),
            "quoteQty": self._json_num(qty * price), "realizedPnl": self._json_num(gross),
            "commission": self._json_num(fee), "commissionAsset": "USDT",
            "time": int(time.time() * 1000), "buyer": close_side == "BUY", "maker": False,
        })
        self._record_income(position["symbol"], "REALIZED_PNL", gross)
        self._record_income(position["symbol"], "COMMISSION", -fee)
        position["quantity"] -= qty
        if position["quantity"] <= self.STEP / 2:
            self.positions.pop(key, None)
            for algo in self.algo_orders.values():
                if (algo.get("symbol") == position["symbol"]
                        and algo.get("positionSide", "BOTH") == position["positionSide"]
                        and algo.get("algoStatus") == "NEW"):
                    algo["algoStatus"] = "CANCELED"
        return gross

    def open_manual_position(self, symbol: str = TEST_SYMBOL, side: str = "long",
                             quantity: float = 50.0, leverage: int = 5) -> Dict[str, Any]:
        return self._open_position(symbol, side, quantity, leverage)

    def add_manual_algo(self, symbol: str = TEST_SYMBOL, side: str = "SELL", position_side: str = "BOTH",
                        order_type: str = "STOP_MARKET", trigger_price: float = 90.0,
                        client_algo_id: str = "manual-protection") -> Dict[str, Any]:
        self._algo_id += 1
        row = {
            "algoId": self._algo_id, "algoType": "CONDITIONAL", "symbol": symbol,
            "side": side.upper(), "positionSide": position_side.upper(), "orderType": order_type.upper(),
            "triggerPrice": self._json_num(trigger_price), "workingType": "MARK_PRICE",
            "closePosition": True, "clientAlgoId": client_algo_id, "algoStatus": "NEW",
        }
        self.algo_orders[str(self._algo_id)] = row
        return row

    def set_price(self, price: float) -> None:
        self.price = float(price)
        self._check_algo_triggers()

    def _check_algo_triggers(self) -> None:
        for algo in list(self.algo_orders.values()):
            if algo.get("algoStatus") != "NEW":
                continue
            symbol = algo["symbol"]
            position_side = algo.get("positionSide", "BOTH")
            key = self._position_key(symbol, position_side)
            position = self.positions.get(key)
            if not position:
                continue
            trigger = float(algo.get("triggerPrice") or 0)
            order_type = algo.get("orderType", "")
            if position["side"] == "long":
                hit = self.price <= trigger if order_type.startswith("STOP") else self.price >= trigger
            else:
                hit = self.price >= trigger if order_type.startswith("STOP") else self.price <= trigger
            if not hit:
                continue
            algo["algoStatus"] = "FINISHED"
            algo["actualOrderId"] = self._new_order_id()
            self._close_position(position, trigger, close_order_id=algo["actualOrderId"])
            break

    # ------------------------------------------------------------ HTTP handlers
    async def handle(self, request: web.Request) -> web.Response:
        body = await request.text()
        raw_params = request.rel_url.raw_query_string if request.method in {"GET", "DELETE", "HEAD"} else body
        path = request.path
        self.calls.append(f"{request.method} {path}")
        auth_error = self._auth(request, raw_params)
        if auth_error is not None:
            return auth_error
        params = self._params(raw_params)
        symbol = params.get("symbol", TEST_SYMBOL)

        if path == "/fapi/v1/ping":
            return web.json_response({})
        if path == "/fapi/v1/time":
            return web.json_response({"serverTime": int(time.time() * 1000)})
        if path == "/fapi/v1/exchangeInfo":
            return web.json_response({"timezone": "UTC", "serverTime": int(time.time() * 1000), "symbols": [{
                "symbol": TEST_SYMBOL, "pair": TEST_SYMBOL, "contractType": "PERPETUAL", "status": "TRADING",
                "baseAsset": "TEST", "quoteAsset": "USDT", "marginAsset": "USDT",
                "pricePrecision": 2, "quantityPrecision": 3, "triggerProtect": "0.05",
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000000", "stepSize": "0.001"},
                    {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "1000000", "stepSize": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }]})
        if path == "/fapi/v1/ticker/24hr":
            row = {
                "symbol": TEST_SYMBOL, "priceChange": "0.20", "priceChangePercent": "0.20",
                "weightedAvgPrice": self._json_num(self.price), "lastPrice": self._json_num(self.price),
                "lastQty": "1", "openPrice": "99.8", "highPrice": "101", "lowPrice": "99",
                "volume": "100000", "quoteVolume": "50000000", "openTime": int(time.time() * 1000) - 86_400_000,
                "closeTime": int(time.time() * 1000), "firstId": 1, "lastId": 100, "count": 100,
            }
            return web.json_response(row if params.get("symbol") else [row])
        if path == "/fapi/v1/ticker/bookTicker":
            row = {"symbol": TEST_SYMBOL, "bidPrice": self._json_num(self.price - 0.01), "bidQty": "100",
                   "askPrice": self._json_num(self.price + 0.01), "askQty": "100", "time": int(time.time() * 1000)}
            return web.json_response(row if params.get("symbol") else [row])
        if path == "/fapi/v1/premiumIndex":
            row = {"symbol": TEST_SYMBOL, "markPrice": self._json_num(self.price), "indexPrice": self._json_num(self.price),
                   "lastFundingRate": "0", "nextFundingTime": int(time.time() * 1000) + 3_600_000,
                   "time": int(time.time() * 1000)}
            return web.json_response(row if params.get("symbol") else [row])
        if path == "/fapi/v1/klines":
            return web.json_response(self._klines(params))
        if path == "/fapi/v3/account":
            return web.json_response(self._account())
        if path == "/fapi/v3/positionRisk":
            rows = self._positions_risk(symbol if params.get("symbol") else None)
            return web.json_response(rows)
        if path == "/fapi/v1/positionSide/dual":
            return web.json_response({"dualSidePosition": self.hedge_mode})
        if path == "/fapi/v1/marginType" and request.method == "POST":
            margin_type = params.get("marginType", "ISOLATED").lower()
            self.margin_type_by_symbol[symbol] = "cross" if margin_type == "crossed" else "isolated"
            return web.json_response({"code": 200, "msg": "success"})
        if path == "/fapi/v1/leverage" and request.method == "POST":
            leverage = int(params.get("leverage", "1"))
            self.leverage_by_symbol[symbol] = leverage
            call = {"symbol": symbol, "leverage": leverage}
            self.leverage_calls.append(call)
            return web.json_response({"symbol": symbol, "leverage": leverage, "maxNotionalValue": "1000000"})
        if path == "/fapi/v1/order":
            if request.method == "POST":
                return self._create_order(params)
            if request.method == "GET":
                order = self.orders.get(str(params.get("orderId", "")))
                if order is None and params.get("origClientOrderId"):
                    order = next((o for o in self.orders.values()
                                  if o.get("clientOrderId") == params["origClientOrderId"]), None)
                return web.json_response(order) if order else self._error(-2013, "Order does not exist.")
            if request.method == "DELETE":
                order = self.orders.get(str(params.get("orderId", "")))
                if not order:
                    return self._error(-2011, "Unknown order sent.")
                order["status"] = "CANCELED"
                return web.json_response(order)
        if path == "/fapi/v1/allOpenOrders" and request.method == "DELETE":
            return web.json_response({"code": 200, "msg": "The operation of cancel all open orders was successful."})
        if path == "/fapi/v1/openAlgoOrders" and request.method == "GET":
            rows = [dict(x) for x in self.algo_orders.values() if x.get("algoStatus") == "NEW"
                    and (not params.get("symbol") or x.get("symbol") == params["symbol"])]
            return web.json_response(rows)
        if path == "/fapi/v1/algoOrder":
            if request.method == "POST":
                return self._create_algo(params)
            if request.method == "DELETE":
                algo_id = str(params.get("algoid", ""))
                algo = self.algo_orders.get(algo_id)
                if not algo:
                    return self._error(-2011, "Unknown order sent.")
                if algo.get("algoStatus") == "NEW":
                    algo["algoStatus"] = "CANCELED"
                return web.json_response({**algo, "code": 200})
        if path == "/fapi/v1/algoOpenOrders" and request.method == "DELETE":
            for algo in self.algo_orders.values():
                if algo.get("symbol") == symbol and algo.get("algoStatus") == "NEW":
                    algo["algoStatus"] = "CANCELED"
            return web.json_response({"code": 200, "msg": "success"})
        if path == "/fapi/v1/income" and request.method == "GET":
            start = int(params.get("startTime", "0"))
            end = int(params.get("endTime", str(int(time.time() * 1000))))
            rows = [x for x in self.income if x["symbol"] == symbol and start <= x["time"] <= end]
            return web.json_response(rows[-int(params.get("limit", "1000")):])
        if path == "/fapi/v1/userTrades" and request.method == "GET":
            start = int(params.get("startTime", "0"))
            end = int(params.get("endTime", str(int(time.time() * 1000))))
            rows = [x for x in self.trades if x["symbol"] == symbol and start <= x["time"] <= end]
            return web.json_response(rows[-int(params.get("limit", "1000")):])
        if path == "/fapi/v1/listenKey":
            if request.method == "POST":
                return web.json_response({"listenKey": "test-listen-key"})
            if request.method == "PUT":
                return web.json_response({"listenKey": "test-listen-key"})
            if request.method == "DELETE":
                return web.json_response({})
        return self._error(-404, f"Unknown fake Binance endpoint: {request.method} {path}", 404)

    def _klines(self, params: Dict[str, str]) -> List[List[Any]]:
        seconds = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
                   "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800,
                   "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800}.get(params.get("interval", "5m"), 300)
        now = int(time.time())
        start = int(params.get("startTime", str((now - seconds * 500) * 1000))) // 1000
        end = int(params.get("endTime", str(now * 1000))) // 1000
        first = start // seconds * seconds
        limit = min(int(params.get("limit", "500")), 1500)
        rows = []
        stamp = first
        while stamp < end and len(rows) < limit:
            close_time = (stamp + seconds) * 1000 - 1
            close = self.price
            rows.append([
                stamp * 1000, self._json_num(close), self._json_num(close + 0.5), self._json_num(close - 0.5),
                self._json_num(close), "1000", close_time, self._json_num(1000 * close), 100,
                "500", self._json_num(500 * close), "0",
            ])
            stamp += seconds
        return rows

    def _account(self) -> Dict[str, Any]:
        unrealized = 0.0
        initial_margin = 0.0
        for position in self.positions.values():
            quantity = position["quantity"]
            direction = 1 if position["side"] == "long" else -1
            unrealized += (self.price - position["entryPrice"]) * quantity * direction
            initial_margin += quantity * position["entryPrice"] / position["leverage"]
        return {
            "feeTier": 0, "canTrade": True, "canDeposit": True, "canWithdraw": True,
            "totalWalletBalance": self._json_num(self.wallet_balance),
            "totalUnrealizedProfit": self._json_num(unrealized),
            "totalMarginBalance": self._json_num(self.wallet_balance + unrealized),
            "totalPositionInitialMargin": self._json_num(initial_margin),
            "totalOpenOrderInitialMargin": "0", "totalCrossWalletBalance": self._json_num(self.wallet_balance),
            "availableBalance": self._json_num(self.wallet_balance + unrealized - initial_margin),
            "maxWithdrawAmount": self._json_num(self.wallet_balance + unrealized - initial_margin),
            "assets": [{
                "asset": "USDT", "walletBalance": self._json_num(self.wallet_balance),
                "unrealizedProfit": self._json_num(unrealized),
                "marginBalance": self._json_num(self.wallet_balance + unrealized),
                "maintMargin": "0", "initialMargin": self._json_num(initial_margin),
                "positionInitialMargin": self._json_num(initial_margin), "openOrderInitialMargin": "0",
                "maxWithdrawAmount": self._json_num(self.wallet_balance + unrealized - initial_margin),
                "crossWalletBalance": self._json_num(self.wallet_balance),
                "crossUnPnl": self._json_num(unrealized), "availableBalance": self._json_num(self.wallet_balance + unrealized - initial_margin),
                "marginAvailable": True, "updateTime": int(time.time() * 1000),
            }],
            "positions": [],
        }

    def _positions_risk(self, symbol: Optional[str]) -> List[Dict[str, Any]]:
        rows = []
        for position in self.positions.values():
            if symbol and position["symbol"] != symbol:
                continue
            direction = 1 if position["side"] == "long" else -1
            unrealized = (self.price - position["entryPrice"]) * position["quantity"] * direction
            margin = position["quantity"] * position["entryPrice"] / position["leverage"]
            rows.append({
                "symbol": position["symbol"], "positionSide": position["positionSide"],
                "positionAmt": self._json_num(position["quantity"] * direction),
                "entryPrice": self._json_num(position["entryPrice"]), "breakEvenPrice": self._json_num(position["entryPrice"]),
                "markPrice": self._json_num(self.price), "unRealizedProfit": self._json_num(unrealized),
                "liquidationPrice": "0", "leverage": str(position["leverage"]),
                "maxNotionalValue": "1000000", "marginType": position["marginType"],
                "isolatedMargin": self._json_num(margin), "isAutoAddMargin": "false",
                "isolatedWallet": self._json_num(margin), "positionInitialMargin": self._json_num(margin),
                "openOrderInitialMargin": "0", "maintMargin": "0", "updateTime": int(time.time() * 1000),
            })
        return rows

    def _create_order(self, params: Dict[str, str]) -> web.Response:
        params = dict(params)
        self.order_requests.append(params)
        symbol = params.get("symbol", TEST_SYMBOL)
        side = params.get("side", "").upper()
        order_type = params.get("type", "MARKET").upper()
        position_side = params.get("positionSide", "BOTH").upper()
        try:
            quantity = float(params.get("quantity", "0"))
        except ValueError:
            return self._error(-1013, "Invalid quantity.")
        reduce_only = params.get("reduceOnly", "false").lower() == "true"
        if quantity <= 0:
            return self._error(-1013, "Quantity must be positive.")
        key = self._position_key(symbol, position_side)
        position = self.positions.get(key)
        is_close = bool(position and (
            reduce_only or (position_side in {"LONG", "SHORT"} and
                            ((position["side"] == "long" and side == "SELL")
                             or (position["side"] == "short" and side == "BUY")))
        ))
        if reduce_only and not position:
            return self._error(-2022, "ReduceOnly Order is rejected.")
        if position and not is_close and position_side == "BOTH" and position["side"] != ("long" if side == "BUY" else "short"):
            return self._error(-2022, "ReduceOnly Order is rejected.")

        order_id = self._new_order_id()
        if is_close:
            fill_qty = min(quantity, position["quantity"])
            self._close_position(position, self.price, fill_qty, order_id)
        else:
            open_side = "long" if position_side == "LONG" else "short" if position_side == "SHORT" else ("long" if side == "BUY" else "short")
            leverage = int(self.leverage_by_symbol.get(symbol, 10))
            self._open_position(symbol, open_side, quantity, leverage, position_side, params.get("newClientOrderId", ""))

        order = {
            "orderId": int(order_id), "symbol": symbol, "status": "FILLED", "clientOrderId": params.get("newClientOrderId", ""),
            "price": "0", "avgPrice": self._json_num(self.price), "origQty": self._json_num(quantity),
            "executedQty": self._json_num(quantity), "cumQuote": self._json_num(quantity * self.price),
            "timeInForce": params.get("timeInForce", "GTC"), "type": order_type, "side": side,
            "positionSide": position_side, "reduceOnly": reduce_only, "workingType": "CONTRACT_PRICE",
            "updateTime": int(time.time() * 1000), "time": int(time.time() * 1000),
        }
        self.orders[order_id] = order
        return web.json_response(order)

    def _create_algo(self, params: Dict[str, str]) -> web.Response:
        self._algo_id += 1
        algo_id = str(self._algo_id)
        try:
            trigger = float(params.get("triggerPrice", "0"))
        except ValueError:
            return self._error(-1102, "Mandatory parameter triggerPrice was not sent.")
        row = {
            "algoId": int(algo_id), "algoType": params.get("algoType", "CONDITIONAL"),
            "symbol": params.get("symbol", TEST_SYMBOL), "side": params.get("side", "").upper(),
            "positionSide": params.get("positionSide", "BOTH").upper(),
            "orderType": params.get("type", "").upper(), "triggerPrice": self._json_num(trigger),
            "workingType": params.get("workingType", "MARK_PRICE"),
            "closePosition": params.get("closePosition", "false").lower() == "true",
            "priceProtect": params.get("priceProtect", "false").lower() == "true",
            "clientAlgoId": params.get("clientAlgoId", ""), "algoStatus": "NEW",
            "createTime": int(time.time() * 1000),
        }
        self.algo_orders[algo_id] = row
        self.plan_changes.append({"symbol": row["symbol"], "type": row["orderType"],
                                  "side": row["side"], "triggerPrice": trigger,
                                  "clientAlgoId": row["clientAlgoId"]})
        return web.json_response({"algoId": int(algo_id), "clientAlgoId": row["clientAlgoId"],
                                  "algoStatus": "NEW", "success": True})


async def start_fake(port: int = 0):
    fake = FakeBinance()
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", fake.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    actual_port = site._server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    return fake, runner, f"http://127.0.0.1:{actual_port}"

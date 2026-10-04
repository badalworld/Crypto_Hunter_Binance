"""Binance USDⓈ-M Futures order execution.

Market/IOC entries use ``POST /fapi/v1/order``. Binance does not support attaching a
bracket to that entry order, so the executor arms independent close-position stop and
TP algo orders immediately after the fill using ``POST /fapi/v1/algoOrder``. Trailing
ratchets place the replacement stop first and then cancel the previous stop, avoiding
an intentional unprotected gap. Every close is direction-bound and reduce-only in
one-way mode; hedge mode uses LONG/SHORT positionSide.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..config import BotConfig
from ..exchange.models import Contract, ExchangeAPIError
from ..exchange.rest import BinanceFuturesREST

log = logging.getLogger("ch.executor")

SIDE_OPEN = {"long": 1, "short": 3}
SIDE_CLOSE = {"long": 4, "short": 2}
POSITION_TYPE = {"long": 1, "short": 2}
STATE_NEW, STATE_PARTIAL = 1, 2
STATE_FILLED, STATE_CANCELED, STATE_INVALID = 3, 4, 5


class PositionGone(Exception):
    """Raised when a protection update is attempted after the exchange position closed."""


@dataclass
class FillResult:
    order_id: str
    avg_price: float
    filled_vol: float
    position_id: Optional[int]
    state: int
    raw: Dict[str, Any]


class OrderExecutor:
    def __init__(self, rest: BinanceFuturesREST, cfg: BotConfig):
        self.rest = rest
        self.cfg = cfg
        self.position_mode: int = 1  # 1 hedge, 2 one-way
        self._leverage_set: Dict[str, float] = {}

    def update_config(self, cfg: BotConfig) -> None:
        self.cfg = cfg

    async def detect_position_mode(self) -> int:
        self.position_mode = await self.rest.get_position_mode()
        return self.position_mode

    # ------------------------------------------------------------------ leverage
    async def ensure_leverage(self, symbol: str, side: str, leverage: int) -> None:
        key = f"{symbol}:{side}:{leverage}:{self.cfg.risk.open_type}"
        if time.time() - self._leverage_set.get(key, 0) < 3600:
            return
        # A rejected margin/leverage change is not assumed benign: the entry must not be
        # submitted at an unknown risk setting.
        await self.rest.change_margin_type(symbol, self.cfg.risk.open_type)
        await self.rest.change_leverage(symbol, leverage)
        self._leverage_set[key] = time.time()

    # --------------------------------------------------------------------- entry
    async def open_position(self, symbol: str, side: str, vol: float, ref_price: float, leverage: int,
                            stop_loss: float, take_profit: float, contract: Contract) -> FillResult:
        del stop_loss, take_profit  # Binance brackets are placed after the fill as Algo orders.
        await self.ensure_leverage(symbol, side, leverage)
        client_id = f"CH{uuid.uuid4().hex[:18]}"
        order_type = 5 if self.cfg.execution.entry_order_type == "market" else 3
        price = None
        if order_type == 3:  # IOC uses an aggressive, tick-rounded limit price.
            slip = 0.003
            price = contract.round_price(
                ref_price * (1 + slip) if side == "long" else ref_price * (1 - slip),
                "up" if side == "long" else "down",
            )
        t0 = time.perf_counter()
        try:
            res = await self.rest.create_order(
                symbol=symbol, vol=vol, side=SIDE_OPEN[side], order_type=order_type,
                price=price, external_oid=client_id, position_mode=self.position_mode,
            )
        except ExchangeAPIError as exc:
            # A transport timeout can happen after Binance accepted the order. Resolve by
            # the unique client ID before returning an error, avoiding duplicate exposure.
            try:
                res = await self.rest.get_order_by_client_id(symbol, client_id)
            except ExchangeAPIError:
                raise exc
            if not res:
                raise exc
        order_id = str(res.get("orderId") or "")
        if not order_id:
            raise ExchangeAPIError(0, "Binance order response did not include orderId", "/fapi/v1/order", res)
        status = str(res.get("status", "")).upper()
        filled = self._filled_qty(res)
        if status == "FILLED" or (filled > 0 and status not in {"NEW", "PARTIALLY_FILLED"}):
            result = self._fill_result(order_id, res)
        else:
            result = await self.await_fill(symbol, order_id, initial=res)
        log.info("ENTRY %s %s qty=%s sent in %.0fms -> order %s, fill=%s @ %s", side.upper(), symbol, vol,
                 (time.perf_counter() - t0) * 1000, order_id, result.filled_vol, result.avg_price)
        return result

    @staticmethod
    def _filled_qty(row: Dict[str, Any]) -> float:
        try:
            return float(row.get("executedQty", row.get("dealVol", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _fill_result(cls, order_id: str, row: Dict[str, Any]) -> FillResult:
        qty = cls._filled_qty(row)
        try:
            avg = float(row.get("avgPrice", row.get("dealAvgPrice", 0)) or 0)
        except (TypeError, ValueError):
            avg = 0.0
        if avg <= 0 and qty > 0:
            try:
                quote = float(row.get("cumQuote", 0) or 0)
                if quote > 0:
                    avg = quote / qty
            except (TypeError, ValueError):
                pass
        status = str(row.get("status", "")).upper()
        state = {
            "FILLED": STATE_FILLED,
            "CANCELED": STATE_CANCELED,
            "EXPIRED": STATE_CANCELED,
            "EXPIRED_IN_MATCH": STATE_CANCELED,
            "REJECTED": STATE_INVALID,
            "PARTIALLY_FILLED": STATE_PARTIAL,
        }.get(status, STATE_NEW if status in {"NEW", ""} else 0)
        return FillResult(
            order_id=order_id, avg_price=avg, filled_vol=qty, position_id=None,
            state=state, raw=row,
        )

    async def await_fill(self, symbol: str, order_id: str, timeout: float = 8.0,
                         initial: Optional[Dict[str, Any]] = None) -> FillResult:
        deadline = time.time() + timeout
        last: Dict[str, Any] = initial or {}
        delay = 0.15
        while time.time() < deadline:
            try:
                last = await self.rest.get_order(order_id, symbol)
            except ExchangeAPIError as exc:
                log.debug("query order %s: %s", order_id, exc.message)
            status = str(last.get("status", "")).upper()
            if status in {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 1.0)
        else:
            status = str(last.get("status", "")).upper()
            if status in {"NEW", "PARTIALLY_FILLED"}:
                try:
                    await self.rest.cancel_orders(symbol, [order_id])
                    last = await self.rest.get_order(order_id, symbol)
                except ExchangeAPIError as exc:
                    log.warning("could not cancel unresolved entry %s: %s", order_id, exc.message)
        return self._fill_result(order_id, last)

    # ----------------------------------------------------------------- protection
    @staticmethod
    def _algo_id(order: Dict[str, Any]) -> Optional[str]:
        value = order.get("algoId", order.get("id"))
        return str(value) if value not in (None, "") else None

    @staticmethod
    def _algo_type(order: Dict[str, Any]) -> str:
        return str(order.get("orderType", order.get("type", ""))).upper()

    @staticmethod
    def _trigger(order: Dict[str, Any]) -> float:
        try:
            return float(order.get("triggerPrice", order.get("stopPrice", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    def _working_type(self) -> str:
        return "MARK_PRICE" if self.cfg.execution.price_trend_source == "fair" else "CONTRACT_PRICE"

    def _matching_orders(self, orders: list[Dict[str, Any]], symbol: str, side: str,
                         trusted_ids: Optional[set[str]] = None) -> list[Dict[str, Any]]:
        close_side = "SELL" if side == "long" else "BUY"
        expected_position_side = "LONG" if side == "long" else "SHORT"
        trusted_ids = trusted_ids or set()
        out = []
        for order in orders:
            if str(order.get("symbol", symbol)) != symbol:
                continue
            if str(order.get("side", "")).upper() != close_side:
                continue
            position_side = str(order.get("positionSide", "BOTH")).upper()
            if position_side not in {expected_position_side, "BOTH"}:
                continue
            if str(order.get("algoStatus", "NEW")).upper() not in {"NEW", "WORKING", "PENDING"}:
                continue
            # Only manage this bot's Algo orders (or an ID explicitly persisted by it).
            # An unrelated manual TP/SL must never be modified or canceled on adoption.
            client_id = str(order.get("clientAlgoId", ""))
            if not client_id.startswith("CH") and self._algo_id(order) not in trusted_ids:
                continue
            out.append(order)
        return out

    async def find_stop_plan_order(self, symbol: str, position_id: Optional[int],
                                   side: Optional[str] = None) -> Optional[Dict[str, Any]]:
        del position_id  # Binance futures positions do not have a positionId.
        try:
            orders = await self.rest.get_stop_open_orders(symbol)
        except ExchangeAPIError as exc:
            log.warning("open algo orders for %s failed: %s", symbol, exc.message)
            return None
        live = [o for o in orders if str(o.get("symbol", symbol)) == symbol]
        if side:
            live = self._matching_orders(live, symbol, side)
        stop = next((o for o in reversed(live) if self._algo_type(o) in {"STOP_MARKET", "STOP"}), None)
        target = next((o for o in reversed(live) if self._algo_type(o) in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}), None)
        sl_id = self._algo_id(stop) if stop else None
        tp_id = self._algo_id(target) if target else None
        if not sl_id and not tp_id:
            return None
        return {
            "id": sl_id or tp_id,
            "sl_id": sl_id,
            "tp_id": tp_id,
            "stopLossPrice": self._trigger(stop) if stop else None,
            "takeProfitPrice": self._trigger(target) if target else None,
            "stop_order": stop,
            "take_profit_order": target,
        }

    async def _place_close_algo(self, symbol: str, side: str, order_type: str, trigger_price: float,
                                contract: Contract) -> str:
        close_side = "SELL" if side == "long" else "BUY"
        trigger = contract.round_price(trigger_price, "down" if side == "long" else "up")
        if order_type == "TAKE_PROFIT_MARKET":
            trigger = contract.round_price(trigger_price, "up" if side == "long" else "down")
        response = await self.rest.place_algo_order(
            symbol=symbol,
            side=close_side,
            position_mode=self.position_mode,
            order_type=order_type,
            trigger_price=trigger,
            working_type=self._working_type(),
            client_algo_id=f"CH{uuid.uuid4().hex[:18]}",
        )
        algo_id = self._algo_id(response)
        if not algo_id:
            raise ExchangeAPIError(0, f"Binance did not confirm {order_type} algoId", "/fapi/v1/algoOrder", response)
        return algo_id

    async def update_stop(self, symbol: str, side: str, stop_plan_order_id: Optional[str], new_stop: float,
                          take_profit: float, contract: Contract, position_id: Optional[int] = None,
                          vol: float = 0.0, leverage: int = 1) -> Dict[str, Any]:
        """Ratchet the exchange SL and ensure its fixed TP; never move the stop backwards."""
        del vol, leverage
        sl = contract.round_price(new_stop, "down" if side == "long" else "up")
        tp = contract.round_price(take_profit, "up" if side == "long" else "down")

        live = []
        for delay in (0.0, 0.2, 0.5, 1.0):
            if delay:
                await asyncio.sleep(delay)
            live = [p for p in await self.rest.get_open_positions(symbol)
                    if p.side == side and p.hold_vol > 0]
            if live:
                break
        if not live:
            raise PositionGone(f"{symbol} {side} is no longer open")

        trusted_ids = {str(stop_plan_order_id)} if stop_plan_order_id else set()
        orders = self._matching_orders(await self.rest.get_stop_open_orders(symbol), symbol, side, trusted_ids)
        sl_orders = [o for o in orders if self._algo_type(o) in {"STOP_MARKET", "STOP"}]
        tp_orders = [o for o in orders if self._algo_type(o) in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}]
        old_sl = next((o for o in sl_orders if self._algo_id(o) == str(stop_plan_order_id)), None)
        if old_sl is None and sl_orders:
            # Prefer an order already at the requested price, otherwise the latest live stop.
            old_sl = next((o for o in reversed(sl_orders) if abs(self._trigger(o) - sl) <= contract.price_unit / 2), None)
            old_sl = old_sl or sl_orders[-1]
        old_tp = tp_orders[-1] if tp_orders else None

        old_sl_id = self._algo_id(old_sl) if old_sl else None
        old_tp_id = self._algo_id(old_tp) if old_tp else None
        if old_sl and abs(self._trigger(old_sl) - sl) <= contract.price_unit / 2:
            sl_id = old_sl_id
        else:
            # Place the new protection first. If this fails, the previous stop remains active.
            sl_id = await self._place_close_algo(symbol, side, "STOP_MARKET", sl, contract)
            if old_sl_id and old_sl_id != sl_id:
                try:
                    await self.rest.cancel_algo_order(old_sl_id)
                except ExchangeAPIError as exc:
                    log.warning("could not retire replaced SL %s for %s: %s", old_sl_id, symbol, exc.message)

        if old_tp and abs(self._trigger(old_tp) - tp) <= contract.price_unit / 2:
            tp_id = old_tp_id
        else:
            tp_id = await self._place_close_algo(symbol, side, "TAKE_PROFIT_MARKET", tp, contract)
            if old_tp_id and old_tp_id != tp_id:
                try:
                    await self.rest.cancel_algo_order(old_tp_id)
                except ExchangeAPIError as exc:
                    log.warning("could not retire replaced TP %s for %s: %s", old_tp_id, symbol, exc.message)

        return {
            "stop_plan_order_id": sl_id,
            "sl_plan_order_id": sl_id,
            "tp_plan_order_id": tp_id,
            "stop_price": sl,
            "take_profit_price": tp,
        }

    # --------------------------------------------------------------------- close
    async def close_position(self, symbol: str, side: str, vol: float, ref_price: float,
                             position_id: Optional[int], contract: Contract) -> FillResult:
        del ref_price, position_id
        res = await self.rest.create_order(
            symbol=symbol, vol=contract.round_vol(vol), side=SIDE_CLOSE[side], order_type=5,
            position_mode=self.position_mode,
            reduce_only=(self.position_mode == 2),
            external_oid=f"CHX{uuid.uuid4().hex[:17]}",
        )
        order_id = str(res.get("orderId") or "")
        if not order_id:
            raise ExchangeAPIError(0, "Binance close response did not include orderId", "/fapi/v1/order", res)
        status = str(res.get("status", "")).upper()
        if status == "FILLED" or self._filled_qty(res) > 0 and status not in {"NEW", "PARTIALLY_FILLED"}:
            return self._fill_result(order_id, res)
        return await self.await_fill(symbol, order_id, initial=res)

    async def cancel_protection(self, symbol: str, position_id: Optional[int], plan_ids: list[str]) -> None:
        del position_id
        ids = [str(value) for value in plan_ids if value]
        if not ids:
            return
        try:
            await self.rest.cancel_stop_orders(ids)
        except ExchangeAPIError as exc:
            log.debug("cancel Binance algo orders for %s failed: %s", symbol, exc.message)

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, Optional


def _decimal_places(value: float) -> int:
    return max(0, -Decimal(str(value)).normalize().as_tuple().exponent)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


@dataclass
class Contract:
    """Trading filters for a Binance USDⓈ-M perpetual contract.

    Binance quantities are expressed in base-asset units (unlike inverse contracts),
    so ``contract_size`` is 1 for the USDT-M symbols this application supports.
    """

    symbol: str
    base_coin: str
    quote_coin: str
    settle_coin: str
    contract_size: float
    price_unit: float
    vol_unit: float
    min_vol: float
    max_vol: float
    price_scale: int
    vol_scale: int
    max_leverage: int
    min_leverage: int
    taker_fee: float
    maker_fee: float
    state: int
    api_allowed: bool
    min_notional: float = 5.0
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "Contract":
        filters = {x.get("filterType"): x for x in d.get("filters", []) if isinstance(x, dict)}
        price_filter = filters.get("PRICE_FILTER", {})
        market_lot = filters.get("MARKET_LOT_SIZE", {})
        lot = filters.get("LOT_SIZE", {})
        # MARKET_LOT_SIZE can be all zeros for some symbols; fall back to LOT_SIZE.
        qty_filter = market_lot if _float(market_lot.get("stepSize")) > 0 else lot
        tick = _float(price_filter.get("tickSize"), 10 ** -int(d.get("pricePrecision", 2)))
        step = _float(qty_filter.get("stepSize"), 10 ** -int(d.get("quantityPrecision", 0)))
        min_notional = _float(
            filters.get("MIN_NOTIONAL", {}).get("notional")
            or filters.get("NOTIONAL", {}).get("minNotional"), 5.0,
        )
        status = d.get("status")
        active = status == "TRADING" if status is not None else bool(d.get("apiAllowed", True))
        return cls(
            symbol=str(d.get("symbol", "")),
            base_coin=str(d.get("baseAsset", d.get("baseCoin", ""))),
            quote_coin=str(d.get("quoteAsset", d.get("quoteCoin", ""))),
            settle_coin=str(d.get("marginAsset", d.get("settleCoin", d.get("quoteAsset", "")))),
            contract_size=1.0,
            price_unit=tick,
            vol_unit=step,
            min_vol=_float(qty_filter.get("minQty", lot.get("minQty", 0.0))),
            max_vol=_float(qty_filter.get("maxQty", lot.get("maxQty", 1e12)), 1e12),
            price_scale=_decimal_places(tick),
            vol_scale=_decimal_places(step),
            # Leverage limits are account- and notional-dependent on Binance. The configured
            # leverage is validated by POST /fapi/v1/leverage before an entry is submitted.
            max_leverage=125,
            min_leverage=1,
            taker_fee=_float(d.get("takerFeeRate"), 0.0005),
            maker_fee=_float(d.get("makerFeeRate"), 0.0002),
            state=0 if active else 1,
            api_allowed=bool(active and d.get("contractType", "PERPETUAL") == "PERPETUAL"),
            min_notional=min_notional,
            raw=d,
        )

    def round_price(self, price: float, direction: str = "nearest") -> float:
        unit = Decimal(str(self.price_unit))
        p = Decimal(str(price))
        if direction == "down":
            q = (p / unit).to_integral_value(rounding=ROUND_DOWN) * unit
        elif direction == "up":
            q = (p / unit).to_integral_value(rounding=ROUND_UP) * unit
        else:
            q = (p / unit).to_integral_value() * unit
        return float(round(q, max(self.price_scale, 0)))

    def round_vol(self, vol: float) -> float:
        unit = Decimal(str(self.vol_unit))
        q = (Decimal(str(vol)) / unit).to_integral_value(rounding=ROUND_DOWN) * unit
        return float(round(q, max(self.vol_scale, 0)))

    def notional(self, vol: float, price: float) -> float:
        return vol * self.contract_size * price


@dataclass
class Ticker:
    symbol: str
    last: float
    fair: float
    index: float
    bid: float
    ask: float
    volume24: float
    amount24: float
    rise_fall_rate: float
    ts: float

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "Ticker":
        last = _float(d.get("lastPrice", d.get("last", d.get("c", 0))))
        fair = _float(d.get("fairPrice", d.get("markPrice", d.get("p", last))), last)
        index = _float(d.get("indexPrice", d.get("i", fair)), fair)
        change = d.get("riseFallRate")
        if change is None:
            change = _float(d.get("priceChangePercent", d.get("P", 0))) / 100.0
        ts = _float(d.get("timestamp", d.get("E", d.get("closeTime", 0))))
        if ts > 10_000_000_000:
            ts /= 1000.0
        return cls(
            symbol=str(d.get("symbol", d.get("s", ""))),
            last=last,
            fair=fair,
            index=index,
            bid=_float(d.get("bid1", d.get("bidPrice", d.get("b", last))), last),
            ask=_float(d.get("ask1", d.get("askPrice", d.get("a", last))), last),
            volume24=_float(d.get("volume24", d.get("volume", d.get("v", 0)))),
            amount24=_float(d.get("amount24", d.get("quoteVolume", d.get("q", 0)))),
            rise_fall_rate=_float(change),
            ts=ts,
        )

    @property
    def spread_pct(self) -> float:
        if self.bid > 0 and self.ask > 0 and self.ask >= self.bid:
            return (self.ask - self.bid) / self.ask * 100.0
        return 0.0


@dataclass
class ExchangePosition:
    """Normalized view of an open Binance USDT-M Futures position."""

    position_id: Optional[int]
    symbol: str
    position_type: int        # 1 long, 2 short
    open_type: int            # 1 isolated, 2 cross
    hold_vol: float
    hold_avg_price: float
    open_avg_price: float
    liquidate_price: float
    im: float                 # estimated/current initial margin
    leverage: int
    realised: float           # retained for persisted-schema compatibility
    state: int
    unrealized: Optional[float]
    hold_fee: Optional[float] = None     # funding received (+) / paid (-), when known
    fees_paid: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "ExchangePosition":
        if "positionAmt" in d or "positionSide" in d:
            position_amt = _float(d.get("positionAmt"))
            position_side = str(d.get("positionSide", "BOTH")).upper()
            side = position_side if position_side in {"LONG", "SHORT"} else ("LONG" if position_amt >= 0 else "SHORT")
            hold_vol = abs(position_amt)
            entry = _float(d.get("entryPrice", d.get("breakEvenPrice", 0)))
            mark = _float(d.get("markPrice", entry), entry)
            lev = int(_float(d.get("leverage"), 1)) or 1
            margin = _float(d.get("positionInitialMargin", d.get("isolatedMargin", 0)))
            if margin <= 0 and entry > 0:
                margin = hold_vol * entry / lev
            margin_type = str(d.get("marginType", "isolated")).lower()
            return cls(
                position_id=None,
                symbol=str(d.get("symbol", "")),
                position_type=1 if side == "LONG" else 2,
                open_type=2 if margin_type in {"cross", "crossed"} else 1,
                hold_vol=hold_vol,
                hold_avg_price=entry,
                open_avg_price=entry,
                liquidate_price=_float(d.get("liquidationPrice")),
                im=margin,
                leverage=lev,
                realised=_float(d.get("realizedProfit", d.get("cr", 0))),
                state=1 if hold_vol > 0 else 0,
                unrealized=_float(d.get("unRealizedPnl", d.get("unrealizedProfit", d.get("up", 0)))),
                hold_fee=(_float(d.get("holdFee")) if d.get("holdFee") is not None else None),
                fees_paid=(abs(_float(d["feesPaid"])) if d.get("feesPaid") is not None else None),
                raw=d,
            )
        # Also accept already-normalized rows (useful for websocket events and tests).
        position_type = int(d.get("position_type", d.get("positionType", 1)) or 1)
        return cls(
            position_id=int(d["position_id"]) if d.get("position_id") is not None else (
                int(d["positionId"]) if d.get("positionId") is not None else None
            ),
            symbol=str(d.get("symbol", "")),
            position_type=position_type,
            open_type=int(d.get("open_type", d.get("openType", 1)) or 1),
            hold_vol=_float(d.get("hold_vol", d.get("holdVol", 0))),
            hold_avg_price=_float(d.get("hold_avg_price", d.get("holdAvgPrice", 0))),
            open_avg_price=_float(d.get("open_avg_price", d.get("openAvgPrice", 0))),
            liquidate_price=_float(d.get("liquidate_price", d.get("liquidatePrice", 0))),
            im=_float(d.get("im", 0)),
            leverage=int(_float(d.get("leverage"), 1)) or 1,
            realised=_float(d.get("realised", 0)),
            state=int(_float(d.get("state", 1), 1)),
            unrealized=(
                _float(d.get("unrealized", d.get("unRealizedPnl")))
                if d.get("unrealized", d.get("unRealizedPnl")) is not None else None
            ),
            hold_fee=(
                _float(d.get("hold_fee", d.get("holdFee")))
                if d.get("hold_fee", d.get("holdFee")) is not None else None
            ),
            fees_paid=(abs(_float(d["fees_paid"])) if d.get("fees_paid") is not None else None),
            raw=d,
        )

    @property
    def side(self) -> str:
        return "long" if self.position_type == 1 else "short"


@dataclass
class PositionLedger:
    """Exchange-derived settlement for one managed position.

    ``realised`` is net PnL (gross price PnL - trading fees + funding), matching
    the change in the Binance Futures wallet for that position's fills.
    """

    position_id: Optional[int]
    close_avg_price: float
    close_pnl: float
    fee: float
    total_fee: float
    funding: float
    realised: float
    profit_ratio: float
    close_vol: float
    state: int

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "PositionLedger":
        if "realised" in d or "realized" in d or "closeProfitLoss" in d:
            realised = _float(d.get("realised", d.get("realized", 0)))
            close_pnl = _float(d.get("closeProfitLoss", d.get("close_pnl", realised)))
            fee = _float(d.get("fee", 0))
            total_fee = _float(d.get("totalFee", d.get("total_fee", abs(fee))), abs(fee))
            funding = _float(d.get("holdFee", d.get("funding", 0)))
            return cls(
                position_id=(int(d["positionId"]) if d.get("positionId") is not None else None),
                close_avg_price=_float(d.get("closeAvgPrice", d.get("close_avg_price", 0))),
                close_pnl=close_pnl,
                fee=fee,
                total_fee=total_fee,
                funding=funding,
                realised=realised,
                profit_ratio=_float(d.get("profitRatio", d.get("profit_ratio", 0))),
                close_vol=_float(d.get("closeVol", d.get("close_vol", 0))),
                state=int(_float(d.get("state", 3), 3)),
            )
        gross = _float(d.get("gross_pnl"))
        total_fee = _float(d.get("total_fee"))
        fee = _float(d.get("fee", -total_fee))
        funding = _float(d.get("funding"))
        return cls(
            position_id=None,
            close_avg_price=_float(d.get("close_avg_price")),
            close_pnl=gross,
            fee=fee,
            total_fee=total_fee,
            funding=funding,
            realised=_float(d.get("net_pnl", gross - total_fee + funding)),
            profit_ratio=_float(d.get("profit_ratio")),
            close_vol=_float(d.get("close_vol")),
            state=3,
        )


@dataclass
class AccountAsset:
    currency: str
    equity: float
    available: float
    cash: float
    frozen: float
    position_margin: float
    unrealized: float
    can_trade: bool = True

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "AccountAsset":
        currency = str(d.get("currency", d.get("asset", "USDT")))
        equity = _float(d.get("equity", d.get("marginBalance", d.get("balance", 0))))
        cash = _float(d.get("cashBalance", d.get("walletBalance", d.get("balance", equity))))
        available = _float(d.get("availableBalance", d.get("available", 0)))
        position_margin = _float(d.get("positionMargin", d.get("positionInitialMargin", d.get("initialMargin", 0))))
        return cls(
            currency=currency,
            equity=equity,
            available=available,
            cash=cash,
            frozen=_float(d.get("frozenBalance", d.get("initialMargin", position_margin))),
            position_margin=position_margin,
            unrealized=_float(d.get("unrealized", d.get("unrealizedProfit", d.get("unRealizedPnl", 0)))),
            can_trade=bool(d.get("can_trade", d.get("canTrade", True))),
        )


class ExchangeAPIError(Exception):
    """An API/transport error returned by Binance Futures."""

    _TRANSIENT = {418, 429, 500, 502, 503, 504, -1000, -1001, -1006, -1007, -1008, -1021}

    def __init__(self, code: int, message: str, path: str = "", payload: Any = None):
        super().__init__(f"Binance {path} -> code={code} msg={message}")
        self.code = code
        self.message = message
        self.path = path
        self.payload = payload

    @property
    def retryable(self) -> bool:
        return self.code in self._TRANSIENT


def is_nan(x: float) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))

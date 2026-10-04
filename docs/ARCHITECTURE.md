# Crypto Hunter Binance architecture

```
┌────────────────────────────────── Binance USDⓈ-M Futures ──────────────────────────────────┐
│ REST https://fapi.binance.com  ·  market WS …/market/stream  ·  private WS …/private │
│ MARKET entry/close orders · conditional STOP_MARKET / TAKE_PROFIT_MARKET Algo orders      │
└─────────────────────────────────────────┬─────────────────────────────────────────────────┘
                                          │ REST + websocket
┌─────────────────────────────────────────▼─────────────────────────────────────────────────┐
│ Bot orchestrator (engine/bot.py)                                                           │
│ scanner → AO signal/filter pipeline → risk gate → executor → position manager            │
│       └── account/position reconciliation · daily loss pause · equity/events              │
└──────────────────────┬──────────────────────────┬─────────────────────────────────────────┘
                       │                          │
         ┌─────────────▼────────────┐  ┌──────────▼─────────────┐
         │ Strategy / scanner       │  │ FastAPI + WebSocket UI │
         │ indicators/divergence    │  │ same dashboard layout, │
         │ filters/ATR ranking      │  │ Binance gold palette   │
         └──────────────────────────┘  └──────────┬─────────────┘
                                                  │
                                   ┌──────────────▼──────────────┐
                                   │ SQLite WAL + Fernet secrets │
                                   └─────────────────────────────┘
```

## 1. Market scanning and signals

- Binance `/fapi/v1/exchangeInfo` is normalized into symbol tick/quantity filters. Only active
  `PERPETUAL` contracts whose quote/margin asset matches the configured quote coin are eligible.
- `/fapi/v1/ticker/24hr`, `/fapi/v1/ticker/bookTicker`, and `/fapi/v1/premiumIndex` provide the
  scanner with last price, 24h quote volume, spread, mark price, and index price.
- The scanner volume-prefilters contracts, backfills OHLCV from `/fapi/v1/klines`, computes ATR%,
  and ranks candidates using `volatility_weight * ATR_rank + (1-volatility_weight) * volume_rank`.
- Public `@kline_5m` streams update the in-memory candle series. The strategy still evaluates only
  after the candle closes and REST-refreshes the recent bars before considering a signal.
- AO divergence, confirmation, volume spike, EMA trend, ATR%, spread, and cooldown filters are
  exchange-independent strategy logic inherited from Crypto Hunter.

## 2. Sizing and risk gates

Sizing is based on Binance USDT-M quantities in base-asset units, rounded down to the symbol's
`MARKET_LOT_SIZE`/`LOT_SIZE` step. It checks the exchange's minimum quantity and notional filters.
Margin is capped by available balance after a fee buffer. Leverage and margin type are requested
before entry; a rejected setting change is not ignored. The scanner and entry gate enforce the
position count, per-symbol cap, opposite-position rule, stale-account check, minimum equity, and
daily-loss pause.

The initial stop distance is `min(ATR * atr_stop_multiplier, ref_price * 0.85 / leverage)` so the
configured stop remains inside the strategy's liquidation-band clamp. ROI is price return times
leverage. The fixed TP and trailing ladder use ROI-on-margin, not account-equity ROI.

## 3. Order execution and exchange protection

### Entry and close

- Entry uses `/fapi/v1/order` with `MARKET` + `newOrderRespType=RESULT`, or an aggressive `LIMIT`
  IOC if configured. A unique `newClientOrderId` lets the executor resolve an ambiguous transport
  timeout instead of blindly resending an entry.
- One-way closes pass `reduceOnly=true`. Hedge-mode orders specify `positionSide=LONG` or `SHORT`;
  the close side is bound to that position side so it cannot open the opposite hedge leg.
- Any unresolved entry order is canceled after the fill wait expires. Partial IOC fills are tracked
  at their actually executed quantity.

### Stop-loss, take-profit, and trailing ratchet

Binance moved USDⓈ-M conditional orders to the Algo Service. The bot uses
`POST /fapi/v1/algoOrder` for both `STOP_MARKET` and `TAKE_PROFIT_MARKET` with `closePosition=true`
and either `MARK_PRICE` or `CONTRACT_PRICE` as the trigger working type. It does not send a
quantity or `reduceOnly` with a close-position algo order.

Binance does not attach the bot's bracket to the market entry. After a fill, the bot registers the
position, then synchronously discovers/arms both algo orders. On a trailing step it submits the new
stop first and only then cancels the previous stop, retaining the old protection if creation fails.
The fixed TP is left unchanged. Positions restored or adopted after a restart are reconciled against
`/fapi/v3/positionRisk` and `/fapi/v1/openAlgoOrders`, and missing protection is re-armed.

If either exchange-side protective order cannot be confirmed, the bot pauses new entries, raises an
activity error, and leaves its software failsafe enabled according to configuration. The software
failsafe requires current price ticks; it is not a substitute for exchange-side protection or a
reliable network.

## 4. WebSocket and REST reconciliation

- Regular market data connects to Binance's routed `/market/stream` endpoint for live subscriptions to
  `@ticker`, `@markPrice@1s`, and `@kline_<interval>`. Best bid/ask comes from ticker updates and
  REST `/fapi/v1/ticker/bookTicker`; the high-frequency `@bookTicker` stream is routed to `/public`
  and is not mixed into the market-data connection. Subscriptions are restored after reconnect.
- Private data obtains a listenKey through `/fapi/v1/listenKey` and connects to
  `/private/ws/<listenKey>`. It consumes `ACCOUNT_UPDATE`, `ORDER_TRADE_UPDATE`, and `ALGO_UPDATE`,
  and renews the listenKey every 30 minutes.
- `/fapi/v3/account` and `/fapi/v3/positionRisk` remain authoritative reconciliation sources. A
  configurable REST sync runs every few seconds even while the streams are connected.
- Public market REST requests use Binance request weights and a conservative global limiter. Safe
  reads may retry with backoff; order-creating requests are not blindly retried.

## 5. PnL and history

Binance exposes position PnL components as separate records. The bot aggregates income rows since a
managed position opened:

| Binance source | Stored as | Meaning |
|---|---|---|
| `incomeType=REALIZED_PNL` | `gross_pnl` | Realized price PnL before fees/funding |
| `incomeType=COMMISSION` (USDT) | `fee` | Trading fees, stored as a positive paid amount |
| `incomeType=FUNDING_FEE` (USDT) | `funding` | Funding received (+) or paid (−) |
| Gross PnL − fees + funding | `pnl` | Net PnL represented in the trade ledger |
| Opposite-side `userTrades` fills | `exit_price` | Quantity-weighted average closing price |

Open-position fees/funding are refreshed less frequently than position risk data to preserve API
budget. Binance income history is finite and a commission paid in a non-USDT asset cannot be added
directly to USDT PnL without a conversion. If an exchange ledger is unavailable or incomplete, the
trade is marked estimated and the dashboard shows the fallback calculation.

## 6. Persistence and API surface

SQLite runs in WAL mode. Persisted state includes managed positions (entry, stop/TP, trailing peak,
order/algo IDs), trades, equity snapshots, cooldowns, key/value lifecycle state, events, dashboard
settings, and encrypted credentials. The dashboard exposes a FastAPI control API and a server-pushed
`/ws` channel. An optional bearer token protects all control endpoints and the dashboard stream.

Credentials are encrypted with a local Fernet master key; the app only displays a masked API key.
Use a strong `CH_DASHBOARD_TOKEN` and TLS if the dashboard is accessible off-machine. The health
endpoint reports process liveness only.

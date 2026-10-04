# Crypto Hunter Binance

Automated **Binance USDⓈ-M USDT-perpetual futures** trading bot. It preserves Crypto Hunter's
5-minute Awesome Oscillator divergence strategy, signal filters, volatility/liquidity scanner,
position sizing, ROI take-profit and stepped trailing-stop rules, persistence, and live dashboard.
The dashboard keeps the original layout and glass styling with a Binance-inspired charcoal and
gold color palette.

> **This is live-trading software.** Pressing **Start** can place real Binance Futures orders.
> There is no paper-trading mode. With the default settings, the bot uses 10× leverage and
> allocates 8% of current equity as margin per entry; futures losses can be rapid and may include
> liquidation. The dashboard's $10,000-in-7-days curve is a mathematical goal, **not a forecast**
> or promise. Use a dedicated API key with withdrawals disabled, whitelist the server IP, and
> only trade money you can afford to lose.

## Strategy and behavior

| Area | What it does |
|---|---|
| Signal | Bullish / bearish Awesome Oscillator divergence on closed 5-minute candles; pivot-confirmed with the same original AO parameters |
| Asset selection | Scans Binance USDT-margined perpetuals, ranks by ATR% and 24h quote volume, and trades only the top-N watch-list |
| Signal filters | Divergence magnitude, confirmation close, volume spike, EMA 50/200 trend on 5m / 1h, minimum ATR%, per-symbol cooldown, and maximum spread |
| Sizing | Compounds from current equity: default 8% margin × 10× leverage; optional stop-risk sizing; quantity rounded to Binance market-lot filters |
| Risk | Default maximum 10 open positions, one position per symbol, 3×ATR initial stop, daily-loss pause, minimum-equity gate, and liquidation-band clamp |
| Exits | Fixed TP at +200% ROI on margin; stepped trailing stop: peak +30% → stop +20%, then each additional +10% peak ROI raises the stop by +10% |
| Binance execution | Signed Futures REST, server-time offset, weighted rate limiting, bounded retries for safe requests, order reconciliation by client ID, hedge/one-way support |
| Exchange protection | Binance conditional TP/SL Algo Service orders (`STOP_MARKET` / `TAKE_PROFIT_MARKET`) use `closePosition=true`; a trailing ratchet places its new stop before retiring the old one |
| Market / account data | Routed `/market/stream` ticker, mark-price and kline streams; best bid/ask from ticker updates and REST book-ticker; `/private/ws/<listenKey>` user stream with keepalive and reconnect; REST reconciliation remains enabled |
| Persistence | SQLite WAL stores managed positions, trailing peak/stop state, trades, equity snapshots, cooldowns, events, and encrypted credentials |
| Dashboard | Live equity curve, positions, realized history, PnL/fees/funding, metrics, signals, watch-list, API budget, settings, and server public IP for Binance key allowlisting |
| Credential security | API key/secret encrypted at rest with Fernet; secrets are redacted from logs; optional dashboard bearer token |

### Binance-specific execution note

Binance USDⓈ-M Futures does not attach this bot's bracket to its market entry. After a fill, the
bot immediately submits separate exchange-side stop-loss and take-profit Algo orders. If it cannot
confirm both protection orders, it pauses **new entries** and keeps the software failsafe active
while it monitors the open position. Do not stop or disconnect the service until you have verified
that each open position has exchange-side protection in Binance.

The bot detects the account's current hedge/one-way mode but does not change it. It uses the
appropriate `positionSide` for hedge mode and `reduceOnly` for one-way market closes. Margin type
and leverage are set/verified before each new entry. The bot does not cancel unrelated manual
orders.

## Quick start

```bash
git clone https://github.com/badalworld/Crypto_Hunter_Binance.git
cd Crypto_Hunter_Binance
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Create a Fernet key and a long dashboard token:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_urlsafe(32))"
# Add the values to .env as CH_MASTER_KEY and CH_DASHBOARD_TOKEN.
set -a; . ./.env; set +a

python -m crypto_hunter
# Open http://localhost:8080/?token=<CH_DASHBOARD_TOKEN>
```

1. In **Settings**, copy the server's public IP and add it to the Binance API key's IP access list.
2. Create a dedicated Binance API key with the read and USDⓈ-M Futures permissions required for the bot. **Keep withdrawals disabled.** Paste the key and secret in **Binance API credentials**, then choose **Save & verify**.
3. Review the strategy/risk values in Settings. The defaults are loaded from [`config.yaml`](config.yaml); dashboard overrides are persisted in SQLite.
4. Press **Start** only after reviewing the risk and checking that the account has USDT in its USDⓈ-M Futures wallet.

`auto_start` is **false** by default. The program starts with the bot stopped; saving an API key alone does not place an order.

### Account and API prerequisites

- Binance Futures enabled for the account and USDT available in the USDⓈ-M Futures wallet.
- A dedicated API key with IP restrictions and read + Futures trading permissions. Do not grant withdrawal/transfer permissions.
- The API key's allowlist must contain the server's public egress IP shown in Settings. If your hosting provider rotates egress IPs, update the allowlist before trading.
- The bot supports Binance hedge and one-way position modes, but does not change the account-wide mode. Review any existing positions/orders before starting.
- Keep system time synchronized. Signed requests use Binance's `timestamp`/`recvWindow` and the server time endpoint.

## Configuration

The original strategy and risk defaults are retained. Values can be changed in `config.yaml` or live
from the dashboard Settings panel; dashboard overrides persist in SQLite and survive restarts.

```yaml
signal:  5m AO divergence (SMA5(HL2) - SMA34(HL2)); 3-left / 2-right pivot confirmation
filters: magnitude 0.15 · close confirmation · volume ×1.5 · EMA 50/200 · min ATR% 0.35
scanner: top 12 · 60 candidates by volume · min 24h quote volume 20,000,000 USDT
risk:    10× leverage · 8% equity margin · max 10 positions · stop = 3×ATR · daily pause at -25%
exits:   +200% ROI TP · trailing ladder starts +30% ROI, initially locks +20%, then ratchets by +10%
```

Other controls include sizing mode (`margin` or `stop_risk`), isolated/cross margin, volume/trend/spread
filters, per-symbol cooldowns, and mark/contract price trigger selection. ROI is **price change ×
leverage**; fees, funding, slippage, and liquidation can make realized returns differ from the
price-only ROI ladder.

Process-level settings (host/port, data directory, log file, master encryption key, and dashboard
token) are in [`.env.example`](.env.example).

### Optional Binance Futures Testnet

The defaults point to the **live** Binance Futures API. For testnet-only verification, use a
separate testnet API key and set the REST and routed WebSocket URLs before saving credentials:

```yaml
execution:
  rest_base_url: https://demo-fapi.binance.com
  ws_market_url: wss://demo-fstream.binance.com/market/stream
  ws_private_url: wss://demo-fstream.binance.com/private
```

Binance documents the current demo/testnet REST and WebSocket roots separately from production in
its [USDⓈ-M General Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)
and the routed [WebSocket stream documentation](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams).
Create the API key in the corresponding demo environment, verify its permissions, and re-check the
official docs before use. Never point a real-money key at demo/testnet URLs.

## Production deployment

**Docker**

```bash
docker compose up -d --build
```

The compose file binds the dashboard to `127.0.0.1:8080`; place a TLS reverse proxy in front if you
need remote access. Set `CH_DASHBOARD_TOKEN` whenever the dashboard can be reached by anyone other
than the machine's local user.

**systemd** — see [`deploy/crypto-hunter.service`](deploy/crypto-hunter.service) for a hardened
unit example.

Operational notes:

- Back up `data/` (SQLite database and `.master_key` if `CH_MASTER_KEY` is not set). Losing the
  master key means re-entering the API credentials.
- Logs rotate at 20 MB × 5 and redact configured secrets.
- `/healthz` reports process liveness; it does not guarantee that market data, API credentials, or
  exchange-side orders are healthy.
- Stopping the bot leaves Binance-side algo TP/SL orders in place, but its software failsafe and
  trailing-stop ratchets no longer run. Verify exchange protection before stopping.
- Binance income/trade history is used to aggregate realized PnL, commissions and funding. If
  exchange history has not caught up, fees are paid in a non-USDT asset, or history is outside the
  available query window, the dashboard may show an estimate or incomplete USDT-denominated totals.

## Tests

```bash
pytest -q
```

Tests use an in-process Binance API emulator and verify HMAC signatures, Binance request formats,
market filters, order lifecycle, Algo TP/SL placement/cancellation, trailing ratchets, restart
recovery, ledger aggregation, and entry gates. **Tests do not submit live orders.**

## Project layout

```
crypto_hunter/
  config.py              strategy/risk/execution models and YAML/DB layering
  security.py            Fernet credential store and log redaction
  exchange/   rest.py    signed Binance USDⓈ-M Futures REST client
              ws_public.py / ws_private.py    public streams and listenKey user stream
              models.py / rate_limiter.py     Binance normalization and request budgets
  strategy/   indicators.py divergence.py filters.py scanner.py
  risk/       roi.py (ROI/trailing ladder) sizing.py (compounding and sizing)
  engine/     bot.py executor.py position_manager.py market_data.py
  persistence/db.py      SQLite schema and queries
  api/        server.py + static/ dashboard
config.yaml · .env.example · Dockerfile · docker-compose.yml · deploy/ · docs/ARCHITECTURE.md
```

## License and risk

MPL-2.0 — see [LICENSE](LICENSE). Futures trading involves substantial risk. This software is
provided as-is; the authors accept no liability for financial losses.

"""End-to-end engine tests against a signature-checking Binance USDⓈ-M emulator."""
import asyncio
from pathlib import Path

import pytest

from crypto_hunter.config import AppSettings, BotConfig
from crypto_hunter.engine.bot import Bot
from crypto_hunter.persistence.db import Database
from crypto_hunter.security import Credentials

from fake_binance import API_KEY, API_SECRET, TEST_SYMBOL, start_fake


@pytest.fixture
async def env(tmp_path: Path):
    fake, runner, url = await start_fake()
    settings = AppSettings()
    settings.data_dir = tmp_path
    settings.db_path = tmp_path / "t.sqlite"
    settings.master_key = None
    cfg = BotConfig().merged({
        "execution": {"rest_base_url": url, "ws_market_url": "ws://127.0.0.1:1/market/stream",
                      "ws_private_url": "ws://127.0.0.1:1/private",
                      "position_sync_interval_sec": 1, "max_retries": 0},
        "scanner": {"min_quote_volume_24h": 0, "rescan_interval_sec": 30},
        "exits": {"software_failsafe": False},
    })
    db = await Database(settings.db_path).open()
    broadcasts = []

    async def bc(msg):
        broadcasts.append(msg)

    bot = Bot(settings, cfg, db, bc)

    async def no_public_ip():
        return {"ip": "192.0.2.10", "source": "test-fixture"}

    bot.rest.public_ip = no_public_ip
    bot.broadcasts = broadcasts
    await bot.boot()
    await bot.set_credentials(API_KEY, API_SECRET)
    try:
        yield bot, fake, db
    finally:
        await bot.shutdown()
        await db.close()
        await runner.cleanup()


async def _wait_for_watchlist(bot: Bot):
    for _ in range(50):
        if bot.watchlist:
            return
        await asyncio.sleep(0.1)
    assert bot.watchlist, "scanner did not populate the watch-list"


@pytest.mark.asyncio
async def test_full_trade_lifecycle(env):
    bot, fake, db = env
    await bot.start()
    assert bot.state == "RUNNING" and bot.asset.equity == pytest.approx(1000.0)
    await _wait_for_watchlist(bot)
    assert [w.symbol for w in bot.watchlist] == [TEST_SYMBOL]

    # --- entry
    await bot.try_enter(TEST_SYMBOL, "long", atr_v=0.5, signal={"test": True})
    assert bot.pm.count() == 1
    p = bot.pm.positions[(TEST_SYMBOL, "long")]
    # USDⓈ-M quantities are base-asset units: 80 USDT margin x 10 leverage / 100 = 8 TEST.
    assert p.vol == pytest.approx(8.0, abs=0.002)  # ask-side sizing rounds down to the 0.001 step
    assert p.margin == pytest.approx(80.0, abs=0.02)
    assert p.stop_price == pytest.approx(p.entry_price - 1.5, abs=0.02)
    assert p.tp_price == pytest.approx(p.entry_price * 1.2, rel=1e-4)
    assert fake.leverage_calls and fake.leverage_calls[0]["leverage"] == 10
    assert len([c for c in fake.calls if c == "POST /fapi/v1/order"]) == 1
    assert len([x for x in fake.algo_orders.values() if x["algoStatus"] == "NEW"]) == 2
    rows = await db.list_positions()
    assert rows and rows[0]["stop_plan_order_id"] == p.stop_plan_order_id

    # Live commissions and unrealized PnL are sourced from Binance account/trade endpoints.
    positions = await bot.rest.get_open_positions()
    await bot._refresh_position_financials(positions, force=True)
    await bot.pm.reconcile(positions, bot._adopt_external)
    assert p.fee_paid == pytest.approx(p.vol * p.entry_price * fake.FEE, rel=1e-6)
    d = p.to_dict()
    assert d["pnl_source"] == "exchange" and d["net_pnl"] < d["unrealized_pnl"]

    # --- price rises: peak 35% ROI -> stop locks +20%
    entry = p.entry_price
    px = entry * (1 + 0.35 / 10)
    fake.set_price(px)
    await bot.pm.on_price(TEST_SYMBOL, px, px)
    await asyncio.sleep(0.3)
    assert p.peak_roi == pytest.approx(35, abs=0.01)
    assert p.stop_roi == 20
    assert p.stop_price == pytest.approx(entry * 1.02, rel=1e-4)
    assert any(x["type"] == "STOP_MARKET" and x["triggerPrice"] == pytest.approx(entry * 1.02, abs=0.011)
               for x in fake.plan_changes)

    # --- same ladder step again: no extra stop/TP requests
    n = len(fake.plan_changes)
    px2 = entry * (1 + 0.38 / 10)
    fake.set_price(px2)
    await bot.pm.on_price(TEST_SYMBOL, px2, px2)
    await asyncio.sleep(0.2)
    assert len(fake.plan_changes) == n and p.stop_roi == 20

    # --- peak 100% -> stop 90%
    px3 = entry * (1 + 1.0 / 10)
    fake.set_price(px3)
    await bot.pm.on_price(TEST_SYMBOL, px3, px3)
    await asyncio.sleep(0.3)
    assert p.stop_roi == 90 and p.stop_price == pytest.approx(entry * 1.09, rel=1e-4)

    # --- pull-back: stop must NOT move down
    px4 = entry * (1 + 0.95 / 10)
    fake.set_price(px4)
    await bot.pm.on_price(TEST_SYMBOL, px4, px4)
    await asyncio.sleep(0.2)
    assert p.stop_roi == 90

    # --- restart persistence: a fresh manager restores peak/stop state
    from crypto_hunter.engine.position_manager import PositionManager
    pm2 = PositionManager(db, bot.executor, bot.cfg, bot.emit)
    await pm2.load()
    restored = pm2.positions[(TEST_SYMBOL, "long")]
    assert restored.peak_roi == pytest.approx(100, abs=0.01)
    assert restored.stop_roi == 90 and restored.stop_plan_order_id == p.stop_plan_order_id

    # --- exchange stop fires; the normal position sync detects and records the close
    px5 = entry * (1 + 0.85 / 10)
    fake.set_price(px5)  # the exchange-side algo triggers at the locked 1.09 x entry price
    for _ in range(40):
        if bot.pm.count() == 0:
            break
        await asyncio.sleep(0.1)
    assert bot.pm.count() == 0
    trades = await db.list_trades()
    assert len(trades) == 1
    trade = trades[0]
    assert trade["reason"] == "TRAIL" and trade["pnl"] > 0
    assert trade["exit_price"] == pytest.approx(entry * 1.09, rel=1e-4)
    # Binance reports realized PnL, commission and funding as separate income records.
    assert trade["pnl_source"] == "exchange"
    assert trade["gross_pnl"] == pytest.approx((trade["exit_price"] - entry) * trade["vol"], rel=1e-6)
    assert trade["fee"] == pytest.approx(trade["vol"] * entry * fake.FEE + trade["vol"] * trade["exit_price"] * fake.FEE, rel=1e-6)
    assert trade["funding"] == pytest.approx(fake.FUNDING)
    assert trade["pnl"] == pytest.approx(trade["gross_pnl"] - trade["fee"] + trade["funding"], abs=1e-9)
    assert trade["exchange_roi"] == pytest.approx(trade["pnl"] / trade["margin"] * 100, rel=1e-6)
    assert trade["roi"] < 90  # net of fees/funding, below the price-only 90% lock
    cooldowns = await db.cooldowns()
    assert TEST_SYMBOL in cooldowns
    metrics = await bot.metrics()
    assert metrics["trades"] == 1 and metrics["win_rate"] == 100 and metrics["exit_reasons"] == {"TRAIL": 1}
    assert any(b.get("type") == "trade" for b in bot.broadcasts)


@pytest.mark.asyncio
async def test_entry_gates_and_manual_close(env):
    bot, fake, db = env
    await bot.start()
    await _wait_for_watchlist(bot)
    await bot.update_config({"risk": {"max_open_positions": 1}})
    await bot.try_enter(TEST_SYMBOL, "short", atr_v=0.5, signal={})
    assert bot.pm.count() == 1
    # A duplicate entry is blocked by the existing symbol/side position.
    await bot.try_enter(TEST_SYMBOL, "short", atr_v=0.5, signal={})
    assert bot.pm.count() == 1
    assert len([c for c in fake.calls if c == "POST /fapi/v1/order"]) == 1
    # Manual close becomes a reduce-only BUY market order in one-way mode.
    assert await bot.close_one(TEST_SYMBOL, "short")
    assert bot.pm.count() == 0
    trade = (await db.list_trades())[0]
    assert trade["reason"] == "MANUAL" and trade["side"] == "short"
    assert len([c for c in fake.calls if c == "POST /fapi/v1/order"]) == 2
    close_request = fake.order_requests[-1]
    assert close_request["side"] == "BUY" and close_request["reduceOnly"] == "true"


@pytest.mark.asyncio
async def test_hedge_mode_order_parameters(env):
    bot, fake, _db = env
    fake.hedge_mode = True
    await bot.start()
    await _wait_for_watchlist(bot)
    await bot.try_enter(TEST_SYMBOL, "long", atr_v=0.5, signal={})
    assert bot.pm.count() == 1
    open_request = fake.order_requests[-1]
    assert open_request["positionSide"] == "LONG" and "reduceOnly" not in open_request
    assert await bot.close_one(TEST_SYMBOL, "long")
    close_request = fake.order_requests[-1]
    assert close_request["side"] == "SELL" and close_request["positionSide"] == "LONG"
    assert "reduceOnly" not in close_request


@pytest.mark.asyncio
async def test_adopts_external_position_and_leaves_manual_algo_untouched(env):
    bot, fake, _db = env
    fake.open_manual_position(TEST_SYMBOL, "long", quantity=50, leverage=5)
    manual = fake.add_manual_algo(trigger_price=90.0, client_algo_id="manual-protection")
    await bot.start()
    assert bot.pm.count() == 1
    p = bot.pm.positions[(TEST_SYMBOL, "long")]
    assert p.vol == 50 and p.leverage == 5
    # Adoption adds this bot's own exchange protection but does not modify/cancel the user's order.
    assert p.sl_plan_order_id is not None and p.tp_plan_order_id is not None
    assert fake.algo_orders[str(manual["algoId"])]["algoStatus"] == "NEW"
    assert fake.algo_orders[str(manual["algoId"])]["triggerPrice"] == "90"
    bot_orders = [x for x in fake.algo_orders.values() if x.get("clientAlgoId", "").startswith("CH")]
    assert len(bot_orders) == 2
    sl = next(x for x in bot_orders if x["orderType"] == "STOP_MARKET")
    assert sl["side"] == "SELL" and float(sl["triggerPrice"]) == pytest.approx(p.stop_price)


@pytest.mark.asyncio
async def test_bad_signature_is_rejected(env):
    bot, _fake, _db = env
    bot.rest.set_credentials(Credentials(API_KEY, "wrong-secret-0000000"))
    from crypto_hunter.exchange.models import ExchangeAPIError
    with pytest.raises(ExchangeAPIError) as exc_info:
        await bot.rest.get_assets()
    assert "signature" in exc_info.value.message.lower()

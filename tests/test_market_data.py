import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import httpx
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from qmt_sim_sdk import SimulationClient
from qmt_simulator.simulator.account_manager import AccountManager
from qmt_simulator.simulator.algo_engine import SimAlgoEngine
from qmt_simulator.simulator.market_data import QuoteStore
from qmt_simulator.simulator.models import Base
from qmt_simulator.simulator.order_engine import OrderEngine
from qmt_simulator.simulator.scenario import CommissionConfig, ScenarioConfig
from qmt_simulator.web import api


@pytest.fixture
def lab(tmp_path, monkeypatch):
    database = create_engine(f"sqlite:///{(tmp_path / 'test.db').as_posix()}",
                             connect_args={"check_same_thread": False})
    Base.metadata.create_all(database)
    manager = AccountManager(sessionmaker(bind=database))
    for account in ("A", "B"):
        manager.create_account(account, account, 10000)
        manager.set_account_config(account, {"execution_mode": "quote", "commission": {"enabled": False}})
    store = QuoteStore()
    engine = OrderEngine(manager, manager._session_factory,
                         ScenarioConfig(filled_prob=1, partial_filled_prob=0, rejected_prob=0,
                                        canceled_prob=0, initial_delay=0, batch_count_min=1, batch_count_max=1),
                         commission=CommissionConfig(enabled=False), quote_store=store)
    monkeypatch.setattr(api, "_account_manager", manager)
    monkeypatch.setattr(api, "_order_engine", engine)
    callbacks = []
    engine.on_order_callback = lambda event: callbacks.append(dict(event))
    with TestClient(api.app) as http:
        client = SimulationClient("http://testserver", session=http)
        yield client, engine, store, callbacks, http
    for oid in list(engine._quote_pending):
        engine.cancel_order_sync({"external_order_id": oid})
    database.dispose()


def submit(client, oid="order", account="A", **kwargs):
    return client.submit_order("000001.SZ", "buy", 100, account,
                               order_id=oid, price=10.3, **kwargs)


def tick(**kwargs):
    return {"symbol": "000001.SZ", "last_price": 10.2, "ts": int(time.time() * 1000), **kwargs}


def test_sdk_quote_matches_limits_partial_fill_and_retry(lab):
    client, engine, store, callbacks, _ = lab
    submit(client)
    ts = int(time.time() * 1000)
    client.publish_quotes([tick(ts=ts, ask_price=10.4, ask_qty=100)])
    assert client.get_order("order").filled_qty == 0
    quote = tick(ts=ts + 1, ask_price=10.2, ask_qty=40)
    assert client.publish_quotes([quote])["accepted"] == 1
    assert client.get_order("order").status == "partial_filled"
    assert client.publish_quotes([quote])["ignored"] == 1
    assert client.get_order("order").filled_qty == 40
    client.publish_quotes([tick(ts=ts + 2, ask_price=10.1, ask_qty=60)])
    order = client.get_order("order")
    assert order.status == "filled"
    assert order.filled_amount == pytest.approx(1014)
    assert client.get_balance("A").cash == pytest.approx(8986)
    assert callbacks[-1]["avg_filled_price"] == pytest.approx(10.14)
    assert client.get_quote("000001.SZ")["ask_price"] == 10.1


def test_snapshot_depth_shared_within_account_independent_across_accounts(lab):
    client, *_ = lab
    client.publish_quote("000001.SZ", 10, ask_qty=150)
    submit(client, "first")
    submit(client, "second")
    submit(client, "other", "B")
    assert client.get_order("first").filled_qty == 100
    assert client.get_order("second").filled_qty == 50
    assert client.get_order("other").filled_qty == 100


def test_cancel_partial_preserves_fills_and_stops_further_matching(lab):
    client, _, _, callbacks, _ = lab
    submit(client)
    client.publish_quote("000001.SZ", 10, ask_qty=25)
    client.cancel_order("order")
    client.publish_quote("000001.SZ", 10, ts=int(time.time() * 1000) + 1)
    assert client.get_order("order").status == "canceled"
    assert client.get_order("order").filled_qty == 25
    assert callbacks[-1]["filled_quantity"] == 25


def test_market_price_valuation_and_sell_limits(lab):
    client, *_ = lab
    client.publish_quote("000001.SZ", 12, ask_price=12.1, bid_price=11.9)
    result = client.submit_order("000001.SZ", "buy", 100, "A", price_type="market", wait=True)
    assert result.avg_price == 12.1
    assert client.get_balance("A").market_value == 1200
    assert client.get_positions("A")[0].cost_price == 12.1
    assert client.get_positions("A")[0].profit_loss == pytest.approx(-10)
    client.submit_order("000001.SZ", "sell", 100, "A", price=12, order_id="sell")
    assert client.get_order("sell").filled_qty == 0
    client.publish_quote("000001.SZ", 12.2, bid_price=12.2, ts=int(time.time() * 1000) + 1)
    assert client.get_order("sell").filled_amount == 1220
    assert client.get_positions("A") == []


@pytest.mark.parametrize("fallback,expected", [("pending", "submitted"), ("reject", "rejected"), ("last", "filled")])
def test_stale_quote_policy(lab, fallback, expected):
    client, *_ = lab
    client.set_execution_mode("A", "quote", quote_fallback=fallback, quote_max_age_ms=100)
    client.publish_quote("000001.SZ", 10, ts=int(time.time() * 1000) - 10000)
    submit(client)
    assert client.get_order("order").status == expected


def test_missing_quote_and_halted_market(lab):
    client, *_ = lab
    client.set_execution_mode("A", "quote", quote_fallback="last")
    submit(client)
    client.publish_quote("000001.SZ", 10, trading_status="halted")
    assert client.get_order("order").filled_qty == 0
    client.publish_quote("600000.SH", 10)
    assert client.get_order("order").filled_qty == 0
    client.publish_quote("000001.SZ", 10, ts=int(time.time() * 1000) + 1)
    assert client.get_order("order").status == "filled"


def test_expired_cached_quote_not_used_on_submission(lab):
    client, _, store, _, _ = lab
    client.set_execution_mode("A", "quote", quote_max_age_ms=1)
    store.publish(tick(ts=int(time.time() * 1000) - 10))
    submit(client)
    assert client.get_order("order").status == "submitted"


def test_account_mode_alias_persistence_and_new_orders_only(lab):
    client, engine, store, _, _ = lab
    client.update_account_config("A", {"fill_mode": "manual", "initial_delay": 0.25})
    assert client.get_account_config("A")["execution_mode"] == "manual"
    client.set_execution_mode("A", "quote")
    assert client.get_account_config("A")["initial_delay"] == 0.25
    submit(client)
    client.set_execution_mode("A", "manual")
    store.publish(tick())
    assert client.get_order("order").status == "filled"
    reopened = AccountManager(engine.manager._session_factory)
    assert reopened.get_execution_mode("A") == "manual"
    assert client.set_execution_mode("A", "auto")["execution_mode"] == "random"


def test_manual_and_random_accounts_do_not_consume_quotes(lab):
    client, engine, store, _, _ = lab
    client.set_execution_mode("A", "manual")
    submit(client)
    deadline = time.monotonic() + 2
    while not client.pending_fills() and time.monotonic() < deadline:
        time.sleep(0.01)
    store.publish(tick(last_price=20))
    assert client.get_order("order").filled_qty == 0
    event = engine.subscribe_completion("order")
    client.decide_fill("order", "fill", fill_price=8, fill_qty=100)
    assert event.wait(2)
    assert client.get_order("order").filled_amount == 800
    client.set_execution_mode("B", "random")
    result = submit(client, "random", "B", wait=True)
    assert result.status == "filled"
    assert 10.3 * 0.99 - 0.01 <= result.avg_price <= 10.3 * 1.01 + 0.01


def test_insufficient_cash_including_fees_and_overselling(lab):
    client, engine, *_ = lab
    client.update_account("A", cash=1000)
    client.update_account_config("A", {"execution_mode": "quote", "commission": {"enabled": True}})
    client.publish_quote("000001.SZ", 10)
    buy = client.submit_order("000001.SZ", "buy", 100, "A", price_type="market", wait=True)
    assert buy.status == "rejected"
    assert client.get_balance("A").cash == 1000
    sell = client.submit_order("000001.SZ", "sell", 100, "A", price_type="market", wait=True)
    assert sell.status == "rejected"
    assert client.list_trades(account_id="A") == []


def test_concurrent_feed_updates_do_not_overfill(lab):
    client, _, store, *_ = lab
    submit(client)
    ts = int(time.time() * 1000)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(store.publish, [tick(ts=ts + i, ask_qty=30) for i in range(20)]))
    order = client.get_order("order")
    assert order.filled_qty == 100
    assert sum(t.quantity for t in client.list_trades(account_id="A")) == 100


def test_wait_timeout_cancels_unfilled_quote_order(lab):
    client, *_ = lab
    result = submit(client, wait=True, timeout=0.02)
    assert result.status == "canceled"
    assert result.timed_out


def test_zero_depth_and_explicit_unlimited_mode(lab):
    client, *_ = lab
    client.publish_quote("000001.SZ", 10, ask_qty=0)
    submit(client)
    assert client.get_order("order").filled_qty == 0
    client.set_execution_mode("B", "quote", use_depth=False)
    submit(client, "unlimited", "B")
    assert client.get_order("unlimited").status == "filled"


def test_multiple_pending_buys_cannot_spend_same_cash(lab):
    client, *_ = lab
    client.update_account("A", cash=1500)
    submit(client, "first")
    submit(client, "second")
    client.publish_quote("000001.SZ", 10)
    assert client.get_order("first").status == "filled"
    assert client.get_order("second").status == "rejected"
    assert client.get_balance("A").cash == 500


def test_gateway_reads_the_same_injected_quote(lab):
    from qmt_simulator.gateway.sim_executor import SimExecutor
    from qmt_simulator.gateway.algo_executor import SimAlgoExecutor
    client, _, _, _, http = lab
    sim = SimExecutor("http://testserver")
    sim._session.close()
    sim._session = http
    algo = SimAlgoExecutor(sim)
    client.publish_quote("000001.SZ", 13.2)
    assert algo._get_market_price("000001.SZ", "A") == 13.2
    client.set_execution_mode("A", "quote", quote_max_age_ms=0)
    time.sleep(0.002)
    with pytest.raises(httpx.HTTPStatusError) as error:
        algo._get_market_price("000001.SZ", "A")
    assert error.value.response.status_code == 409


def test_batch_validation_is_atomic_and_out_of_order_is_ignored(lab):
    client, _, store, _, http = lab
    response = http.post("/api/market-data/quotes", json=[tick(), tick(last_price=-1)])
    assert response.status_code == 422
    assert store.all() == []
    ts = int(time.time() * 1000)
    assert client.publish_quotes([tick(ts=ts, seq=2)])["accepted"] == 1
    assert client.publish_quotes([tick(ts=ts - 1, seq=3)])["ignored"] == 1
    assert client.publish_quotes([tick(ts=ts + 1, seq=1)])["ignored"] == 1
    assert client.publish_quotes([tick(ts=ts, seq=3)])["accepted"] == 1


@pytest.mark.parametrize("change", [{"last_price": "NaN"}, {"last_price": "Infinity"},
                                   {"bid_price": 11, "ask_price": 10}, {"ask_qty": -1},
                                   {"ask_qty": 1.5}, {"symbol": ""}, {"ts": 0},
                                   {"trading_status": "unknown"}])
def test_invalid_quote_rejected(lab, change):
    *_, http = lab
    assert http.post("/api/market-data/quote", json=tick(**change)).status_code == 422


@pytest.mark.parametrize("config", [{"execution_mode": "unknown"}, {"quote_max_age_ms": -1},
                                    {"quote_max_age_ms": None}, {"quote_max_age_ms": 2.5},
                                    {"use_depth": "true"}, {"quote_fallback": "bad"}])
def test_invalid_account_config_rejected(lab, config):
    *_, http = lab
    assert http.put("/api/accounts/A/config", json=config).status_code == 422


def test_algorithm_uses_injected_quote_and_does_not_fall_back(lab):
    client, engine, *_ = lab
    algo = SimAlgoEngine(engine, engine.manager)
    try:
        with pytest.raises(ValueError):
            algo._get_market_price("000001.SZ", "A")
        client.publish_quote("000001.SZ", 15.5)
        assert algo._get_market_price("000001.SZ", "A") == 15.5
    finally:
        algo._pool.shutdown(wait=True)


@pytest.fixture
def quote_clock(monkeypatch):
    from types import SimpleNamespace
    from qmt_simulator.simulator import market_data

    clock = SimpleNamespace(wall=time.time(), mono=100.0)
    monkeypatch.setattr(market_data, "time", SimpleNamespace(
        time=lambda: clock.wall, monotonic=lambda: clock.mono))
    return clock


def test_monitor_distinguishes_receipt_from_quote_freshness(lab, quote_clock):
    client, _, _, _, http = lab
    assert client.market_data_status()["feed_status"] == "never"
    payload = tick(ts=int(quote_clock.wall * 1000) - 10000, source="test-feed")
    client.publish_quotes([payload])
    state = client.market_data_status()
    assert state["feed_status"] == "recent"
    assert (state["fresh_count"], state["stale_count"]) == (0, 1)
    assert state["received_count"] == state["accepted_count"] == 1
    quote = client.get_quote("000001.sz")
    assert quote["stale"] and quote["age_ms"] == 10000
    assert quote["received_at"] == state["last_accepted_at"]
    assert quote["received_age_ms"] == 0
    assert client.list_quotes(max_age_ms=20000)[0]["stale"] is False
    assert client.market_data_status(max_age_ms=20000)["fresh_count"] == 1
    # A monitoring threshold must not alter any account's matching settings.
    assert client.get_account_config("A")["quote_max_age_ms"] == 3000
    for path in ("status", "quotes", "quote/000001.SZ"):
        assert http.get(f"/api/market-data/{path}?max_age_ms=-1").status_code == 422
    assert http.get("/api/market-data/quote/MISSING.SZ").status_code == 404


def test_duplicate_receipts_count_but_do_not_extend_retention(quote_clock):
    store = QuoteStore(retention_ms=1000)
    payload = tick(ts=int(quote_clock.wall * 1000))
    store.publish(payload)
    accepted_at = store.status()["last_accepted_at"]
    quote_clock.wall += 0.75
    quote_clock.mono += 0.75
    assert store.publish(payload)[1] is False
    state = store.status()
    assert (state["received_count"], state["accepted_count"], state["ignored_count"]) == (2, 1, 1)
    assert state["last_received_at"] > accepted_at
    assert state["last_accepted_at"] == accepted_at
    quote_clock.wall += 0.5
    quote_clock.mono += 0.5
    assert store.get("000001.SZ") is None
    assert store.snapshots() == []
    assert store.status()["expired_count"] == 1
    assert store.status()["received_count"] == 2


def test_capacity_keeps_recent_updates_and_replaces_snapshots(quote_clock):
    store = QuoteStore(max_symbols=2)
    ts = int(quote_clock.wall * 1000)
    store.publish(tick(symbol="A.SZ", ts=ts))
    store.publish(tick(symbol="B.SZ", ts=ts))
    store.publish(tick(symbol="A.SZ", ts=ts + 1, last_price=11))
    assert store.status()["symbol_count"] == 2
    assert store.get("B.SZ") is not None  # Reads do not extend retention/order.
    store.publish(tick(symbol="C.SZ", ts=ts))
    assert {q.symbol for q in store.all()} == {"A.SZ", "C.SZ"}
    assert store.get("A.SZ").last_price == 11
    assert store.status()["evicted_count"] == 1
    assert store.status()["expired_count"] == 0


def test_cleanup_uses_monotonic_clock_and_feed_becomes_idle(quote_clock):
    store = QuoteStore(retention_ms=5000)
    store.publish(tick(ts=int(quote_clock.wall * 1000)))
    quote_clock.wall -= 3600  # Wall-clock correction must not retain data forever.
    quote_clock.mono += 4
    assert store.status()["feed_status"] == "idle"
    assert store.status()["symbol_count"] == 1
    quote_clock.mono += 2
    assert store.cleanup() == 1
    assert store.status()["storage"] == "memory"
    assert store.status()["persistent"] is False
    assert store.status()["accepted_count"] == 1


def test_background_cleanup_runs_without_reads_and_stops(quote_clock):
    import threading

    store = QuoteStore(retention_ms=1000)
    store.publish(tick(ts=int(quote_clock.wall * 1000)))
    cleaned = threading.Event()
    store.subscribe_evictions(lambda removed: cleaned.set())
    quote_clock.mono += 2
    store.start_cleanup(interval_seconds=0.01)
    try:
        assert cleaned.wait(2), "idle snapshots must expire even without HTTP requests"
    finally:
        store.stop_cleanup()
    assert not store._cleanup_thread.is_alive()
    assert store.status()["symbol_count"] == 0


def test_cleanup_releases_depth_without_deleting_ledger(lab, quote_clock):
    client, engine, store, _, _ = lab
    store.retention_ms = 1000
    client.publish_quote("000001.SZ", 10, ask_qty=100)
    submit(client)
    assert client.get_order("order").status == "filled"
    assert engine._quote_depth
    cash = client.get_balance("A").cash
    trades = [trade.to_dict() for trade in client.list_trades(account_id="A")]
    positions = [(p.symbol, p.quantity, p.cost_price) for p in client.get_positions("A")]
    quote_clock.mono += 2
    assert store.cleanup() == 1
    assert engine._quote_depth == {}
    assert client.get_balance("A").cash == cash
    assert [trade.to_dict() for trade in client.list_trades(account_id="A")] == trades
    assert [(p.symbol, p.quantity, p.cost_price) for p in client.get_positions("A")] == positions
    assert client.get_order("order").status == "filled"
    # Even an account allowing stale quotes must wait after cache eviction.
    client.set_execution_mode("A", "quote", quote_fallback="last")
    submit(client, "after-cleanup")
    assert client.get_order("after-cleanup").status == "submitted"


def test_replacement_and_capacity_eviction_release_old_depth(lab):
    client, engine, store, _, _ = lab
    store.max_symbols = 1
    ts = int(time.time() * 1000)
    client.publish_quote("000001.SZ", 10, ts=ts, ask_qty=100)
    submit(client)
    assert engine._quote_depth
    client.publish_quote("000001.SZ", 10, ts=ts + 1, ask_qty=100)
    assert engine._quote_depth == {}
    submit(client, "second")
    assert engine._quote_depth
    client.publish_quote("600000.SH", 10)
    assert engine._quote_depth == {}
    assert client.market_data_status()["evicted_count"] == 1

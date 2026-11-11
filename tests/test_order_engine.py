import time
import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from qmt_simulator.simulator.models import Base
from qmt_simulator.simulator.account_manager import AccountManager
from qmt_simulator.simulator.scenario import ScenarioConfig
from qmt_simulator.simulator.order_engine import OrderEngine


@pytest.fixture
def engine_setup():
    db_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(db_engine)
    Session = sessionmaker(bind=db_engine)
    manager = AccountManager(Session)
    manager.create_account("ACC1", "测试账号", initial_cash=1_000_000.0)
    scenario = ScenarioConfig(
        filled_prob=1.0, partial_filled_prob=0.0, rejected_prob=0.0, canceled_prob=0.0,
        initial_delay=0.1, batch_interval_min=0.05, batch_interval_max=0.1,
        batch_count_min=2, batch_count_max=2,
    )
    eng = OrderEngine(manager=manager, session_factory=Session, scenario=scenario)
    return eng


def test_submit_order_returns_success(engine_setup):
    eng = engine_setup
    order_data = {
        "order_id": "ORD001", "fund_account_id": "ACC1", "account_id": "ACC1",
        "symbol": "000001.SZ", "direction": "buy", "price_type": "limit",
        "price": 10.0, "quantity": 100,
    }
    result = eng.submit_order_sync(order_data)
    assert result["success"] is True
    assert result["external_order_id"] != ""


def test_submit_order_rejected_insufficient_cash(engine_setup):
    eng = engine_setup
    order_data = {
        "order_id": "ORD002", "fund_account_id": "ACC1", "account_id": "ACC1",
        "symbol": "000001.SZ", "direction": "buy", "price_type": "limit",
        "price": 100.0, "quantity": 100_000,  # 需要 1000 万，超出余额
    }
    callbacks = []
    eng.on_order_callback = lambda d: callbacks.append(d)
    result = eng.submit_order_sync(order_data)
    assert result["success"] is False


def test_order_callbacks_fired(engine_setup):
    eng = engine_setup
    callbacks = []
    eng.on_order_callback = lambda d: callbacks.append(dict(d))

    order_data = {
        "order_id": "ORD003", "fund_account_id": "ACC1", "account_id": "ACC1",
        "symbol": "000001.SZ", "direction": "buy", "price_type": "limit",
        "price": 10.0, "quantity": 100,
    }
    eng.submit_order_sync(order_data)
    time.sleep(1.0)  # 等待批次完成（initial_delay=0.1）

    statuses = [c["status"] for c in callbacks]
    assert "submitted" in statuses
    assert "filled" in statuses


def test_cancel_order(engine_setup):
    eng = engine_setup
    # 使用长延迟场景测试撤单
    eng.scenario.initial_delay = 10.0
    order_data = {
        "order_id": "ORD004", "fund_account_id": "ACC1", "account_id": "ACC1",
        "symbol": "000001.SZ", "direction": "buy", "price_type": "limit",
        "price": 10.0, "quantity": 100,
    }
    result = eng.submit_order_sync(order_data)
    ext_id = result["external_order_id"]

    cancel_result = eng.cancel_order_sync({
        "external_order_id": ext_id,
        "account_id": "ACC1",
        "fund_account_id": "ACC1",
    })
    assert cancel_result["success"] is True


def test_query_positions(engine_setup):
    eng = engine_setup
    positions = eng.query_positions_sync("ACC1")
    assert isinstance(positions, list)


def test_query_balance(engine_setup):
    eng = engine_setup
    balance = eng.query_balance_sync("ACC1")
    assert "cash" in balance
    assert "total_asset" in balance


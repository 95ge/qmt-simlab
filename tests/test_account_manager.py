import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from qmt_simulator.simulator.models import Base
from qmt_simulator.simulator.account_manager import AccountManager


@pytest.fixture
def manager():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return AccountManager(Session)


def test_create_and_get_account(manager):
    manager.create_account("ACC1", "账号1", initial_cash=500_000.0)
    acc = manager.get_account("ACC1")
    assert acc["id"] == "ACC1"
    assert acc["cash"] == 500_000.0


def test_create_account_with_positions(manager):
    positions = [{"symbol": "000001.SZ", "quantity": 200, "available_qty": 200, "cost_price": 10.5}]
    manager.create_account("ACC1", "账号1", initial_cash=1_000_000.0, positions=positions)
    acc = manager.get_account("ACC1")
    assert len(acc["positions"]) == 1
    assert acc["positions"][0]["symbol"] == "000001.SZ"


def test_reset_account(manager):
    manager.create_account("ACC1", "账号1", initial_cash=1_000_000.0)
    manager.deduct_cash("ACC1", 200_000.0)
    manager.reset_account("ACC1")
    acc = manager.get_account("ACC1")
    assert acc["cash"] == 1_000_000.0


def test_deduct_and_add_cash(manager):
    manager.create_account("ACC1", "账号1", initial_cash=1_000_000.0)
    manager.deduct_cash("ACC1", 100_000.0)
    acc = manager.get_account("ACC1")
    assert acc["cash"] == 900_000.0
    manager.add_cash("ACC1", 50_000.0)
    acc = manager.get_account("ACC1")
    assert acc["cash"] == 950_000.0


def test_update_position_buy(manager):
    manager.create_account("ACC1", "账号1", initial_cash=1_000_000.0)
    manager.update_position_on_trade("ACC1", "000001.SZ", "buy", quantity=100, price=10.0)
    acc = manager.get_account("ACC1")
    pos = next(p for p in acc["positions"] if p["symbol"] == "000001.SZ")
    assert pos["quantity"] == 100
    assert pos["cost_price"] == 10.0


def test_list_accounts(manager):
    manager.create_account("ACC1", "账号1", initial_cash=500_000.0)
    manager.create_account("ACC2", "账号2", initial_cash=800_000.0)
    accounts = manager.list_accounts()
    assert len(accounts) == 2


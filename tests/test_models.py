import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from qmt_simulator.simulator.models import Base, Account, Position, Order, Trade


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()


def test_create_account(session):
    acc = Account(id="10104711", name="测试账号", initial_cash=1_000_000.0, cash=1_000_000.0)
    session.add(acc)
    session.commit()
    result = session.query(Account).filter_by(id="10104711").first()
    assert result is not None
    assert result.cash == 1_000_000.0


def test_create_position(session):
    acc = Account(id="10104711", name="测试账号", initial_cash=1_000_000.0, cash=1_000_000.0)
    session.add(acc)
    pos = Position(account_id="10104711", symbol="000001.SZ", quantity=100, available_qty=100, cost_price=10.0)
    session.add(pos)
    session.commit()
    result = session.query(Position).filter_by(account_id="10104711").first()
    assert result.symbol == "000001.SZ"


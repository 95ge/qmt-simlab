"""SQLAlchemy ORM 数据模型"""
from datetime import datetime
from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"

    id = Column(String, primary_key=True)           # fund_account_id
    name = Column(String, nullable=False)
    initial_cash = Column(Float, nullable=False, default=1_000_000.0)
    cash = Column(Float, nullable=False, default=1_000_000.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    scenario_config = Column(Text, nullable=True)  # JSON：含场景概率、延迟、手续费


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, ForeignKey("accounts.id"), nullable=False)
    symbol = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False, default=0)
    available_qty = Column(Integer, nullable=False, default=0)
    cost_price = Column(Float, nullable=False, default=0.0)


class Order(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True)           # order_id from gateway
    account_id = Column(String, ForeignKey("accounts.id"), nullable=False)
    symbol = Column(String, nullable=False)
    direction = Column(String, nullable=False)       # buy / sell
    price_type = Column(String, nullable=False)      # limit / market
    price = Column(Float, nullable=False, default=0.0)
    quantity = Column(Integer, nullable=False)
    filled_qty = Column(Integer, nullable=False, default=0)
    filled_amount = Column(Float, nullable=False, default=0.0)
    status = Column(String, nullable=False, default="pending")
    reject_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Trade(Base):
    __tablename__ = "trades"

    id = Column(String, primary_key=True)
    order_id = Column(String, ForeignKey("orders.id"), nullable=False)
    account_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    commission = Column(Float, nullable=False, default=0.0)  # 手续费（元）
    trade_time = Column(Integer, nullable=False)     # 毫秒时间戳

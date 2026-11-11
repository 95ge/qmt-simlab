"""Small typed response models used by :mod:`qmt_sim_sdk`."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


class _Model:
    @classmethod
    def from_dict(cls, data: Optional[dict]):
        data = data or {}
        names = cls.__dataclass_fields__
        return cls(**{name: data[name] for name in names if name in data})

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Position(_Model):
    symbol: str = ""
    quantity: int = 0
    available_qty: int = 0
    cost_price: float = 0.0


@dataclass
class Account(_Model):
    id: str = ""
    name: str = ""
    initial_cash: float = 0.0
    cash: float = 0.0
    created_at: Optional[str] = None
    positions: List[Position] = field(default_factory=list)
    scenario_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[dict]):
        data = data or {}
        account = super().from_dict(data)
        account.positions = [Position.from_dict(item) for item in data.get("positions", [])]
        return account


@dataclass
class Balance(_Model):
    total_asset: float = 0.0
    cash: float = 0.0
    available_cash: float = 0.0
    frozen_cash: float = 0.0
    market_value: float = 0.0
    profit_loss: float = 0.0


@dataclass
class Order(_Model):
    order_id: str = ""
    external_order_id: str = ""
    symbol: str = ""
    direction: str = ""
    price_type: str = "limit"
    price: float = 0.0
    quantity: int = 0
    filled_qty: int = 0
    filled_amount: float = 0.0
    filled_quantity: int = 0
    filled_price: float = 0.0
    status: str = ""
    status_msg: str = ""
    reject_reason: Optional[str] = None
    account_id: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    trades: List[dict] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Optional[dict]):
        """Accept both REST representations (``id`` and ``order_id``)."""
        data = dict(data or {})
        if not data.get("order_id") and data.get("id"):
            data["order_id"] = data["id"]
        if not data.get("external_order_id") and data.get("order_id"):
            data["external_order_id"] = data["order_id"]
        return super().from_dict(data)


@dataclass
class Trade(_Model):
    id: str = ""
    trade_id: str = ""
    order_id: str = ""
    account_id: str = ""
    symbol: str = ""
    direction: str = ""
    price: float = 0.0
    quantity: int = 0
    amount: float = 0.0
    commission: float = 0.0
    trade_time: Any = None


@dataclass
class OrderResult(_Model):
    success: bool = False
    message: str = ""
    internal_order_id: str = ""
    external_order_id: str = ""
    status: str = ""
    filled_qty: int = 0
    filled_amount: float = 0.0
    avg_price: float = 0.0
    trades: List[dict] = field(default_factory=list)
    timed_out: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)


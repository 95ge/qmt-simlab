"""xtquant-style trading facade backed by the qmt-simlab REST API.

This module intentionally mirrors the common stock-trading surface of
``xtquant.xttrader``.  It does not pretend to implement QMT market data,
credit, futures or option-specific features; unsupported methods fail clearly
instead of silently placing a different kind of order.
"""

from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .client import SimulationAPIError, SimulationClient


class xtconstant:
    """The stock constants used by the public xtquant examples."""

    SH_MARKET = 0
    SZ_MARKET = 1
    SECURITY_ACCOUNT = 2
    CREDIT_ACCOUNT = 3

    STOCK_BUY = 23
    STOCK_SELL = 24
    CREDIT_BUY = 23
    CREDIT_SELL = 24

    LATEST_PRICE = 5
    FIX_PRICE = 11
    ANY_PRICE = 12

    ORDER_UNREPORTED = 48
    ORDER_WAIT_REPORTING = 49
    ORDER_REPORTED = 50
    ORDER_REPORTED_CANCEL = 51
    ORDER_PARTSUCC_CANCEL = 52
    ORDER_PART_CANCEL = 53
    ORDER_CANCELED = 54
    ORDER_PART_SUCC = 55
    ORDER_SUCCEEDED = 56
    ORDER_JUNK = 57
    ORDER_UNKNOWN = 255


STOCK_BUY = xtconstant.STOCK_BUY
STOCK_SELL = xtconstant.STOCK_SELL
FIX_PRICE = xtconstant.FIX_PRICE
LATEST_PRICE = xtconstant.LATEST_PRICE


class StockAccount:
    """Compatibility replacement for ``xtquant.xttype.StockAccount``."""

    def __init__(self, account_id: str, account_type: str = "STOCK"):
        self.account_id = str(account_id)
        self.account_type = account_type

    def __repr__(self) -> str:
        return f"StockAccount({self.account_id!r}, {self.account_type!r})"


@dataclass
class XtAsset:
    account_id: str
    total_asset: float = 0.0
    cash: float = 0.0
    frozen_cash: float = 0.0
    market_value: float = 0.0

    @property
    def m_dCash(self) -> float:
        return self.cash

    @property
    def m_dTotalAsset(self) -> float:
        return self.total_asset

    @property
    def m_dMarketValue(self) -> float:
        return self.market_value


@dataclass
class XtPosition:
    account_id: str
    stock_code: str
    volume: int = 0
    can_use_volume: int = 0
    open_price: float = 0.0
    avg_price: float = 0.0
    market_value: float = 0.0
    stock_name: str = ""
    frozen_volume: int = 0
    on_road_volume: int = 0
    yesterday_volume: int = 0


@dataclass
class XtOrder:
    account_id: str
    account_type: str
    stock_code: str
    order_type: int
    order_volume: int
    price_type: int
    price: float
    order_id: int | str
    order_status: int
    traded_volume: int = 0
    traded_price: float = 0.0
    traded_amount: float = 0.0
    order_time: str = ""
    order_remark: str = ""
    status_msg: str = ""
    order_sysid: str = ""
    strategy_name: str = ""


@dataclass
class XtTrade:
    account_id: str
    account_type: str
    stock_code: str
    order_type: int
    traded_volume: int
    traded_price: float
    traded_amount: float
    trade_id: str
    order_id: int | str
    trade_time: Any = None
    order_remark: str = ""


@dataclass
class XtOrderResponse:
    account_id: str
    account_type: str
    order_id: int | str
    seq: int
    order_remark: str = ""
    error_id: int = 0
    error_msg: str = ""


@dataclass
class XtCancelOrderResponse:
    account_id: str
    account_type: str
    order_id: int | str
    seq: int
    error_id: int = 0
    error_msg: str = ""


@dataclass
class XtOrderError:
    account_id: str
    account_type: str
    order_id: int | str
    error_id: int
    error_msg: str
    order_remark: str = ""


@dataclass
class XtCancelError:
    account_id: str
    account_type: str
    order_id: int | str
    error_id: int
    error_msg: str


@dataclass
class XtAccountStatus:
    account_id: str
    account_type: str = "STOCK"
    status: int = 0
    status_msg: str = "connected"


class XtQuantTraderCallback:
    """No-op callback base matching the common xtquant callback methods."""

    def on_disconnected(self):
        pass

    def on_stock_order(self, order):
        pass

    def on_stock_trade(self, trade):
        pass

    def on_stock_position(self, position):
        pass

    def on_stock_asset(self, asset):
        pass

    def on_order_error(self, order_error):
        pass

    def on_cancel_error(self, cancel_error):
        pass

    def on_order_stock_async_response(self, response):
        pass

    def on_cancel_order_stock_async_response(self, response):
        pass

    def on_account_status(self, status):
        pass


_STATUS_TO_CODE = {
    "submitted": xtconstant.ORDER_REPORTED,
    "partial_filled": xtconstant.ORDER_PART_SUCC,
    "filled": xtconstant.ORDER_SUCCEEDED,
    "canceled": xtconstant.ORDER_CANCELED,
    "rejected": xtconstant.ORDER_JUNK,
}


class XtQuantTrader:
    """xtquant-style stock trader backed by a qmt-simlab server.

    ``path`` is accepted for source compatibility with MiniQMT and is not
    used.  ``session_id`` remains useful because it namespaces generated
    public order IDs in the simulator.
    """

    def __init__(
        self,
        path: str = "",
        session_id: int = 0,
        *,
        server_url: str = "http://127.0.0.1:19720",
        client: Optional[SimulationClient] = None,
        poll_interval: float = 0.5,
    ):
        self.path = path
        self.session_id = int(session_id or int(time.time()))
        self.client = client or SimulationClient(server_url)
        self.poll_interval = max(0.1, float(poll_interval))
        self.callback: Optional[XtQuantTraderCallback] = None
        self.connected = False
        self._started = False
        self._stop_event = threading.Event()
        self._run_thread: Optional[threading.Thread] = None
        self._callback_cursor = 0
        self._subscriptions: Dict[str, StockAccount] = {}
        self._order_sequence = itertools.count(1)
        self._request_sequence = itertools.count(1)
        self._order_meta: Dict[str, Dict[str, Any]] = {}

    # Lifecycle -------------------------------------------------------------
    def register_callback(self, callback: XtQuantTraderCallback):
        self.callback = callback

    def start(self):
        self._started = True
        self._stop_event.clear()

    def connect(self) -> int:
        try:
            self.client.health()
        except SimulationAPIError:
            self.connected = False
            return -1
        self.connected = True
        if self.callback:
            for account in self._subscriptions.values():
                self._emit("on_account_status", XtAccountStatus(account.account_id, account.account_type))
        return 0

    def stop(self):
        self.connected = False
        self._stop_event.set()
        if self._run_thread and self._run_thread.is_alive():
            self._run_thread.join(timeout=2)
        self._run_thread = None
        if self.callback:
            self._emit("on_disconnected")

    def run_forever(self):
        if not self.connected:
            return
        self._start_event_pump()
        while not self._stop_event.wait(0.5):
            pass

    def set_relaxed_response_order_enabled(self, enabled: bool):
        # The HTTP adapter already handles requests outside the event pump.
        return None

    # Account subscription --------------------------------------------------
    def subscribe(self, account: StockAccount) -> int:
        account = self._account(account)
        self._subscriptions[account.account_id] = account
        if self.connected:
            self._emit("on_account_status", XtAccountStatus(account.account_id, account.account_type))
        self._start_event_pump()
        return 0 if self.connected else -1

    def unsubscribe(self, account: StockAccount) -> int:
        account = self._account(account)
        self._subscriptions.pop(account.account_id, None)
        return 0

    # Orders ----------------------------------------------------------------
    def order_stock(
        self,
        account: StockAccount,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> int:
        if not self.connected:
            return -1
        account = self._account(account)
        public_id = self._new_order_id()
        result = self._submit(
            account,
            public_id,
            stock_code,
            order_type,
            order_volume,
            price_type,
            price,
            strategy_name,
            order_remark,
        )
        return public_id if result.get("success") else -1

    def order_stock_async(self, account, stock_code, order_type, order_volume,
                          price_type, price, strategy_name="", order_remark="") -> int:
        if not self.connected:
            return -1
        seq = next(self._request_sequence)
        public_id = self._new_order_id()
        account = self._account(account)
        result = self._submit(
            account, public_id, stock_code, order_type, order_volume,
            price_type, price, strategy_name, order_remark,
        )
        response = XtOrderResponse(
            account.account_id, account.account_type, public_id, seq,
            order_remark=order_remark,
            error_id=0 if result.get("success") else -1,
            error_msg=result.get("message", "") if not result.get("success") else "",
        )
        self._emit("on_order_stock_async_response", response)
        return seq if result.get("success") else -1

    def cancel_order_stock(self, account: StockAccount, order_id: int | str) -> int:
        account = self._account(account)
        try:
            result = self.client.cancel_order(str(order_id))
            return 0 if result.get("success") else -1
        except SimulationAPIError:
            return -1

    def cancel_order_stock_async(self, account, order_id) -> int:
        seq = next(self._request_sequence)
        result = self.cancel_order_stock(account, order_id)
        account = self._account(account)
        response = XtCancelOrderResponse(
            account.account_id, account.account_type, order_id, seq,
            error_id=0 if result == 0 else -1,
            error_msg="" if result == 0 else "cancel failed",
        )
        self._emit("on_cancel_order_stock_async_response", response)
        return seq if result == 0 else -1

    def cancel_order_stock_sysid(self, account, market: int, order_sysid: str) -> int:
        return self.cancel_order_stock(account, order_sysid)

    def cancel_order_stock_sysid_async(self, account, market: int, order_sysid: str) -> int:
        return self.cancel_order_stock_async(account, order_sysid)

    # Queries ---------------------------------------------------------------
    def query_stock_asset(self, account: StockAccount) -> Optional[XtAsset]:
        account = self._account(account)
        try:
            data = self.client.get_balance(account.account_id)
        except SimulationAPIError:
            return None
        return XtAsset(
            account_id=account.account_id,
            total_asset=data.total_asset,
            cash=data.cash,
            frozen_cash=data.frozen_cash,
            market_value=data.market_value,
        )

    def query_stock_orders(self, account: StockAccount, cancelable_only: bool = False):
        account = self._account(account)
        try:
            orders = self.client.list_orders(account_id=account.account_id)
        except SimulationAPIError:
            return []
        result = [self._to_xt_order(account, order) for order in orders]
        if cancelable_only:
            result = [o for o in result if o.order_status in {
                xtconstant.ORDER_REPORTED,
                xtconstant.ORDER_PART_SUCC,
            }]
        return result

    def query_stock_order(self, account: StockAccount, order_id: int | str):
        account = self._account(account)
        try:
            order = self.client.get_order(str(order_id))
        except SimulationAPIError:
            return None
        return self._to_xt_order(account, order) if order else None

    def query_stock_trades(self, account: StockAccount):
        account = self._account(account)
        try:
            trades = self.client.list_trades(account_id=account.account_id)
        except SimulationAPIError:
            return []
        return [self._to_xt_trade(account, trade) for trade in trades]

    def query_stock_positions(self, account: StockAccount):
        account = self._account(account)
        try:
            positions = self.client.get_positions(account.account_id)
        except SimulationAPIError:
            return []
        return [
            XtPosition(
                account_id=account.account_id,
                stock_code=position.symbol,
                volume=position.quantity,
                can_use_volume=position.available_qty,
                open_price=position.cost_price,
                avg_price=position.cost_price,
                market_value=position.quantity * position.cost_price,
            )
            for position in positions
        ]

    def query_stock_position(self, account: StockAccount, stock_code: str):
        return next((p for p in self.query_stock_positions(account) if p.stock_code == stock_code), None)

    # Internal helpers -----------------------------------------------------
    @staticmethod
    def _account(account: StockAccount | str) -> StockAccount:
        return account if isinstance(account, StockAccount) else StockAccount(str(account))

    def _new_order_id(self) -> int:
        return self.session_id * 1_000_000 + next(self._order_sequence)

    def _submit(self, account, public_id, stock_code, order_type, volume,
                price_type, price, strategy_name, order_remark):
        direction = "buy" if order_type in (xtconstant.STOCK_BUY, xtconstant.CREDIT_BUY) else "sell"
        api_price_type = "limit" if price_type == xtconstant.FIX_PRICE and price > 0 else "market"
        try:
            result = self.client.submit_order(
                symbol=stock_code,
                direction=direction,
                quantity=int(volume),
                account_id=account.account_id,
                price=float(price),
                price_type=api_price_type,
                order_id=str(public_id),
            ).to_dict()
        except SimulationAPIError as exc:
            return {"success": False, "message": str(exc)}
        self._order_meta[str(public_id)] = {
            "account": account,
            "order_type": order_type,
            "strategy_name": strategy_name,
            "order_remark": order_remark,
        }
        return result

    def _start_event_pump(self):
        if self._run_thread and self._run_thread.is_alive():
            return
        if not self.connected:
            return
        try:
            existing = self.client.callbacks(0)
            self._callback_cursor = max((e.get("id", 0) for e in existing), default=0)
        except SimulationAPIError:
            return
        self._stop_event.clear()
        self._run_thread = threading.Thread(target=self._event_loop, daemon=True, name="qmt-simlab-events")
        self._run_thread.start()

    def _event_loop(self):
        while not self._stop_event.wait(self.poll_interval):
            try:
                events = self.client.callbacks(self._callback_cursor)
            except SimulationAPIError:
                continue
            for event in events:
                self._callback_cursor = max(self._callback_cursor, event.get("id", 0))
                data = event.get("data", {})
                account_id = str(data.get("account_id") or data.get("fund_account_id") or "")
                if self._subscriptions and account_id not in self._subscriptions:
                    continue
                if event.get("type") == "order_callback":
                    self._dispatch_order_event(data)
                elif event.get("type") == "trade_callback":
                    self._dispatch_trade_event(data)

    def _dispatch_order_event(self, data: dict):
        account = self._subscriptions.get(str(data.get("account_id") or data.get("fund_account_id")), StockAccount(str(data.get("account_id", ""))))
        server_id = str(data.get("order_id") or data.get("external_order_id") or "")
        meta = self._order_meta.get(server_id, {})
        public_id = self._public_order_id(server_id)
        order = XtOrder(
            account_id=account.account_id,
            account_type=account.account_type,
            stock_code=data.get("symbol", ""),
            order_type=meta.get("order_type", xtconstant.STOCK_BUY if data.get("direction") == "buy" else xtconstant.STOCK_SELL),
            order_volume=int(data.get("quantity", 0)),
            price_type=xtconstant.FIX_PRICE,
            price=float(data.get("price", 0.0)),
            order_id=public_id,
            order_status=_STATUS_TO_CODE.get(data.get("status", ""), xtconstant.ORDER_UNKNOWN),
            traded_volume=int(data.get("filled_quantity", data.get("filled_qty", 0)) or 0),
            traded_price=float(data.get("filled_price", data.get("avg_filled_price", 0.0)) or 0.0),
            order_remark=meta.get("order_remark", ""),
            status_msg=data.get("status_msg", ""),
            order_sysid=server_id,
            strategy_name=meta.get("strategy_name", ""),
        )
        self._emit("on_stock_order", order)
        if order.order_status == xtconstant.ORDER_JUNK:
            self._emit("on_order_error", XtOrderError(
                account.account_id, account.account_type, public_id, -1,
                order.status_msg or "order rejected", order.order_remark,
            ))

    def _dispatch_trade_event(self, data: dict):
        account = self._subscriptions.get(str(data.get("account_id") or data.get("fund_account_id")), StockAccount(str(data.get("account_id", ""))))
        server_id = str(data.get("order_id") or "")
        meta = self._order_meta.get(server_id, {})
        self._emit("on_stock_trade", XtTrade(
            account_id=account.account_id,
            account_type=account.account_type,
            stock_code=data.get("symbol", ""),
            order_type=meta.get("order_type", xtconstant.STOCK_BUY if data.get("direction") == "buy" else xtconstant.STOCK_SELL),
            traded_volume=int(data.get("quantity", 0)),
            traded_price=float(data.get("price", 0.0)),
            traded_amount=float(data.get("amount", 0.0)),
            trade_id=str(data.get("trade_id", "")),
            order_id=self._public_order_id(server_id),
            trade_time=data.get("trade_time"),
            order_remark=meta.get("order_remark", ""),
        ))

    def _public_order_id(self, server_id: str) -> int | str:
        try:
            return int(server_id)
        except (TypeError, ValueError):
            return server_id

    def _to_xt_order(self, account, order):
        raw_id = order.order_id or order.external_order_id
        meta = self._order_meta.get(str(raw_id), {})
        return XtOrder(
            account_id=account.account_id,
            account_type=account.account_type,
            stock_code=order.symbol,
            order_type=meta.get("order_type", xtconstant.STOCK_BUY if order.direction == "buy" else xtconstant.STOCK_SELL),
            order_volume=order.quantity,
            price_type=xtconstant.FIX_PRICE if order.price_type == "limit" else xtconstant.LATEST_PRICE,
            price=order.price,
            order_id=self._public_order_id(str(raw_id)),
            order_status=_STATUS_TO_CODE.get(order.status, xtconstant.ORDER_UNKNOWN),
            traded_volume=order.filled_quantity or order.filled_qty,
            traded_price=order.filled_price,
            traded_amount=order.filled_amount,
            order_time=order.created_at or "",
            order_remark=meta.get("order_remark", ""),
            status_msg=order.status_msg,
            order_sysid=order.external_order_id or str(raw_id),
            strategy_name=meta.get("strategy_name", ""),
        )

    @staticmethod
    def _to_xt_trade(account, trade):
        order_type = xtconstant.STOCK_BUY if trade.direction == "buy" else xtconstant.STOCK_SELL
        return XtTrade(
            account_id=account.account_id,
            account_type=account.account_type,
            stock_code=trade.symbol,
            order_type=order_type,
            traded_volume=trade.quantity,
            traded_price=trade.price,
            traded_amount=trade.amount,
            trade_id=trade.trade_id or trade.id,
            order_id=trade.order_id,
            trade_time=trade.trade_time,
        )

    def _emit(self, method: str, *args):
        callback = self.callback
        if callback is None:
            return
        fn = getattr(callback, method, None)
        if fn:
            try:
                fn(*args)
            except Exception:
                # Callback failures must not stop the event pump.
                pass


__all__ = [
    "StockAccount",
    "XtAccountStatus",
    "XtAsset",
    "XtCancelError",
    "XtCancelOrderResponse",
    "XtOrder",
    "XtOrderError",
    "XtOrderResponse",
    "XtPosition",
    "XtQuantTrader",
    "XtQuantTraderCallback",
    "XtTrade",
    "xtconstant",
]


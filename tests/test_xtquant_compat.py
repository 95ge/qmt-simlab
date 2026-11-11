from qmt_sim_sdk.models import Balance, Order, OrderResult, Position, Trade
from qmt_sim_sdk.xtquant import xtconstant
from qmt_sim_sdk.xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from qmt_sim_sdk.xtquant.xttype import StockAccount


class FakeClient:
    def __init__(self):
        self.submissions = []
        self.orders = []

    def health(self):
        return {"status": "healthy"}

    def callbacks(self, since=0):
        return []

    def submit_order(self, **payload):
        self.submissions.append(payload)
        order = OrderResult(
            success=True,
            internal_order_id=payload["order_id"],
            external_order_id=payload["order_id"],
            status="submitted",
        )
        return order

    def cancel_order(self, order_id):
        return {"success": True}

    def get_balance(self, account_id):
        return Balance(total_asset=100_000, cash=98_000, market_value=2_000)

    def list_orders(self, account_id=None):
        return self.orders

    def get_order(self, order_id):
        return self.orders[0] if self.orders else None

    def list_trades(self, account_id=None):
        return [Trade(
            id="T1", trade_id="T1", order_id="1001", account_id=account_id,
            symbol="000001.SZ", direction="buy", price=10.0, quantity=100,
            amount=1_000,
        )]

    def get_positions(self, account_id):
        return [Position(symbol="000001.SZ", quantity=100, available_qty=100, cost_price=10.0)]


class RecordingCallback(XtQuantTraderCallback):
    def __init__(self):
        self.orders = []
        self.trades = []
        self.responses = []

    def on_stock_order(self, order):
        self.orders.append(order)

    def on_stock_trade(self, trade):
        self.trades.append(trade)

    def on_order_stock_async_response(self, response):
        self.responses.append(response)


def test_xtquant_constants_and_order_surface():
    assert xtconstant.STOCK_BUY == 23
    assert xtconstant.STOCK_SELL == 24
    assert xtconstant.FIX_PRICE == 11
    assert xtconstant.ORDER_SUCCEEDED == 56

    fake = FakeClient()
    trader = XtQuantTrader("", 7, client=fake)
    account = StockAccount("demo")
    callback = RecordingCallback()
    trader.register_callback(callback)
    trader.start()

    assert trader.connect() == 0
    assert trader.subscribe(account) == 0
    order_id = trader.order_stock(
        account, "000001.SZ", xtconstant.STOCK_BUY, 100,
        xtconstant.FIX_PRICE, 10.0, "strategy", "remark",
    )

    assert order_id == 7_000_001
    assert fake.submissions[0]["direction"] == "buy"
    assert fake.submissions[0]["price_type"] == "limit"
    assert fake.submissions[0]["account_id"] == "demo"
    assert trader.query_stock_asset(account).cash == 98_000
    assert trader.query_stock_positions(account)[0].can_use_volume == 100
    assert trader.query_stock_trades(account)[0].trade_id == "T1"

    async_seq = trader.order_stock_async(
        account, "000001.SZ", xtconstant.STOCK_BUY, 100,
        xtconstant.FIX_PRICE, 10.0,
    )
    assert async_seq == 1
    assert callback.responses[0].error_id == 0


def test_xtquant_callbacks_translate_simulator_events():
    fake = FakeClient()
    trader = XtQuantTrader("", 1, client=fake)
    account = StockAccount("demo")
    callback = RecordingCallback()
    trader.register_callback(callback)
    trader.start()
    trader.connect()
    trader.subscribe(account)

    trader._dispatch_order_event({
        "account_id": "demo", "order_id": "1000001", "symbol": "000001.SZ",
        "direction": "buy", "price": 10.0, "quantity": 100,
        "status": "filled", "filled_quantity": 100, "filled_price": 10.0,
    })
    trader._dispatch_trade_event({
        "account_id": "demo", "order_id": "1000001", "symbol": "000001.SZ",
        "direction": "buy", "quantity": 100, "price": 10.0,
        "amount": 1_000, "trade_id": "T1",
    })

    assert callback.orders[0].order_status == xtconstant.ORDER_SUCCEEDED
    assert callback.orders[0].order_id == 1_000_001
    assert callback.trades[0].trade_id == "T1"

"""订单引擎：接收指令，随机决定结果，分批回调"""
import logging
import random
import threading
import time
import uuid
from datetime import datetime
from typing import Callable, Dict, Optional

from sqlalchemy.orm import sessionmaker

from .account_manager import AccountManager
from .models import Order, Trade
from .scenario import ScenarioConfig, CommissionConfig

logger = logging.getLogger(__name__)

REJECT_REASONS = ["风控拒绝", "账户余额不足", "标的停牌", "超出涨跌幅限制", "系统繁忙"]


class OrderEngine:
    def __init__(self, manager: AccountManager, session_factory: sessionmaker,
                 scenario: ScenarioConfig, commission: CommissionConfig = None):
        self.manager = manager
        self._session_factory = session_factory
        self.scenario = scenario
        self.commission = commission or CommissionConfig()
        self._cancel_events: Dict[str, threading.Event] = {}
        self._completion_events: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()

        # 回调属性（由外部设置）
        self.on_order_callback: Optional[Callable] = None
        self.on_trade_callback: Optional[Callable] = None

        # 手动成交：等待决策的订单
        self._manual_pending: Dict[str, threading.Event] = {}   # order_id → Event
        self._manual_decisions: Dict[str, dict] = {}            # order_id → decision

    # ── 公开接口 ──────────────────────────────────────────────

    def submit_order_sync(self, order_data: dict) -> dict:
        fund_id = order_data.get("fund_account_id") or order_data.get("account") or order_data.get("account_id", "")
        order_id = order_data.get("order_id") or str(uuid.uuid4())
        symbol = order_data.get("symbol", "")
        direction = order_data.get("direction", "buy")
        price = float(order_data.get("price") or 10.0)
        quantity = int(order_data.get("quantity", 0))
        price_type = order_data.get("price_type", "limit")

        if price_type == "market":
            price = 10.0

        # 风控校验
        reject_reason = self._risk_check(fund_id, direction, price, quantity)
        if reject_reason:
            self._fire_order(order_id, order_id, symbol, "rejected", reject_reason, 0, 0.0,
                             direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return {"success": False, "external_order_id": "", "error_code": -1, "message": reject_reason}

        external_id = order_id  # 仿真中保持一致

        # 持久化订单
        db = self._session_factory()
        try:
            db.add(Order(
                id=order_id,
                account_id=fund_id,
                symbol=symbol,
                direction=direction,
                price_type=price_type,
                price=price,
                quantity=quantity,
                filled_qty=0,
                filled_amount=0.0,
                status="submitted",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ))
            db.commit()
        finally:
            db.close()

        # 立即回调 submitted
        self._fire_order(order_id, external_id, symbol, "submitted", "已报", 0, 0.0,
                         direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)

        # 创建撤单 Event，启动后台成交线程
        cancel_event = threading.Event()
        with self._lock:
            self._cancel_events[external_id] = cancel_event

        threading.Thread(
            target=self._fill_loop,
            args=(order_id, external_id, fund_id, symbol, direction, price, quantity, cancel_event),
            daemon=True,
        ).start()

        return {"success": True, "internal_order_id": order_id, "external_order_id": external_id, "error_code": 0, "message": "订单已提交"}

    def submit_order_blocking(self, order_data: dict, timeout: float = 30.0) -> dict:
        """提交订单并阻塞等待终态（成交/拒绝/撤单）或超时。

        适用于算法引擎子订单：提交后等待完成再提交下一单。
        """
        order_id = order_data.get("order_id") or str(uuid.uuid4())
        order_data = {**order_data, "order_id": order_id}

        # 先注册 completion event（在 submit 前，防止极速成交时错过信号）
        evt = threading.Event()
        with self._lock:
            self._completion_events[order_id] = evt

        result = self.submit_order_sync(order_data)
        if not result.get("success"):
            # 提交失败（风控拦截），清理 completion event
            with self._lock:
                self._completion_events.pop(order_id, None)
            return result

        # 等待终态；超时后自动撤单（TTL 机制）
        completed = evt.wait(timeout=timeout)

        if not completed:
            # 订单超时未达终态，主动撤单以避免挂单残留
            external_id = result.get("external_order_id", "")
            if external_id:
                self.cancel_order_sync({"external_order_id": external_id})
                evt.wait(timeout=5.0)   # 等待撤单回调触发终态

        # 读取最终状态
        order_final = self.get_order_with_trades(order_id)
        if order_final:
            result["status"] = order_final.get("status", "unknown")
            filled_qty = order_final.get("filled_qty", 0)
            filled_amount = order_final.get("filled_amount", 0.0)
            result["filled_qty"] = filled_qty
            result["filled_amount"] = filled_amount
            result["avg_price"] = round(filled_amount / filled_qty, 4) if filled_qty > 0 else 0.0
            result["trades"] = order_final.get("trades", [])
            if order_final.get("reject_reason"):
                result["reject_reason"] = order_final["reject_reason"]
            result["timed_out"] = not completed   # 标记是否因超时而撤单

        return result

    def subscribe_completion(self, order_id: str) -> threading.Event:
        """预注册 completion 事件，在 submit 前调用以避免竞争。"""
        evt = threading.Event()
        with self._lock:
            self._completion_events[order_id] = evt
        return evt

    def cancel_order_sync(self, order_data: dict) -> dict:
        ext_id = order_data.get("external_order_id", "")
        with self._lock:
            event = self._cancel_events.get(ext_id)
        if event:
            event.set()
            return {"success": True, "message": "撤单指令已发送"}
        # 检查订单是否已结束
        db = self._session_factory()
        try:
            order = db.query(Order).filter_by(id=ext_id).first()
            if order and order.status in ("filled", "rejected", "canceled"):
                return {"success": False, "message": f"订单已{order.status}，无法撤单"}
        finally:
            db.close()
        return {"success": False, "message": "订单不存在"}

    def query_positions_sync(self, fund_account_id: str) -> list:
        acc = self.manager.get_account(fund_account_id)
        if not acc:
            return []
        return acc.get("positions", [])

    def query_balance_sync(self, fund_account_id: str) -> dict:
        acc = self.manager.get_account(fund_account_id)
        if not acc:
            return {"total_asset": 0.0, "cash": 0.0, "market_value": 0.0}
        cash = acc["cash"]
        market_value = sum(p["quantity"] * p["cost_price"] for p in acc.get("positions", []))
        return {
            "total_asset": cash + market_value,
            "cash": cash,
            "available_cash": cash,
            "market_value": market_value,
            "frozen_cash": 0.0,
            "profit_loss": 0.0,
        }

    def query_order_sync(self, external_order_id: str, account_id: str, fund_account_id: str):
        """order_id 为空时返回该账户全部近期订单（含成交明细）列表"""
        db = self._session_factory()
        try:
            if not external_order_id:
                # 查全部订单
                q = db.query(Order)
                if fund_account_id:
                    q = q.filter_by(account_id=fund_account_id)
                elif account_id:
                    q = q.filter_by(account_id=account_id)
                orders = q.order_by(Order.created_at.desc()).limit(100).all()
                result = []
                for o in orders:
                    trades = db.query(Trade).filter_by(order_id=o.id).all()
                    result.append({
                        "order_id": o.id,
                        "symbol": o.symbol,
                        "direction": o.direction,
                        "price_type": o.price_type,
                        "price": o.price,
                        "quantity": o.quantity,
                        "filled_qty": o.filled_qty,
                        "filled_amount": o.filled_amount,
                        "status": o.status,
                        "created_at": o.created_at.strftime("%Y-%m-%d %H:%M:%S") if o.created_at else "",
                        "trades": [
                            {"price": t.price, "quantity": t.quantity, "amount": t.amount,
                             "commission": t.commission, "trade_time": t.trade_time}
                            for t in trades
                        ],
                    })
                return result
            # 查单笔订单
            order = db.query(Order).filter_by(id=external_order_id).first()
            if not order:
                return {}
            return {
                "order_id": order.id,
                "external_order_id": order.id,
                "symbol": order.symbol,
                "direction": order.direction,
                "price": order.price,
                "quantity": order.quantity,
                "status": order.status,
                "filled_quantity": order.filled_qty,
                "filled_price": order.price if order.filled_qty else 0.0,
                "status_msg": order.status,
            }
        finally:
            db.close()

    def get_orders(self, account_id: str = None, status: str = None,
                   limit: int = 50, offset: int = 0,
                   date_from: str = None, date_to: str = None) -> list:
        from datetime import datetime as _dt2, timedelta as _td
        db = self._session_factory()
        try:
            q = db.query(Order)
            if account_id:
                q = q.filter_by(account_id=account_id)
            if status:
                q = q.filter_by(status=status)
            if date_from:
                q = q.filter(Order.created_at >= _dt2.strptime(date_from, "%Y-%m-%d"))
            if date_to:
                q = q.filter(Order.created_at < _dt2.strptime(date_to, "%Y-%m-%d") + _td(days=1))
            orders = q.order_by(Order.created_at.desc()).offset(offset).limit(limit).all()
            return [self._order_to_dict(o) for o in orders]
        finally:
            db.close()

    def get_trades(self, account_id: str = None, limit: int = 50, offset: int = 0,
                   date_from: str = None, date_to: str = None) -> list:
        from datetime import datetime as _dt2, timedelta as _td
        db = self._session_factory()
        try:
            q = db.query(Trade)
            if account_id:
                q = q.filter_by(account_id=account_id)
            if date_from:
                ts_from = int(_dt2.strptime(date_from, "%Y-%m-%d").timestamp() * 1000)
                q = q.filter(Trade.trade_time >= ts_from)
            if date_to:
                ts_to = int((_dt2.strptime(date_to, "%Y-%m-%d") + _td(days=1)).timestamp() * 1000)
                q = q.filter(Trade.trade_time < ts_to)
            trades = q.order_by(Trade.trade_time.desc()).offset(offset).limit(limit).all()
            result = []
            for t in trades:
                # trade_time 存的是毫秒时间戳，转成可读字符串
                try:
                    from datetime import datetime as _dt
                    ts_str = _dt.fromtimestamp(t.trade_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_str = str(t.trade_time)
                result.append({
                    "id": t.id,
                    "order_id": t.order_id,
                    "account_id": t.account_id,
                    "symbol": t.symbol,
                    "direction": t.direction,
                    "price": t.price,
                    "quantity": t.quantity,
                    "amount": t.amount,
                    "commission": t.commission,
                    "trade_time": ts_str,
                })
            return result
        finally:
            db.close()

    def get_order_with_trades(self, order_id: str) -> dict:
        db = self._session_factory()
        try:
            order = db.query(Order).filter_by(id=order_id).first()
            if not order:
                return {}
            trades = db.query(Trade).filter_by(order_id=order_id).all()
            d = self._order_to_dict(order)
            def _fmt_ts(ms):
                try:
                    from datetime import datetime as _dt
                    return _dt.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return str(ms)

            d["trades"] = [
                {
                    "trade_id": t.id, "price": t.price, "quantity": t.quantity,
                    "amount": t.amount, "commission": t.commission,
                    "trade_time": _fmt_ts(t.trade_time),
                }
                for t in trades
            ]
            return d
        finally:
            db.close()

    def get_pending_manual_fills(self) -> list:
        """返回等待手动成交决策的订单列表"""
        with self._lock:
            pending_ids = list(self._manual_pending.keys())
        result = []
        for oid in pending_ids:
            order = self.get_order_with_trades(oid)
            if order:
                result.append(order)
        return result

    def decide_manual_fill(self, order_id: str, action: str,
                           fill_qty: int = None, fill_price: float = None,
                           reason: str = None) -> bool:
        """处理手动成交决策。action: 'fill' | 'reject' | 'cancel'"""
        with self._lock:
            evt = self._manual_pending.get(order_id)
            if not evt:
                return False
            self._manual_decisions[order_id] = {
                'action': action,
                'fill_qty': fill_qty,
                'fill_price': fill_price,
                'reason': reason,
            }
        evt.set()
        return True

    # ── 内部逻辑 ──────────────────────────────────────────────

    def _risk_check(self, fund_id: str, direction: str, price: float, quantity: int) -> Optional[str]:
        if direction == "buy":
            needed = price * quantity
            cash = self.manager.get_cash(fund_id)
            if cash < needed:
                return f"可用资金不足（需要 {needed:.2f}，可用 {cash:.2f}）"
        return None

    def _fill_loop(self, order_id, external_id, fund_id, symbol, direction, price, quantity, cancel_event):
        # 加载账号专属配置（fill_mode 也存储在此，fallback 到 auto）
        _cfg = self.manager.get_account_config(fund_id)

        if _cfg.get('fill_mode', 'auto') == 'manual':
            self._handle_manual_fill(order_id, external_id, fund_id, symbol, direction, price, quantity, cancel_event)
            return

        from .scenario import ScenarioConfig, CommissionConfig
        # Use the engine's configured scenario as the fallback.  Reading the
        # module-level default here made per-engine test and service settings
        # silently fall back to a five-second delay.
        _defaults = self.scenario.to_dict()

        s = ScenarioConfig(
            filled_prob=_cfg.get("filled_prob", _defaults["filled_prob"]),
            partial_filled_prob=_cfg.get("partial_filled_prob", _defaults["partial_filled_prob"]),
            rejected_prob=_cfg.get("rejected_prob", _defaults["rejected_prob"]),
            canceled_prob=_cfg.get("canceled_prob", _defaults["canceled_prob"]),
            initial_delay=_cfg.get("initial_delay", _defaults["initial_delay"]),
            batch_interval_min=_cfg.get("batch_interval_min", _defaults["batch_interval_min"]),
            batch_interval_max=_cfg.get("batch_interval_max", _defaults["batch_interval_max"]),
            batch_count_min=_cfg.get("batch_count_min", _defaults["batch_count_min"]),
            batch_count_max=_cfg.get("batch_count_max", _defaults["batch_count_max"]),
        )
        _comm_cfg = _cfg.get("commission", self.commission.to_dict())
        comm = CommissionConfig(
            enabled=_comm_cfg.get("enabled", True),
            commission_rate=_comm_cfg.get("commission_rate", 0.00025),
            min_commission=_comm_cfg.get("min_commission", 5.0),
            stamp_duty_rate=_comm_cfg.get("stamp_duty_rate", 0.001),
            transfer_fee_rate=_comm_cfg.get("transfer_fee_rate", 0.00002),
        )

        cancel_event.wait(s.initial_delay)
        if cancel_event.is_set():
            self._finalize_order(order_id, external_id, symbol, "canceled", "已撤单",
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return

        outcome = random.choices(
            ["filled", "partial_filled", "rejected", "canceled"],
            weights=[s.filled_prob, s.partial_filled_prob, s.rejected_prob, s.canceled_prob],
        )[0]

        if outcome == "rejected":
            reason = random.choice(REJECT_REASONS)
            self._finalize_order(order_id, external_id, symbol, "rejected", reason,
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return

        if outcome == "canceled":
            self._finalize_order(order_id, external_id, symbol, "canceled", "已撤单",
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return

        if outcome == "filled":
            # 全量成交：一次性分 batch_count 批完成
            fill_qty = quantity
            batch_count = random.randint(s.batch_count_min, s.batch_count_max)
            batches = self._split_into_batches(fill_qty, batch_count)
            cumulative_qty = 0

            for i, batch_qty in enumerate(batches):
                if cancel_event.is_set():
                    self._finalize_order(order_id, external_id, symbol, "canceled", "已撤单",
                                         direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
                    return

                cumulative_qty += batch_qty
                is_last = (i == len(batches) - 1)
                final_status = "filled" if is_last else "partial_filled"

                fill_price = round(price * random.uniform(0.99, 1.01), 2)
                amount = fill_price * batch_qty
                commission_val = comm.calculate(direction, amount)

                self._update_order_in_db(order_id, cumulative_qty, amount, final_status)

                if direction == "buy":
                    self.manager.deduct_cash(fund_id, amount + commission_val)
                else:
                    self.manager.add_cash(fund_id, amount - commission_val)
                self.manager.update_position_on_trade(fund_id, symbol, direction, batch_qty, fill_price)

                trade_id = str(uuid.uuid4())
                trade_time = int(time.time() * 1000)
                self._persist_trade(trade_id, order_id, fund_id, symbol, direction, fill_price, batch_qty, amount, commission_val, trade_time)
                self._fire_trade(trade_id, order_id, external_id, fund_id, symbol, direction, fill_price, batch_qty, amount, commission_val, trade_time)
                self._fire_order(order_id, external_id, symbol, final_status, final_status, cumulative_qty, fill_price,
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)

                if not is_last:
                    interval = random.uniform(s.batch_interval_min, s.batch_interval_max)
                    cancel_event.wait(interval)

            with self._lock:
                self._cancel_events.pop(external_id, None)
            self._signal_completion(order_id)

        else:  # partial_filled：挂单模式，每隔一段时间成交一批，直到撤单或全部成交
            remaining = quantity
            cumulative_qty = 0
            first_fill = True

            while remaining > 0:
                if cancel_event.is_set():
                    self._finalize_order(order_id, external_id, symbol, "canceled", "已撤单",
                                         direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
                    return

                # 每批成交剩余数量的 10%~40%，至少 1 手
                batch_pct = random.uniform(0.1, 0.4)
                batch_qty = max(1, int(remaining * batch_pct))
                batch_qty = min(batch_qty, remaining)

                cumulative_qty += batch_qty
                remaining -= batch_qty
                is_last = (remaining == 0)
                final_status = "filled" if is_last else "partial_filled"

                fill_price = round(price * random.uniform(0.99, 1.01), 2)
                amount = fill_price * batch_qty
                commission_val = comm.calculate(direction, amount)

                self._update_order_in_db(order_id, cumulative_qty, amount, final_status)

                if direction == "buy":
                    self.manager.deduct_cash(fund_id, amount + commission_val)
                else:
                    self.manager.add_cash(fund_id, amount - commission_val)
                self.manager.update_position_on_trade(fund_id, symbol, direction, batch_qty, fill_price)

                trade_id = str(uuid.uuid4())
                trade_time = int(time.time() * 1000)
                self._persist_trade(trade_id, order_id, fund_id, symbol, direction, fill_price, batch_qty, amount, commission_val, trade_time)
                self._fire_trade(trade_id, order_id, external_id, fund_id, symbol, direction, fill_price, batch_qty, amount, commission_val, trade_time)
                self._fire_order(order_id, external_id, symbol, final_status, final_status, cumulative_qty, fill_price,
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)

                if first_fill:
                    # 首次成交后通知阻塞调用方，让其获取部分成交状态
                    first_fill = False
                    self._signal_completion(order_id)

                if is_last:
                    with self._lock:
                        self._cancel_events.pop(external_id, None)
                    return

                # 等待下一批成交，期间可被撤单信号唤醒
                interval = random.uniform(s.batch_interval_min, s.batch_interval_max)
                cancel_event.wait(interval)

    def _handle_manual_fill(self, order_id, external_id, fund_id, symbol, direction, price, quantity, cancel_event):
        """手动成交模式：等待管理员在 UI 上作出决策后再处理结果。"""
        decision_evt = threading.Event()
        with self._lock:
            self._manual_pending[order_id] = decision_evt

        # 循环等待，同时响应撤单信号（cancel_event）
        while True:
            if decision_evt.wait(timeout=1.0):
                break
            if cancel_event.is_set():
                with self._lock:
                    self._manual_pending.pop(order_id, None)
                    self._manual_decisions.pop(order_id, None)
                self._finalize_order(order_id, external_id, symbol, 'canceled', '已撤单',
                                     direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
                return

        with self._lock:
            self._manual_pending.pop(order_id, None)
            decision = self._manual_decisions.pop(order_id, None)

        if not decision or decision['action'] == 'cancel':
            self._finalize_order(order_id, external_id, symbol, 'canceled', '手动撤单',
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return

        if decision['action'] == 'reject':
            reason = decision.get('reason') or '手动拒绝'
            self._finalize_order(order_id, external_id, symbol, 'rejected', reason,
                                 direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)
            return

        # action == 'fill'
        fill_qty = decision.get('fill_qty') or quantity
        fill_qty = min(max(1, int(fill_qty)), quantity)
        fill_price = decision.get('fill_price') or price
        fill_price = max(0.01, float(fill_price))

        amount = fill_price * fill_qty
        commission_val = self.commission.calculate(direction, amount)

        final_status = 'filled' if fill_qty >= quantity else 'partial_filled'
        self._update_order_in_db(order_id, fill_qty, amount, final_status)

        if direction == 'buy':
            self.manager.deduct_cash(fund_id, amount + commission_val)
        else:
            self.manager.add_cash(fund_id, amount - commission_val)
        self.manager.update_position_on_trade(fund_id, symbol, direction, fill_qty, fill_price)

        trade_id = str(uuid.uuid4())
        trade_time = int(time.time() * 1000)
        self._persist_trade(trade_id, order_id, fund_id, symbol, direction,
                            fill_price, fill_qty, amount, commission_val, trade_time)
        self._fire_trade(trade_id, order_id, external_id, fund_id, symbol, direction,
                         fill_price, fill_qty, amount, commission_val, trade_time)
        self._fire_order(order_id, external_id, symbol, final_status, final_status, fill_qty, fill_price,
                         direction=direction, price=price, quantity=quantity, fund_account_id=fund_id)

        with self._lock:
            self._cancel_events.pop(external_id, None)
        self._signal_completion(order_id)

    def _split_into_batches(self, total: int, count: int) -> list:
        if count <= 1 or total <= 1:
            return [total]
        splits = sorted(random.sample(range(1, total), min(count - 1, total - 1)))
        boundaries = [0] + splits + [total]
        return [boundaries[i + 1] - boundaries[i] for i in range(len(boundaries) - 1)]

    def _finalize_order(self, order_id, external_id, symbol, status, msg,
                        direction="", price=0.0, quantity=0, fund_account_id=""):
        self._update_order_status_in_db(order_id, status)
        self._fire_order(order_id, external_id, symbol, status, msg, 0, 0.0,
                         direction=direction, price=price, quantity=quantity, fund_account_id=fund_account_id)
        with self._lock:
            self._cancel_events.pop(external_id, None)
        self._signal_completion(order_id)

    def _signal_completion(self, order_id: str):
        """通知等待该订单完成的 subscriber（一次性）。"""
        with self._lock:
            evt = self._completion_events.pop(order_id, None)
        if evt:
            evt.set()

    def _update_order_in_db(self, order_id, filled_qty, filled_amount_increment, status):
        db = self._session_factory()
        try:
            order = db.query(Order).filter_by(id=order_id).first()
            if order:
                order.filled_qty = filled_qty
                order.filled_amount += filled_amount_increment
                order.status = status
                order.updated_at = datetime.utcnow()
                db.commit()
        finally:
            db.close()

    def _update_order_status_in_db(self, order_id, status):
        db = self._session_factory()
        try:
            order = db.query(Order).filter_by(id=order_id).first()
            if order:
                order.status = status
                order.updated_at = datetime.utcnow()
                db.commit()
        finally:
            db.close()

    def _persist_trade(self, trade_id, order_id, account_id, symbol, direction, price, quantity, amount, commission, trade_time):
        db = self._session_factory()
        try:
            db.add(Trade(
                id=trade_id, order_id=order_id, account_id=account_id,
                symbol=symbol, direction=direction, price=price,
                quantity=quantity, amount=amount, commission=commission,
                trade_time=trade_time,
            ))
            db.commit()
        except Exception as e:
            logger.error(f"持久化成交记录失败: {e}")
            db.rollback()
        finally:
            db.close()

    def _fire_order(self, order_id, external_id, symbol, status, status_msg, filled_quantity, avg_filled_price,
                    direction="", price=0.0, quantity=0, fund_account_id=""):
        if self.on_order_callback:
            try:
                self.on_order_callback({
                    "internal_order_id": order_id,
                    "order_id": order_id,
                    "external_order_id": external_id,
                    "account_id": fund_account_id,
                    "symbol": symbol,
                    "direction": direction,
                    "price": price,
                    "quantity": quantity,
                    "status": status,
                    "status_msg": status_msg,
                    "filled_quantity": filled_quantity,
                    "avg_filled_price": avg_filled_price,
                    "order_time": str(datetime.utcnow()),
                })
            except Exception as e:
                logger.error(f"on_order_callback 异常: {e}")

    def _fire_trade(self, trade_id, order_id, external_id, fund_account_id, symbol, direction, price, quantity, amount, commission, trade_time):
        if self.on_trade_callback:
            try:
                self.on_trade_callback({
                    "trade_id": trade_id,
                    "internal_order_id": order_id,
                    "order_id": order_id,
                    "external_order_id": external_id,
                    "account_id": fund_account_id,
                    "symbol": symbol,
                    "direction": direction,
                    "price": price,
                    "quantity": quantity,
                    "amount": amount,
                    "commission": commission,
                    "trade_time": trade_time,
                })
            except Exception as e:
                logger.error(f"on_trade_callback 异常: {e}")

    def _order_to_dict(self, order: Order) -> dict:
        return {
            "id": order.id,
            "account_id": order.account_id,
            "symbol": order.symbol,
            "direction": order.direction,
            "price_type": order.price_type,
            "price": order.price,
            "quantity": order.quantity,
            "filled_qty": order.filled_qty,
            "filled_amount": order.filled_amount,
            "status": order.status,
            "reject_reason": order.reject_reason,
            "created_at": order.created_at.isoformat() if order.created_at else None,
            "updated_at": order.updated_at.isoformat() if order.updated_at else None,
        }

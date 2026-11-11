"""
仿真客户端算法执行器

与 gj_executor-qmt/executor/algo_executor.py 接口完全一致，
底层通过 SimExecutor.submit_order_blocking() 等待仿真服务器成交，
并向网关上报 order_callback / trade_callback。
"""
import logging
import time
import threading
import uuid
import urllib.request
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class FatalAlgoError(Exception):
    """子单遇到不可恢复错误（拒绝、资金不足等），需立即终止整个算法任务。"""


class AlgoType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    TWAP = "twap"
    POV = "pov"
    ICEBERG = "iceberg"
    VWAP = "vwap"
    LIMIT_UP = "limit_up"
    FAST_TRADE = "fast_trade"
    STEALTH = "stealth"


class AlgoStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class AlgoTask:
    algo_id: str
    algo_type: str
    symbol: str
    symbol_name: str = ""
    direction: str = "buy"
    total_quantity: int = 0
    params: dict = field(default_factory=dict)

    status: str = "pending"
    progress: float = 0.0
    filled_quantity: int = 0
    avg_price: float = 0.0
    total_amount: float = 0.0
    total_commission: float = 0.0   # 累计手续费
    price_min: float = 0.0           # 最低成交价
    price_max: float = 0.0           # 最高成交价

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    elapsed_time: int = 0
    estimated_remaining: int = 0

    fund_account_id: str = ""
    error_msg: str = ""
    child_orders: List[dict] = field(default_factory=list)
    logs: List[dict] = field(default_factory=list)   # 细粒度执行日志


class SimAlgoExecutor:
    """
    仿真算法执行器客户端版。

    使用 sim_executor.submit_order_blocking() 等待每笔子订单完成（阻塞 HTTP），
    完成后直接向网关上报 order_callback / trade_callback。
    """

    def __init__(self, sim_executor, backend_url: str = "http://localhost:19600"):
        self.sim = sim_executor            # SimExecutor 实例
        self.backend_url = backend_url.rstrip("/")
        self.tasks: Dict[str, AlgoTask] = {}
        self._lock = threading.RLock()

        # 网关回调（由 WSGatewayClient 注入）
        self.on_order_callback: Optional[Callable] = None
        self.on_trade_callback: Optional[Callable] = None
        self.on_progress_callback: Optional[Callable] = None
        self.on_complete_callback: Optional[Callable] = None

    # ── 公开接口 ──────────────────────────────────────────────────

    def start_algo(self, data: dict) -> dict:
        algo_id = data.get("algo_id") or str(uuid.uuid4())
        algo_type = data.get("algo_type", "")
        symbol = data.get("symbol", "")
        quantity = int(data.get("quantity", 0))

        if not symbol:
            return {"success": False, "message": "股票代码不能为空"}
        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}

        task = AlgoTask(
            algo_id=algo_id,
            algo_type=algo_type,
            symbol=symbol,
            symbol_name=data.get("symbol_name", ""),
            direction=data.get("direction", "buy"),
            total_quantity=quantity,
            params=data.get("params", {}),
            fund_account_id=data.get("fund_account_id", ""),
        )

        dispatch = {
            AlgoType.MARKET.value: self._execute_market,
            AlgoType.LIMIT.value: self._execute_limit,
            AlgoType.TWAP.value: self._execute_twap,
            AlgoType.POV.value: self._execute_pov,
            AlgoType.ICEBERG.value: self._execute_iceberg,
            AlgoType.VWAP.value: self._execute_vwap,
            AlgoType.LIMIT_UP.value: self._execute_limit_up,
            AlgoType.FAST_TRADE.value: self._execute_fast_trade,
            AlgoType.STEALTH.value: self._execute_stealth,
        }
        fn = dispatch.get(algo_type)
        if not fn:
            return {"success": False, "message": f"不支持的算法类型: {algo_type}"}

        with self._lock:
            self.tasks[algo_id] = task

        threading.Thread(target=fn, args=(task,), daemon=True, name=f"algo-{algo_id[:8]}").start()
        logger.info(f"[SimAlgo] 任务启动: {algo_id} type={algo_type} qty={quantity}")
        return {"success": True, "message": "算法任务已启动", "algo_id": algo_id}

    def pause_algo(self, algo_id: str) -> dict:
        with self._lock:
            task = self.tasks.get(algo_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task.status != AlgoStatus.RUNNING.value:
            return {"success": False, "message": f"任务状态不是运行中: {task.status}"}
        task.status = AlgoStatus.PAUSED.value
        self._report_progress(task, "任务已暂停")
        return {"success": True, "message": "已暂停"}

    def resume_algo(self, algo_id: str) -> dict:
        with self._lock:
            task = self.tasks.get(algo_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task.status != AlgoStatus.PAUSED.value:
            return {"success": False, "message": f"任务状态不是暂停: {task.status}"}
        task.status = AlgoStatus.RUNNING.value
        self._report_progress(task, "任务已恢复")
        return {"success": True, "message": "已恢复"}

    def cancel_algo(self, algo_id: str) -> dict:
        with self._lock:
            task = self.tasks.get(algo_id)
        if not task:
            return {"success": False, "message": "任务不存在"}
        if task.status in (AlgoStatus.COMPLETED.value, AlgoStatus.CANCELLED.value):
            return {"success": False, "message": f"任务已结束: {task.status}"}
        task.status = AlgoStatus.CANCELLED.value
        task.end_time = datetime.now()
        self._report_progress(task, "任务已取消")
        return {"success": True, "message": "已取消"}

    def get_task_status(self, algo_id: str) -> Optional[dict]:
        with self._lock:
            task = self.tasks.get(algo_id)
        return self._task_to_dict(task) if task else None

    def list_tasks(self, status: Optional[str] = None) -> List[dict]:
        with self._lock:
            tasks = list(self.tasks.values())
        result = []
        for t in tasks:
            if status is None or t.status == status:
                result.append(self._task_to_dict(t))
        return result

    # ── 内部工具 ──────────────────────────────────────────────────

    def _task_to_dict(self, task: AlgoTask) -> dict:
        return {
            "algo_id": task.algo_id,
            "algo_type": task.algo_type,
            "symbol": task.symbol,
            "symbol_name": task.symbol_name,
            "direction": task.direction,
            "status": task.status,
            "progress": round(task.progress, 2),
            "total_quantity": task.total_quantity,
            "filled_quantity": task.filled_quantity,
            "avg_price": task.avg_price,
            "total_amount": task.total_amount,
            "fund_account_id": task.fund_account_id,
            "start_time": task.start_time.isoformat() if task.start_time else None,
            "end_time": task.end_time.isoformat() if task.end_time else None,
            "elapsed_time": task.elapsed_time,
            "estimated_remaining": task.estimated_remaining,
            "error_msg": task.error_msg,
            "total_commission": round(task.total_commission, 4),
            "price_min": round(task.price_min, 3) if task.price_min > 0 else None,
            "price_max": round(task.price_max, 3) if task.price_max > 0 else None,
            "child_orders": task.child_orders,
            "logs": task.logs,
        }

    def _add_log(self, task: AlgoTask, msg: str, level: str = "info"):
        """向任务日志追加一条带时间戳的记录。"""
        entry = {"time": datetime.now().isoformat(), "level": level, "msg": msg}
        with self._lock:
            task.logs.append(entry)

    def _report_progress(self, task: AlgoTask, message: str):
        if task.start_time:
            task.elapsed_time = int((datetime.now() - task.start_time).total_seconds())
        if self.on_progress_callback:
            try:
                self.on_progress_callback(self._task_to_dict(task) | {"message": message})
            except Exception as e:
                logger.error(f"[SimAlgo] 进度回调异常: {e}")

    def _complete_task(self, task: AlgoTask, success: bool, message: str):
        with self._lock:
            task.status = AlgoStatus.COMPLETED.value if success else AlgoStatus.FAILED.value
            task.end_time = datetime.now()
            if not success:
                task.error_msg = message
        self._report_progress(task, message)

        # 上报父订单的终态到后端（算法专属状态）
        if task.status == AlgoStatus.CANCELLED.value:
            final_status = "algo_cancelled"
        elif not success:
            final_status = "algo_failed"
        elif task.filled_quantity >= task.total_quantity:
            final_status = "algo_done"
        elif task.filled_quantity > 0:
            final_status = "algo_partial"
        else:
            final_status = "algo_failed"
        self._fire_order(
            order_id=task.algo_id,
            symbol=task.symbol,
            status=final_status,
            status_msg=message,
            filled_qty=task.filled_quantity,
            avg_price=task.avg_price,
        )

        if self.on_complete_callback:
            try:
                self.on_complete_callback(task.algo_id, success, message)
            except Exception as e:
                logger.error(f"[SimAlgo] 完成回调异常: {e}")

    def _submit_and_wait(self, task: AlgoTask, qty: int, price: float,
                         price_type: str, timeout: float,
                         is_cleanup: bool = False) -> bool:
        """提交子订单（阻塞），完成后更新任务进度并向网关上报回调。
        超时后仿真器会自动撤单，返回实际成交量。
        is_cleanup=True 表示这是主循环结束后的收尾补单。
        """
        with self._lock:
            slice_index = len(task.child_orders) + 1
            order_id = f"{task.algo_id}_{slice_index}"

        order_data = {
            "order_id": order_id,
            "symbol": task.symbol,
            "symbol_name": task.symbol_name,
            "direction": task.direction,
            "price_type": price_type,
            "price": price,
            "quantity": qty,
            "fund_account_id": task.fund_account_id,
        }

        prefix = "[收尾]" if is_cleanup else f"[切片#{slice_index}]"
        price_desc = f"{price:.3f}" if price > 0 else "市价"
        self._add_log(task, f"{prefix} 提交委托 qty={qty} price={price_desc} TTL={timeout:.0f}s")
        logger.info(f"[SimAlgo] 子订单: {order_id} qty={qty} price={price} "
                    f"timeout={timeout}s{'  [收尾]' if is_cleanup else ''}")
        submit_time = datetime.now()
        result = self.sim.submit_order_blocking(order_data, timeout=timeout)
        elapsed_ms = int((datetime.now() - submit_time).total_seconds() * 1000)

        filled_qty = result.get("filled_qty", 0)
        filled_amount = result.get("filled_amount", 0.0)
        avg_p = filled_amount / filled_qty if filled_qty > 0 else 0.0
        timed_out = result.get("timed_out", False)

        # 累计手续费（从 trades 中汇总）
        slice_commission = sum(t.get("commission", 0) for t in result.get("trades", []))

        # 判断子单状态
        order_status = result.get("status", "")
        is_rejected = order_status == "rejected" or (not result.get("success") and not filled_qty)
        err_msg = ""
        if is_rejected:
            slice_status = "rejected"
            err_msg = result.get("reject_reason") or result.get("status_msg") or "订单被拒绝"
        elif filled_qty >= qty:
            slice_status = "filled"
        elif timed_out:
            # 超时：submit_order_blocking 内部已自动撤单
            slice_status = "partial_cancelled" if filled_qty > 0 else "cancelled"
        else:
            # 非超时的部分成交/零成交：委托单可能仍在市场挂单，必须主动撤销
            # 防止收尾阶段补单时与旧挂单重叠导致超买
            ext_id = result.get("external_order_id", order_id)
            logger.info(f"[SimAlgo] 子单 {order_id} 非超时部成({filled_qty}/{qty})，主动撤销剩余挂单")
            cancel_result = self.sim.cancel_order_sync({"external_order_id": ext_id})
            logger.info(f"[SimAlgo] 撤单结果: {cancel_result}")
            slice_status = "partial_cancelled" if filled_qty > 0 else "cancelled"

        # 记录切片结果到细粒度日志
        if slice_status == "filled":
            self._add_log(task, f"{prefix} 全部成交 {filled_qty}股 @{avg_p:.3f} 耗时{elapsed_ms/1000:.1f}s 手续费{slice_commission:.2f}")
        elif slice_status == "partial_cancelled":
            self._add_log(task, f"{prefix} 部分成交 {filled_qty}/{qty}股 @{avg_p:.3f}，余量已撤销 耗时{elapsed_ms/1000:.1f}s", "warn")
        elif slice_status == "cancelled":
            self._add_log(task, f"{prefix} 未成交，已撤单", "warn")
        elif slice_status == "rejected":
            self._add_log(task, f"{prefix} 委托被拒绝: {err_msg}", "error")

        # 记录子单执行详情（供前端执行日志查看）
        with self._lock:
            task.child_orders.append({
                "index": slice_index,
                "order_id": order_id,
                "quantity": qty,
                "unfilled": qty - filled_qty,
                "price": round(price, 3),
                "price_type": price_type,
                "filled_qty": filled_qty,
                "avg_price": round(avg_p, 3),
                "commission": round(slice_commission, 4),
                "status": slice_status,
                "is_cleanup": is_cleanup,
                "submit_time": submit_time.isoformat(),
                "elapsed_ms": elapsed_ms,
                "message": err_msg if is_rejected else (
                    f"部分成交 {filled_qty}/{qty}，已撤余量" if slice_status == "partial_cancelled" else (
                    f"已撤单，未成交" if slice_status == "cancelled" else ""
                )),
            })

        if is_rejected:
            logger.warning(f"[SimAlgo] 子订单被拒绝，终止算法: {err_msg}")
            raise FatalAlgoError(err_msg)

        # 更新任务聚合进度
        if filled_qty > 0:
            with self._lock:
                task.filled_quantity += filled_qty
                task.total_amount += filled_amount
                task.total_commission += slice_commission
                task.avg_price = task.total_amount / task.filled_quantity if task.filled_quantity > 0 else 0.0
                task.progress = task.filled_quantity / task.total_quantity * 100
                # 更新价格区间
                if task.price_min == 0 or avg_p < task.price_min:
                    task.price_min = avg_p
                if avg_p > task.price_max:
                    task.price_max = avg_p
                if task.start_time:
                    task.elapsed_time = int((datetime.now() - task.start_time).total_seconds())
            self._report_progress(task, f"成交 {filled_qty} 股 @{avg_p:.3f}")

            # ── 向后端上报父订单的聚合状态（算法专属状态，区别于普通订单） ──────
            self._fire_order(
                order_id=task.algo_id,
                symbol=task.symbol,
                status="algo_running",
                status_msg=f"算法累计成交 {task.filled_quantity}/{task.total_quantity}",
                filled_qty=task.filled_quantity,
                avg_price=task.avg_price,
            )
            # 同时上报成交明细，归属到父订单方便后端成交记录关联
            for trade in result.get("trades", []):
                self._fire_trade(
                    trade_id=trade.get("trade_id", str(uuid.uuid4())),
                    order_id=task.algo_id,
                    symbol=task.symbol,
                    direction=task.direction,
                    price=trade.get("price", avg_p),
                    quantity=trade.get("quantity", filled_qty),
                    amount=trade.get("amount", filled_amount),
                    trade_time=trade.get("trade_time", int(time.time() * 1000)),
                )

        return filled_qty >= qty

    def _cleanup_remaining(self, task: AlgoTask, max_attempts: int = 3, ttl: float = 30.0):
        """主循环结束后，对未成交的剩余数量进行收尾补单。
        使用更大的滑点以提高成交概率，最多尝试 max_attempts 次。
        """
        attempt = 0
        while task.filled_quantity < task.total_quantity and attempt < max_attempts:
            if task.status != AlgoStatus.RUNNING.value:
                break
            remaining = task.total_quantity - task.filled_quantity
            if remaining <= 0:
                break
            attempt += 1
            self._add_log(task, f"收尾补单 #{attempt}/{max_attempts}: 获取行情…")
            price = self._get_slippage_price(task.symbol, task.direction, 0.02, task)  # 2% 滑点
            price_type = "limit" if price > 0 else "market"
            logger.info(f"[SimAlgo] 收尾补单 #{attempt}: {task.algo_id} 剩余={remaining}")
            self._report_progress(task, f"收尾补单 #{attempt}: 剩余 {remaining} 股")
            self._submit_and_wait(task, remaining, price, price_type, ttl, is_cleanup=True)

    def _fire_order(self, order_id, symbol, status, status_msg, filled_qty, avg_price):
        if self.on_order_callback:
            try:
                self.on_order_callback({
                    "order_id": order_id,
                    "external_order_id": order_id,
                    "symbol": symbol,
                    "status": status,
                    "status_msg": status_msg,
                    "filled_quantity": filled_qty,
                    "avg_filled_price": avg_price,
                    "order_time": datetime.now().isoformat(),
                })
            except Exception as e:
                logger.error(f"[SimAlgo] order_callback 异常: {e}")

    def _fire_trade(self, trade_id, order_id, symbol, direction, price, quantity, amount, trade_time):
        if self.on_trade_callback:
            try:
                self.on_trade_callback({
                    "trade_id": trade_id,
                    "order_id": order_id,
                    "external_order_id": order_id,
                    "symbol": symbol,
                    "direction": direction,
                    "price": price,
                    "quantity": quantity,
                    "amount": amount,
                    "trade_time": trade_time,
                })
            except Exception as e:
                logger.error(f"[SimAlgo] trade_callback 异常: {e}")

    def _wait_resume(self, task: AlgoTask) -> bool:
        while task.status == AlgoStatus.PAUSED.value:
            time.sleep(0.5)
            if task.status == AlgoStatus.CANCELLED.value:
                return False
        return task.status != AlgoStatus.CANCELLED.value

    def _get_market_price(self, symbol: str, account_id: str = "") -> float:
        response = self.sim._session.get(
            f"{self.sim.server_url}/api/market-data/price/{symbol}",
            params={"account_id": account_id}, timeout=3)
        response.raise_for_status()
        local = response.json()
        if local.get("execution_mode") == "quote":
            return float(local["price"])
        try:
            url = f"{self.backend_url}/api/v1/realtime-quote/single/{symbol}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = json.loads(resp.read().decode())
            price = float(body.get("data", {}).get("price", 0))
            return price if price > 0 else 0.0
        except Exception:
            return 0.0

    def _get_slippage_price(self, symbol: str, direction: str, slippage: float = 0.005,
                            task: "AlgoTask | None" = None) -> float:
        price = self._get_market_price(symbol, task.fund_account_id if task else "")
        if price <= 0:
            if task:
                self._add_log(task, f"获取 {symbol} 行情失败，将使用市价委托", "warn")
            return 0.0
        result = round(price * (1 + slippage if direction == "buy" else 1 - slippage), 2)
        if task:
            self._add_log(task, f"行情 {symbol} 现价={price:.3f}  {'买入' if direction == 'buy' else '卖出'}报价={result:.3f} (滑点{slippage*100:.1f}%)")
        return result

    # ── 算法实现 ──────────────────────────────────────────────────

    def _execute_market(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行市价算法")

            price = self._get_slippage_price(task.symbol, task.direction, 0.005, task)
            price_type = "limit" if price > 0 else "market"
            filled = self._submit_and_wait(task, task.total_quantity, price, price_type, 15)

            if task.status == AlgoStatus.CANCELLED.value:
                return
            self._complete_task(task, filled, "市价算法执行完成" if filled else
                                f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] 市价异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_limit(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行限价算法")

            limit_price = task.params.get("price", 0.0)
            if limit_price <= 0:
                self._complete_task(task, False, "限价必须大于0")
                return

            timeout = task.params.get("timeout", 300)
            filled = self._submit_and_wait(task, task.total_quantity, limit_price, "limit", timeout)

            if task.status == AlgoStatus.CANCELLED.value:
                return
            self._complete_task(task, filled, "限价算法执行完成" if filled else
                                f"超时/部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] 限价异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_twap(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行TWAP算法")

            duration = task.params.get("duration", 600)
            slice_count = task.params.get("slice_count", 10)
            price_limit = task.params.get("price_limit", 0.0)

            slice_qty = task.total_quantity // slice_count
            slice_interval = duration / slice_count

            self._add_log(task, f"收到TWAP指令: {task.symbol} {task.direction} "
                                 f"总量={task.total_quantity} 时长={duration}s "
                                 f"切片数={slice_count} 每片={slice_qty}股 间隔={slice_interval:.1f}s"
                                 + (f" 价格上限={price_limit}" if price_limit > 0 else ""))

            for i in range(slice_count):
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return

                current_qty = (task.total_quantity - task.filled_quantity) if i == slice_count - 1 else slice_qty
                if current_qty <= 0:
                    self._add_log(task, f"切片 {i+1}/{slice_count}: 剩余数量为0，跳过")
                    break

                self._add_log(task, f"切片 {i+1}/{slice_count}: 获取 {task.symbol} 行情…")
                price = self._get_slippage_price(task.symbol, task.direction, 0.005, task)
                if price_limit > 0:
                    if task.direction == "buy" and price > price_limit:
                        self._add_log(task, f"切片 {i+1}/{slice_count}: 报价{price:.3f} 超过上限{price_limit}，跳过本轮，等待{slice_interval:.1f}s", "warn")
                        time.sleep(slice_interval)
                        continue
                    if task.direction == "sell" and price < price_limit:
                        self._add_log(task, f"切片 {i+1}/{slice_count}: 报价{price:.3f} 低于下限{price_limit}，跳过本轮，等待{slice_interval:.1f}s", "warn")
                        time.sleep(slice_interval)
                        continue

                price_type = "limit" if price > 0 else "market"
                task.estimated_remaining = int((slice_count - i - 1) * slice_interval)
                self._report_progress(task, f"TWAP切片 {i+1}/{slice_count}, 数量={current_qty}")

                slice_start = time.time()
                self._submit_and_wait(task, current_qty, price, price_type, slice_interval * 1.5)

                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return

                if i < slice_count - 1:
                    remaining_wait = slice_interval - (time.time() - slice_start)
                    if remaining_wait > 0:
                        self._add_log(task, f"切片 {i+1}/{slice_count}: 等待 {remaining_wait:.1f}s 后执行下一切片")
                        time.sleep(remaining_wait)

            # 主循环结束后收尾：补买/补卖未成交的剩余数量
            if task.filled_quantity < task.total_quantity and task.status == AlgoStatus.RUNNING.value:
                self._add_log(task, f"主循环结束，剩余 {task.total_quantity - task.filled_quantity} 股未成，进入收尾补单阶段", "warn")
                self._cleanup_remaining(task, max_attempts=3, ttl=30.0)

            if task.filled_quantity >= task.total_quantity:
                self._add_log(task, f"TWAP执行完成，累计成交 {task.filled_quantity} 股 均价={task.avg_price:.3f}")
                self._complete_task(task, True, "TWAP算法执行完成")
            else:
                self._add_log(task, f"TWAP结束，部分成交 {task.filled_quantity}/{task.total_quantity} 股", "warn")
                self._complete_task(task, task.filled_quantity > 0,
                                    f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] TWAP异常: {e}", exc_info=True)
            self._add_log(task, f"执行异常: {e}", "error")
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_iceberg(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行冰山算法")

            display_qty = task.params.get("display_quantity", 100)
            interval = task.params.get("interval", 5)
            use_limit = task.params.get("price_type", "") == "limit"
            fixed_price = task.params.get("price", 0.0)

            remaining = task.total_quantity
            slice_num = 0

            while remaining > 0:
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return

                current_qty = min(display_qty, remaining)
                slice_num += 1

                if use_limit and fixed_price > 0:
                    price, price_type = fixed_price, "limit"
                else:
                    price = self._get_slippage_price(task.symbol, task.direction, 0.005, task)
                    price_type = "limit" if price > 0 else "market"

                task.estimated_remaining = int((remaining / display_qty) * interval)
                self._report_progress(task, f"冰山切片 {slice_num}, 数量={current_qty}")
                self._submit_and_wait(task, current_qty, price, price_type, 30)

                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return

                remaining = task.total_quantity - task.filled_quantity
                if remaining > 0 and interval > 0:
                    time.sleep(interval)

            self._complete_task(task, True, f"冰山算法完成，共{slice_num}片")
        except Exception as e:
            logger.error(f"[SimAlgo] 冰山异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_vwap(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行VWAP算法")

            duration = task.params.get("duration", 600)
            time_slices = task.params.get("time_slices", 10)
            volume_dist = task.params.get("volume_distribution", [
                0.15, 0.12, 0.10, 0.08, 0.08,
                0.08, 0.08, 0.08, 0.10, 0.13
            ])
            total_ratio = sum(volume_dist)
            volume_dist = [v / total_ratio for v in volume_dist]
            slice_interval = duration / time_slices

            for i in range(time_slices):
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return

                ratio = volume_dist[i] if i < len(volume_dist) else (1.0 / time_slices)
                slice_qty = int(task.total_quantity * ratio)
                if i == time_slices - 1:
                    slice_qty = task.total_quantity - task.filled_quantity
                if slice_qty <= 0:
                    continue

                price = self._get_slippage_price(task.symbol, task.direction, 0.005, task)
                price_type = "limit" if price > 0 else "market"
                task.estimated_remaining = int((time_slices - i - 1) * slice_interval)

                slice_start = time.time()
                self._report_progress(task, f"VWAP切片 {i+1}/{time_slices}, 数量={slice_qty}")
                self._submit_and_wait(task, slice_qty, price, price_type, slice_interval * 1.5)

                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return

                if i < time_slices - 1:
                    remaining_wait = slice_interval - (time.time() - slice_start)
                    if remaining_wait > 0:
                        time.sleep(remaining_wait)

            if task.filled_quantity < task.total_quantity and task.status == AlgoStatus.RUNNING.value:
                self._cleanup_remaining(task, max_attempts=3, ttl=30.0)

            if task.filled_quantity >= task.total_quantity:
                self._complete_task(task, True, "VWAP算法执行完成")
            else:
                self._complete_task(task, task.filled_quantity > 0,
                                    f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] VWAP异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_pov(self, task: AlgoTask):
        try:
            import random
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行POV算法")

            target_ratio = task.params.get("target_ratio", 0.2)
            time_limit = task.params.get("time_limit", 600)
            check_interval = task.params.get("check_interval", 10)
            price_limit = task.params.get("price_limit", 0.0)

            start_time = time.time()

            while task.filled_quantity < task.total_quantity:
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return
                if time.time() - start_time > time_limit:
                    self._complete_task(task, False, f"超时，成交: {task.filled_quantity}/{task.total_quantity}")
                    return

                simulated_market_vol = random.randint(500, 2000) * 100
                target_qty = int(simulated_market_vol * target_ratio)
                remaining = task.total_quantity - task.filled_quantity
                target_qty = min(target_qty, remaining)

                if target_qty > 0:
                    price = self._get_slippage_price(task.symbol, task.direction, 0.003, task)
                    if price_limit > 0:
                        if task.direction == "buy" and price > price_limit:
                            time.sleep(check_interval)
                            continue
                        if task.direction == "sell" and price < price_limit:
                            time.sleep(check_interval)
                            continue
                    price_type = "limit" if price > 0 else "market"
                    self._report_progress(task, f"POV跟随市场 {target_qty} 股")
                    self._submit_and_wait(task, target_qty, price, price_type, check_interval * 2)

                time.sleep(check_interval)

            self._complete_task(task, True, "POV算法执行完成")
        except Exception as e:
            logger.error(f"[SimAlgo] POV异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_limit_up(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行涨停板抢单算法")

            limit_up_price = task.params.get("price", 0.0)
            if limit_up_price <= 0:
                price = self._get_market_price(task.symbol, task.fund_account_id)
                limit_up_price = round(price * 1.10, 2) if price > 0 else 0.0

            if limit_up_price <= 0:
                self._complete_task(task, False, "无法获取涨停价格")
                return

            timeout = task.params.get("timeout", 30)
            self._report_progress(task, f"涨停板抢单 价格={limit_up_price} 数量={task.total_quantity}")
            filled = self._submit_and_wait(task, task.total_quantity, limit_up_price, "limit", timeout)

            if task.status == AlgoStatus.CANCELLED.value:
                return
            self._complete_task(task, filled, "涨停抢单完成" if filled else
                                f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] 涨停抢单异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_fast_trade(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行快速成交算法")

            max_attempts = task.params.get("max_attempts", 5)
            slippage = task.params.get("slippage", 0.01)
            attempt_interval = task.params.get("attempt_interval", 2)

            for attempt in range(max_attempts):
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return

                remaining = task.total_quantity - task.filled_quantity
                if remaining <= 0:
                    break

                price = self._get_slippage_price(task.symbol, task.direction, slippage, task)
                price_type = "limit" if price > 0 else "market"
                self._report_progress(task, f"快速成交 第{attempt+1}次, 剩余={remaining}")
                self._submit_and_wait(task, remaining, price, price_type, 10)

                if task.filled_quantity >= task.total_quantity:
                    break
                if attempt < max_attempts - 1:
                    time.sleep(attempt_interval)

            if task.filled_quantity >= task.total_quantity:
                self._complete_task(task, True, "快速成交完成")
            else:
                self._complete_task(task, False, f"达到最大尝试次数，成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] 快速成交异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_stealth(self, task: AlgoTask):
        try:
            import random
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行智能隐蔽算法")

            duration = task.params.get("duration", 1200)
            min_slice = task.params.get("min_slice_qty", 100)
            max_slice = task.params.get("max_slice_qty", 500)
            price_limit = task.params.get("price_limit", 0.0)

            start_time = time.time()
            slice_num = 0

            while task.filled_quantity < task.total_quantity:
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return
                if time.time() - start_time > duration:
                    break

                remaining = task.total_quantity - task.filled_quantity
                slice_qty = min(random.randint(min_slice, max_slice), remaining)
                slice_qty = max(100, slice_qty)

                price = self._get_slippage_price(task.symbol, task.direction,
                                                  random.uniform(0.001, 0.005), task)
                if price_limit > 0:
                    if task.direction == "buy" and price > price_limit:
                        time.sleep(random.uniform(3, 8))
                        continue
                    if task.direction == "sell" and price < price_limit:
                        time.sleep(random.uniform(3, 8))
                        continue

                price_type = "limit" if price > 0 else "market"
                slice_num += 1
                task.estimated_remaining = int(duration - (time.time() - start_time))
                self._report_progress(task, f"隐蔽切片 {slice_num}, 数量={slice_qty}")
                self._submit_and_wait(task, slice_qty, price, price_type, 20)

                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return

                time.sleep(random.uniform(5, 30))

            if task.filled_quantity >= task.total_quantity:
                self._complete_task(task, True, f"隐蔽算法完成，共{slice_num}片")
            else:
                self._complete_task(task, False, f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[SimAlgo] 隐蔽异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

"""
仿真算法交易引擎

实现与 gj_executor-qmt 相同的算法类型，底层使用 OrderEngine 的阻塞提交。
"""
import logging
import time
import threading
import uuid
import urllib.request
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

from .order_engine import OrderEngine
from .account_manager import AccountManager

logger = logging.getLogger(__name__)


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

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    elapsed_time: int = 0
    estimated_remaining: int = 0

    fund_account_id: str = ""
    error_msg: str = ""
    child_orders: List[str] = field(default_factory=list)


class SimAlgoEngine:
    """仿真算法引擎：使用 OrderEngine.submit_order_blocking 实现各种算法。"""

    def __init__(self, order_engine: OrderEngine, account_manager: AccountManager,
                 backend_url: str = "http://localhost:19600", max_workers: int = 20):
        self._order_engine = order_engine
        self._account_manager = account_manager
        self.backend_url = backend_url.rstrip("/")
        self.tasks: Dict[str, AlgoTask] = {}
        self._lock = threading.RLock()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="algo")

        # 可选回调
        self.on_progress_callback: Optional[Callable] = None
        self.on_complete_callback: Optional[Callable] = None

    # ── 公开接口 ──────────────────────────────────────────────────

    def start_algo(self, data: dict) -> dict:
        """启动算法任务。data 为 gateway start_algo 命令的 data 字段。"""
        algo_id = data.get("algo_id") or str(uuid.uuid4())
        algo_type = data.get("algo_type", "")
        symbol = data.get("symbol", "")
        symbol_name = data.get("symbol_name", "")
        direction = data.get("direction", "buy")
        quantity = int(data.get("quantity", 0))
        fund_account_id = data.get("fund_account_id", "")
        params = data.get("params", {})

        if not symbol:
            return {"success": False, "message": "股票代码不能为空"}
        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}

        task = AlgoTask(
            algo_id=algo_id,
            algo_type=algo_type,
            symbol=symbol,
            symbol_name=symbol_name,
            direction=direction,
            total_quantity=quantity,
            params=params,
            fund_account_id=fund_account_id,
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

        self._pool.submit(fn, task)
        logger.info(f"[Algo] 任务已启动: {algo_id} type={algo_type} qty={quantity}")
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
        if not task:
            return None
        return self._task_to_dict(task)

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
            "child_orders": task.child_orders,
        }

    def _report_progress(self, task: AlgoTask, message: str):
        if task.start_time:
            task.elapsed_time = int((datetime.now() - task.start_time).total_seconds())
        if self.on_progress_callback:
            try:
                self.on_progress_callback({
                    "algo_id": task.algo_id,
                    "status": task.status,
                    "progress": task.progress,
                    "filled_quantity": task.filled_quantity,
                    "avg_price": task.avg_price,
                    "message": message,
                })
            except Exception as e:
                logger.error(f"[Algo] 进度回调异常: {e}")

    def _complete_task(self, task: AlgoTask, success: bool, message: str):
        with self._lock:
            task.status = AlgoStatus.COMPLETED.value if success else AlgoStatus.FAILED.value
            task.end_time = datetime.now()
            if not success:
                task.error_msg = message
        self._report_progress(task, message)
        if self.on_complete_callback:
            try:
                self.on_complete_callback(task.algo_id, success, message)
            except Exception as e:
                logger.error(f"[Algo] 完成回调异常: {e}")

    def _submit_and_wait(self, task: AlgoTask, qty: int, price: float,
                         price_type: str, timeout: float) -> bool:
        """提交子订单并阻塞等待完成（成交/撤单/拒绝）。返回是否完全成交。"""
        with self._lock:
            order_id = f"{task.algo_id}_{len(task.child_orders) + 1}"

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

        logger.info(f"[Algo] 子订单提交: {order_id} qty={qty} price={price} type={price_type}")
        result = self._order_engine.submit_order_blocking(order_data, timeout=timeout)

        if not result.get("success") and not result.get("filled_qty"):
            logger.warning(f"[Algo] 子订单失败: {result.get('message')}")
            return False

        task.child_orders.append(order_id)

        filled_qty = result.get("filled_qty", 0)
        filled_amount = result.get("filled_amount", 0.0)

        if filled_qty > 0:
            with self._lock:
                task.filled_quantity += filled_qty
                task.total_amount += filled_amount
                task.avg_price = task.total_amount / task.filled_quantity if task.filled_quantity > 0 else 0.0
                task.progress = task.filled_quantity / task.total_quantity * 100
                if task.start_time:
                    task.elapsed_time = int((datetime.now() - task.start_time).total_seconds())

            avg_p = filled_amount / filled_qty if filled_qty > 0 else 0.0
            self._report_progress(task, f"成交 {filled_qty} 股 @{avg_p:.2f}")

        return filled_qty >= qty

    def _wait_resume(self, task: AlgoTask) -> bool:
        """暂停时阻塞等待，返回 False 表示已取消。"""
        while task.status == AlgoStatus.PAUSED.value:
            time.sleep(0.5)
            if task.status == AlgoStatus.CANCELLED.value:
                return False
        return task.status != AlgoStatus.CANCELLED.value

    def _get_market_price(self, symbol: str) -> float:
        """从后端获取实时行情价格，失败返回 0.0。"""
        try:
            url = f"{self.backend_url}/api/v1/realtime-quote/single/{symbol}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = json.loads(resp.read().decode())
            price = float(body.get("data", {}).get("price", 0))
            return price if price > 0 else 0.0
        except Exception:
            return 0.0

    def _get_slippage_price(self, symbol: str, direction: str, slippage: float = 0.005) -> float:
        price = self._get_market_price(symbol)
        if price <= 0:
            return 0.0
        return round(price * (1 + slippage if direction == "buy" else 1 - slippage), 2)

    # ── 算法实现 ──────────────────────────────────────────────────

    def _execute_market(self, task: AlgoTask):
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行市价算法")

            price = self._get_slippage_price(task.symbol, task.direction, 0.005)
            price_type = "limit" if price > 0 else "market"

            filled = self._submit_and_wait(task, task.total_quantity, price, price_type, 15)

            if task.status == AlgoStatus.CANCELLED.value:
                return
            self._complete_task(task, filled, "市价算法执行完成" if filled else
                                f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[Algo] 市价异常: {e}", exc_info=True)
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
            logger.error(f"[Algo] 限价异常: {e}", exc_info=True)
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

            logger.info(f"[Algo] TWAP: 时长={duration}s 切片={slice_count} 每片={slice_qty}股")

            for i in range(slice_count):
                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return
                if not self._wait_resume(task):
                    self._complete_task(task, False, "任务已取消")
                    return

                current_qty = (task.total_quantity - task.filled_quantity) if i == slice_count - 1 else slice_qty
                if current_qty <= 0:
                    break

                price = self._get_slippage_price(task.symbol, task.direction, 0.005)
                if price_limit > 0:
                    if task.direction == "buy" and price > price_limit:
                        logger.warning(f"[Algo] TWAP 跳过切片 {i+1}：价格 {price} > 限价 {price_limit}")
                        time.sleep(slice_interval)
                        continue
                    if task.direction == "sell" and price < price_limit:
                        logger.warning(f"[Algo] TWAP 跳过切片 {i+1}：价格 {price} < 限价 {price_limit}")
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
                    elapsed = time.time() - slice_start
                    remaining_wait = slice_interval - elapsed
                    if remaining_wait > 0:
                        time.sleep(remaining_wait)

            if task.filled_quantity >= task.total_quantity:
                self._complete_task(task, True, "TWAP算法执行完成")
            else:
                self._complete_task(task, False, f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[Algo] TWAP异常: {e}", exc_info=True)
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
                    price = self._get_slippage_price(task.symbol, task.direction, 0.005)
                    price_type = "limit" if price > 0 else "market"

                task.estimated_remaining = int((remaining / display_qty) * interval)
                self._report_progress(task, f"冰山切片 {slice_num}, 数量={current_qty}, 价格={price}")

                self._submit_and_wait(task, current_qty, price, price_type, 30)

                if task.status == AlgoStatus.CANCELLED.value:
                    self._complete_task(task, False, "任务已取消")
                    return

                remaining = task.total_quantity - task.filled_quantity
                if remaining > 0 and interval > 0:
                    time.sleep(interval)

            self._complete_task(task, True, f"冰山算法完成，共{slice_num}片")
        except Exception as e:
            logger.error(f"[Algo] 冰山异常: {e}", exc_info=True)
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

                price = self._get_slippage_price(task.symbol, task.direction, 0.005)
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

            if task.filled_quantity >= task.total_quantity:
                self._complete_task(task, True, "VWAP算法执行完成")
            else:
                self._complete_task(task, False, f"部分成交: {task.filled_quantity}/{task.total_quantity}")
        except Exception as e:
            logger.error(f"[Algo] VWAP异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_pov(self, task: AlgoTask):
        """POV：按参与率循环提交，直到完成或超时。仿真模式用随机间隔模拟市场量。"""
        try:
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

                # 仿真模式：模拟一批市场成交量
                import random
                simulated_market_vol = random.randint(500, 2000) * 100
                target_qty = int(simulated_market_vol * target_ratio)
                remaining = task.total_quantity - task.filled_quantity
                target_qty = min(target_qty, remaining)

                if target_qty > 0:
                    price = self._get_slippage_price(task.symbol, task.direction, 0.003)
                    if price_limit > 0:
                        if task.direction == "buy" and price > price_limit:
                            time.sleep(check_interval)
                            continue
                        if task.direction == "sell" and price < price_limit:
                            time.sleep(check_interval)
                            continue

                    price_type = "limit" if price > 0 else "market"
                    elapsed = time.time() - start_time
                    remaining_qty = task.total_quantity - task.filled_quantity
                    task.estimated_remaining = int(remaining_qty / (task.filled_quantity / elapsed)) if task.filled_quantity > 0 else 0
                    self._report_progress(task, f"POV跟随市场 {target_qty} 股")
                    self._submit_and_wait(task, target_qty, price, price_type, check_interval * 2)

                time.sleep(check_interval)

            self._complete_task(task, True, "POV算法执行完成")
        except Exception as e:
            logger.error(f"[Algo] POV异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_limit_up(self, task: AlgoTask):
        """涨停板抢单：立即以涨停价提交全部数量。"""
        try:
            task.status = AlgoStatus.RUNNING.value
            task.start_time = datetime.now()
            self._report_progress(task, "开始执行涨停板抢单算法")

            limit_up_price = task.params.get("price", 0.0)
            if limit_up_price <= 0:
                price = self._get_market_price(task.symbol)
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
            logger.error(f"[Algo] 涨停抢单异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_fast_trade(self, task: AlgoTask):
        """快速成交：多次尝试直到全部成交或超时。"""
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

                price = self._get_slippage_price(task.symbol, task.direction, slippage)
                price_type = "limit" if price > 0 else "market"

                self._report_progress(task, f"快速成交 第{attempt+1}次尝试, 剩余={remaining}")
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
            logger.error(f"[Algo] 快速成交异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

    def _execute_stealth(self, task: AlgoTask):
        """智能隐蔽算法：随机化切片大小和间隔，避免明显规律。"""
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
                                                  random.uniform(0.001, 0.005))
                if price_limit > 0:
                    if task.direction == "buy" and price > price_limit:
                        time.sleep(random.uniform(3, 8))
                        continue
                    if task.direction == "sell" and price < price_limit:
                        time.sleep(random.uniform(3, 8))
                        continue

                price_type = "limit" if price > 0 else "market"
                slice_num += 1
                elapsed = time.time() - start_time
                task.estimated_remaining = int(duration - elapsed)

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
            logger.error(f"[Algo] 隐蔽算法异常: {e}", exc_info=True)
            self._complete_task(task, False, f"执行异常: {e}")

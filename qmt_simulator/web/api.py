"""FastAPI HTTP 管理接口"""
import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import FileResponse
from pydantic import BaseModel

from qmt_simulator.simulator.scenario import scenario as _scenario, commission as _commission, get_default_config_dict
from qmt_simulator.simulator.account_manager import AccountManager
from qmt_simulator.simulator.order_engine import OrderEngine

logger = logging.getLogger(__name__)

app = FastAPI(title="XM 仿真执行器管理", version="2.0.0")

# 这些依赖在 main.py / server_main.py 启动时注入
_account_manager: Optional[AccountManager] = None
_order_engine: Optional[OrderEngine] = None
_algo_engine = None  # Optional[SimAlgoEngine]
_gateway_connected = False
_executor_id = "sim-executor"
_scenario_path: Optional[Path] = None   # 场景配置持久化路径


def _load_scenario(path: Path):
    """从 JSON 文件加载场景配置（含手续费）。"""
    try:
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            if "filled_prob" in d:
                _scenario.update_probs(
                    d.get("filled_prob", 0.6),
                    d.get("partial_filled_prob", 0.2),
                    d.get("rejected_prob", 0.1),
                    d.get("canceled_prob", 0.1),
                )
            _scenario.update_delay(
                d.get("initial_delay"),
                d.get("batch_interval_min"),
                d.get("batch_interval_max"),
                d.get("batch_count_min"),
                d.get("batch_count_max"),
            )
            # 加载手续费配置
            if "commission" in d:
                c = d["commission"]
                _commission.update(
                    enabled=c.get("enabled"),
                    commission_rate=c.get("commission_rate"),
                    min_commission=c.get("min_commission"),
                    stamp_duty_rate=c.get("stamp_duty_rate"),
                    transfer_fee_rate=c.get("transfer_fee_rate"),
                )
    except Exception as e:
        logger.warning(f"加载场景配置失败: {e}")


def _save_scenario():
    """将当前场景配置（含手续费）持久化到 JSON 文件。"""
    if _scenario_path is None:
        return
    try:
        _scenario_path.parent.mkdir(parents=True, exist_ok=True)
        data = _scenario.to_dict()
        data["commission"] = _commission.to_dict()
        _scenario_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"保存场景配置失败: {e}")

# 回调事件队列（供客户端轮询）
_callbacks: deque = deque(maxlen=2000)
_callback_counter = 0
_callback_lock = threading.Lock()


def _store_callback(cb_type: str, data: dict):
    global _callback_counter
    with _callback_lock:
        _callback_counter += 1
        _callbacks.append({"id": _callback_counter, "type": cb_type, "data": data, "ts": time.time()})


def init_api(account_manager: AccountManager, order_engine: OrderEngine,
             executor_id: str, algo_engine=None, scenario_path: Optional[Path] = None):
    global _account_manager, _order_engine, _executor_id, _algo_engine, _scenario_path
    _account_manager = account_manager
    _order_engine = order_engine
    _executor_id = executor_id
    _algo_engine = algo_engine
    if scenario_path is not None:
        _scenario_path = scenario_path
        _load_scenario(scenario_path)
    # 将全局 commission 单例注入 order_engine（持久化加载后生效）
    order_engine.commission = _commission

    # 挂载 order/trade 回调到事件队列
    _orig_order_cb = order_engine.on_order_callback
    _orig_trade_cb = order_engine.on_trade_callback

    def _order_cb(data):
        _store_callback("order_callback", data)
        if _orig_order_cb:
            _orig_order_cb(data)

    def _trade_cb(data):
        _store_callback("trade_callback", data)
        if _orig_trade_cb:
            _orig_trade_cb(data)

    order_engine.on_order_callback = _order_cb
    order_engine.on_trade_callback = _trade_cb


def set_gateway_connected(connected: bool):
    global _gateway_connected
    _gateway_connected = connected


# ── Pydantic 模型 ────────────────────────────────────────────


class CreateAccountRequest(BaseModel):
    id: str
    name: str
    initial_cash: float = 1_000_000.0
    positions: List[dict] = []


class UpdateAccountRequest(BaseModel):
    name: Optional[str] = None
    cash: Optional[float] = None


class UpdateScenarioProbsRequest(BaseModel):
    filled: float
    partial_filled: float
    rejected: float
    canceled: float


class UpdateScenarioDelayRequest(BaseModel):
    initial_delay: Optional[float] = None
    batch_interval_min: Optional[float] = None
    batch_interval_max: Optional[float] = None
    batch_count_min: Optional[int] = None
    batch_count_max: Optional[int] = None


class UpdateCommissionRequest(BaseModel):
    enabled: Optional[bool] = None
    commission_rate: Optional[float] = None    # 佣金费率，如 0.00025 (万2.5)
    min_commission: Optional[float] = None     # 最低佣金（元），如 5.0
    stamp_duty_rate: Optional[float] = None    # 印花税率（仅卖出），如 0.001 (千1)
    transfer_fee_rate: Optional[float] = None  # 过户费率，如 0.00002 (万0.2)


class SubmitOrderRequest(BaseModel):
    order_id: Optional[str] = None
    symbol: str
    direction: str = "buy"
    price_type: str = "limit"
    price: float = 0.0
    quantity: int = 0
    fund_account_id: str = ""
    wait: bool = False
    timeout: float = 30.0


class AlgoStartRequest(BaseModel):
    algo_id: Optional[str] = None
    algo_type: str
    symbol: str
    symbol_name: str = ""
    direction: str = "buy"
    quantity: int
    fund_account_id: str = ""
    params: dict = {}


class AlgoControlRequest(BaseModel):
    action: str  # pause / resume / cancel


class ManualFillDecisionRequest(BaseModel):
    action: str          # 'fill' | 'reject' | 'cancel'
    fill_qty: Optional[int] = None
    fill_price: Optional[float] = None
    reason: Optional[str] = None


# ── 系统状态 ─────────────────────────────────────────────────


@app.get("/api/status")
def get_status():
    accounts = _account_manager.list_accounts() if _account_manager else []
    orders = _order_engine.get_orders(limit=1000) if _order_engine else []
    today_orders = [o for o in orders if o.get("created_at", "").startswith(
        __import__("datetime").date.today().isoformat()
    )]
    algo_count = len(_algo_engine.list_tasks()) if _algo_engine else 0
    return {
        "executor_id": _executor_id,
        "gateway_connected": _gateway_connected,
        "account_count": len(accounts),
        "today_order_count": len(today_orders),
        "active_algo_count": algo_count,
    }


# ── 账号管理 ─────────────────────────────────────────────────


@app.post("/api/accounts", status_code=201)
def create_account(req: CreateAccountRequest):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    existing = _account_manager.get_account(req.id)
    if existing:
        raise HTTPException(400, f"账号 {req.id} 已存在")
    return _account_manager.create_account(req.id, req.name, req.initial_cash, req.positions)


@app.get("/api/accounts")
def list_accounts():
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    return _account_manager.list_accounts()


@app.get("/api/accounts/{account_id}")
def get_account(account_id: str):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    acc = _account_manager.get_account(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    return acc


@app.get("/api/accounts/{account_id}/balance")
def get_account_balance(account_id: str):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.query_balance_sync(account_id)


@app.get("/api/accounts/{account_id}/positions")
def get_account_positions(account_id: str):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.query_positions_sync(account_id)


@app.put("/api/accounts/{account_id}")
def update_account(account_id: str, req: UpdateAccountRequest):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    result = _account_manager.update_account(account_id, req.name, req.cash)
    if not result:
        raise HTTPException(404, "账号不存在")
    return result


@app.delete("/api/accounts/{account_id}")
def delete_account(account_id: str):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    ok = _account_manager.delete_account(account_id)
    if not ok:
        raise HTTPException(404, "账号不存在")
    return {"success": True}


@app.post("/api/accounts/{account_id}/reset")
def reset_account(account_id: str):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    result = _account_manager.reset_account(account_id)
    if not result:
        raise HTTPException(404, "账号不存在")
    return result


# ── 订单管理 ─────────────────────────────────────────────────


@app.post("/api/orders/submit")
async def submit_order(req: SubmitOrderRequest):
    """提交订单。wait=true 时阻塞直到终态（适合算法引擎子订单）。"""
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")

    order_data = {
        "order_id": req.order_id,
        "symbol": req.symbol,
        "direction": req.direction,
        "price_type": req.price_type,
        "price": req.price,
        "quantity": req.quantity,
        "fund_account_id": req.fund_account_id,
    }

    try:
        if req.wait:
            import asyncio
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, _order_engine.submit_order_blocking, order_data, req.timeout
            )
        else:
            result = _order_engine.submit_order_sync(order_data)
        return result
    except Exception as e:
        logger.error(f"submit_order 异常: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders/{order_id}/cancel")
def cancel_order(order_id: str):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.cancel_order_sync({"external_order_id": order_id})


@app.get("/api/orders")
def list_orders(account_id: Optional[str] = None, status: Optional[str] = None,
                limit: int = 50, offset: int = 0,
                date_from: Optional[str] = None, date_to: Optional[str] = None):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.get_orders(account_id=account_id, status=status,
                                    limit=limit, offset=offset,
                                    date_from=date_from, date_to=date_to)


@app.get("/api/trades")
def list_trades(account_id: Optional[str] = None, limit: int = 50, offset: int = 0,
                date_from: Optional[str] = None, date_to: Optional[str] = None):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.get_trades(account_id=account_id, limit=limit, offset=offset,
                                    date_from=date_from, date_to=date_to)


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    result = _order_engine.get_order_with_trades(order_id)
    if not result:
        raise HTTPException(404, "订单不存在")
    return result


# ── 算法任务 ─────────────────────────────────────────────────


@app.post("/api/algo/start")
def start_algo(req: AlgoStartRequest):
    if not _algo_engine:
        raise HTTPException(503, "算法引擎未就绪")
    data = {
        "algo_id": req.algo_id,
        "algo_type": req.algo_type,
        "symbol": req.symbol,
        "symbol_name": req.symbol_name,
        "direction": req.direction,
        "quantity": req.quantity,
        "fund_account_id": req.fund_account_id,
        "params": req.params,
    }
    return _algo_engine.start_algo(data)


@app.post("/api/algo/{algo_id}/control")
def control_algo(algo_id: str, req: AlgoControlRequest):
    if not _algo_engine:
        raise HTTPException(503, "算法引擎未就绪")
    action = req.action
    if action == "pause":
        return _algo_engine.pause_algo(algo_id)
    elif action == "resume":
        return _algo_engine.resume_algo(algo_id)
    elif action == "cancel":
        return _algo_engine.cancel_algo(algo_id)
    else:
        raise HTTPException(400, f"未知操作: {action}")


@app.get("/api/algo")
def list_algo_tasks(status: Optional[str] = None, account_id: Optional[str] = None):
    if not _algo_engine:
        return []
    tasks = _algo_engine.list_tasks(status=status)
    if account_id:
        tasks = [t for t in tasks if t.get("fund_account_id") == account_id]
    return tasks


@app.get("/api/algo/{algo_id}")
def get_algo_task(algo_id: str):
    if not _algo_engine:
        raise HTTPException(503, "算法引擎未就绪")
    task = _algo_engine.get_task_status(algo_id)
    if not task:
        raise HTTPException(404, "算法任务不存在")
    return task


# ── 回调事件轮询（供外部客户端用） ───────────────────────────


@app.get("/api/callbacks")
def get_callbacks(since: int = 0):
    """返回 id > since 的所有回调事件。客户端轮询后按最大 id 推进 cursor。"""
    with _callback_lock:
        return [c for c in _callbacks if c["id"] > since]


# ── 场景控制 ─────────────────────────────────────────────────


@app.get("/api/scenario")
def get_scenario():
    """获取全局默认场景配置（只读，供页面初始展示用）。"""
    return get_default_config_dict()


# ── 手动成交 ──────────────────────────────────────────────────


@app.get("/api/pending-fills")
def get_pending_fills():
    """获取等待手动成交决策的订单列表（仅在手动模式下有意义）。"""
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    return _order_engine.get_pending_manual_fills()


@app.post("/api/pending-fills/{order_id}/decide")
def decide_pending_fill(order_id: str, req: ManualFillDecisionRequest):
    """对手动待成交订单作出决策。action: fill / reject / cancel。"""
    if not _order_engine:
        raise HTTPException(503, "服务未就绪")
    ok = _order_engine.decide_manual_fill(
        order_id, req.action,
        fill_qty=req.fill_qty,
        fill_price=req.fill_price,
        reason=req.reason,
    )
    if not ok:
        raise HTTPException(404, "找不到待处理订单（可能已超时或已决策）")
    return {"success": True}


# ── 账号级配置 ────────────────────────────────────────────────


@app.get("/api/accounts/{account_id}/config")
def get_account_config(account_id: str):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    cfg = _account_manager.get_account_config(account_id)
    if not cfg:
        cfg = get_default_config_dict()
    return cfg


@app.put("/api/accounts/{account_id}/config")
def update_account_config(account_id: str, config: dict = Body(...)):
    if not _account_manager:
        raise HTTPException(503, "服务未就绪")
    ok = _account_manager.set_account_config(account_id, config)
    if not ok:
        raise HTTPException(404, "账号不存在")
    return config

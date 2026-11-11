"""
仿真客户端 WebSocket 网关客户端

与 gj_executor-qmt/grpc_service/ws_gateway_client.py 接口一致，
连接 FastAPI 后端网关，转发交易指令到仿真服务器。

额外功能：
  - 回调事件轮询线程：轮询仿真服务器 /api/callbacks，将 order/trade 回调转发给网关
  - 账号管理指令（account_manage）转发到仿真服务器 HTTP API
"""
import asyncio
import json
import logging
import os
import platform
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _print_order_callback(d: dict):
    oid        = (d.get("order_id") or d.get("external_order_id") or "")[-12:]
    sym        = d.get("symbol", "")
    acct       = d.get("account_id") or d.get("fund_account_id") or ""
    dire       = "买入" if d.get("direction") == "buy" else ("卖出" if d.get("direction") == "sell" else "")
    price      = d.get("price", 0)
    qty        = d.get("quantity", 0)
    filled_qty = d.get("filled_quantity", 0)
    avg_price  = d.get("avg_filled_price", 0)
    status_map = {
        "submitted": "已报", "partial_filled": "部分成交",
        "filled": "已成交", "rejected": "已拒绝",
        "canceled": "已撤单", "pending": "待报",
    }
    status  = status_map.get(d.get("status", ""), d.get("status", ""))
    msg     = d.get("status_msg") or d.get("message") or d.get("reject_reason") or ""
    order_info = f" {dire} {qty}股 @{price:.2f}" if dire and qty else ""
    detail  = f"  已成交{filled_qty}股 均价{avg_price:.2f}" if filled_qty else ""
    suffix  = f"  [{msg}]" if msg and d.get("status") in ("rejected",) else ""
    acct_str = f"[{acct}] " if acct else ""
    print(f"[委托回调] {acct_str}…{oid}  {sym}{order_info}  → {status}{detail}{suffix}")


def _print_trade_callback(d: dict):
    oid        = (d.get("order_id") or "")[-12:]
    acct       = d.get("account_id") or d.get("fund_account_id") or ""
    sym        = d.get("symbol", "")
    dire       = "买入" if d.get("direction") == "buy" else "卖出"
    qty        = d.get("quantity", 0)
    price      = d.get("price", 0)
    amount     = d.get("amount", 0)
    commission = d.get("commission")
    comm_str   = f"  手续费{commission:.2f}" if commission else ""
    acct_str   = f"[{acct}] " if acct else ""
    print(f"[成交回调] {acct_str}…{oid}  {sym} {dire} {qty}股 @{price:.2f}"
          + (f"  金额{amount:.2f}" if amount else "") + comm_str)


try:
    import websockets
    import websockets.exceptions
except ImportError:
    raise ImportError("请安装 websockets 库: pip install websockets")


class WSGatewayClient:
    """仿真执行器网关 WebSocket 客户端（与 gj_executor-qmt 接口一致）。"""

    def __init__(self, ws_url: str, executor_id: str, executor_type: str,
                 sim_executor, reconnect_interval: int = 5):
        self.ws_url = ws_url
        self.executor_type = executor_type
        self._executor = sim_executor
        self.reconnect_interval = reconnect_interval
        self.executor_id = self._load_or_create_executor_id(executor_id)

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._stop_event = threading.Event()
        self._connected = False

        # 回调轮询状态
        # 初始化为当前最大 ID（启动时跳过历史回调，只处理启动后的新事件）
        self._callback_cursor: int = self._fetch_current_cursor()
        self._callback_poll_thread: Optional[threading.Thread] = None

    def _fetch_current_cursor(self) -> int:
        """启动时查询仿真服务器当前最大 callback ID，避免重放历史回调。"""
        try:
            import requests
            server_url = self._executor.server_url
            resp = requests.get(f"{server_url}/api/callbacks", params={"since": 0}, timeout=3)
            if resp.status_code == 200:
                events = resp.json()
                if events:
                    max_id = max(e.get("id", 0) for e in events)
                    logger.info(f"[SimWS] 启动时跳过 {len(events)} 条历史回调（cursor→{max_id}）")
                    return max_id
        except Exception as e:
            logger.debug(f"[SimWS] 初始化 cursor 失败（仿真服务器可能未启动）: {e}")
        return 0

    def _load_or_create_executor_id(self, configured_id: str) -> str:
        from .settings import CACHE_DIR
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = CACHE_DIR / "executor_id.txt"

        if configured_id:
            cache_file.write_text(configured_id, encoding="utf-8")
            return configured_id

        if cache_file.exists():
            saved = cache_file.read_text(encoding="utf-8").strip()
            if saved:
                return saved

        new_id = f"sim-{str(uuid.uuid4())[:8]}"
        cache_file.write_text(new_id, encoding="utf-8")
        return new_id

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name="ws-sim-gateway")
        self._thread.start()

        # 启动回调轮询线程
        self._callback_poll_thread = threading.Thread(
            target=self._callback_poll_loop, daemon=True, name="sim-callback-poll"
        )
        self._callback_poll_thread.start()

        logger.info(f"[SimWS] 已启动 executor_id={self.executor_id}")

    def stop(self):
        self._stop_event.set()

    def report_qmt_status(self, connected: bool, status_code: int, status_msg: str):
        self._send("qmt_status", {
            "executor_id": self.executor_id,
            "connected": connected,
            "status_code": status_code,
            "status_msg": status_msg,
            "timestamp": int(time.time() * 1000),
        })

    def report_order(self, order_data: dict):
        self._send("order_callback", order_data)

    def report_trade(self, trade_data: dict):
        self._send("trade_callback", trade_data)

    def report_balance(self, balance_data: dict):
        self._send("balance_callback", balance_data)

    def report_algo_progress(self, task_data: dict):
        """上报算法执行进度（含切片明细）给后端持久化"""
        self._send("algo_progress", task_data)

    # ── 内部实现 ──────────────────────────────────────────────────

    def _send(self, msg_type: str, data: dict):
        if self._loop and self._send_queue and not self._stop_event.is_set():
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put({"type": msg_type, "data": data}),
                self._loop,
            )

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main())
        finally:
            self._loop.close()

    async def _main(self):
        self._send_queue = asyncio.Queue()
        retry_delay = 1.0
        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
                retry_delay = 1.0
            except Exception as e:
                logger.warning(f"[SimWS] 连接断开: {e}，{retry_delay:.0f}s 后重试")
                self._connected = False
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    async def _connect_and_run(self):
        logger.info(f"[SimWS] 正在连接: {self.ws_url}")
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
            self._connected = True
            logger.info(f"[SimWS] 连接成功，发送注册消息")

            reg_msg = {"type": "register", "data": self._build_register_data()}
            await ws.send(json.dumps(reg_msg, ensure_ascii=False))

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
            try:
                await asyncio.gather(self._receive_loop(ws), self._send_loop(ws))
            finally:
                heartbeat_task.cancel()
                self._connected = False

    async def _receive_loop(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await self._dispatch(msg)

    async def _send_loop(self, ws):
        while True:
            msg = await self._send_queue.get()
            try:
                await ws.send(json.dumps(msg, ensure_ascii=False))
            except websockets.exceptions.ConnectionClosed:
                raise
            except Exception as e:
                logger.error(f"[SimWS] 发送失败: {e}")

    async def _heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(30)
            await self._send_queue.put({
                "type": "heartbeat",
                "data": {"executor_id": self.executor_id, "timestamp": int(time.time() * 1000)},
            })

    async def _dispatch(self, msg: dict):
        msg_type = msg.get("type", "")
        data = msg.get("data", {})
        command_id = data.get("command_id", "")

        if msg_type == "register_ack":
            logger.info(f"[SimWS] 注册确认: {data.get('message')}")
            return

        try:
            result = await self._handle_command(msg_type, data)
            success = result.get("success", True) if isinstance(result, dict) else True
            message = result.get("message", "") if isinstance(result, dict) else ""
            await self._send_queue.put({
                "type": "command_response",
                "data": {
                    "command_id": command_id,
                    "success": success,
                    "message": message,
                    "data": result,
                },
            })
        except Exception as e:
            logger.error(f"[SimWS] 指令异常 {msg_type}: {e}", exc_info=True)
            await self._send_queue.put({
                "type": "command_response",
                "data": {"command_id": command_id, "success": False, "message": str(e), "data": {}},
            })

    async def _handle_command(self, msg_type: str, data: dict) -> dict:
        loop = asyncio.get_event_loop()

        if msg_type == "submit_order":
            return await loop.run_in_executor(None, self._executor.submit_order_sync, data)

        elif msg_type == "cancel_order":
            return await loop.run_in_executor(None, self._executor.cancel_order_sync, data)

        elif msg_type == "query":
            query_type = data.get("query_type", "")
            params = data.get("params", {})
            account_id = params.get("fund_account_id", "") if isinstance(params, dict) else str(params)
            if query_type == "balance":
                return await loop.run_in_executor(None, self._executor.query_balance_sync, account_id)
            elif query_type == "positions":
                return await loop.run_in_executor(None, self._executor.query_positions_sync, account_id)
            elif query_type == "order":
                order_id = params.get("order_id", "") if isinstance(params, dict) else ""
                return await loop.run_in_executor(
                    None, self._executor.query_order_sync, order_id, "", account_id
                )
            return {"success": False, "message": f"未知查询类型: {query_type}"}

        elif msg_type == "start_algo":
            return await loop.run_in_executor(None, self._executor.start_algo_task, data)

        elif msg_type == "algo_control":
            algo_id = data.get("algo_id", "")
            action = data.get("action", "")
            if action == "pause":
                return await loop.run_in_executor(None, self._executor.pause_algo_task, algo_id)
            elif action == "resume":
                return await loop.run_in_executor(None, self._executor.resume_algo_task, algo_id)
            elif action == "cancel":
                return await loop.run_in_executor(None, self._executor.cancel_algo_task, algo_id)
            return {"success": False, "message": f"未知操作: {action}"}

        elif msg_type == "algo_query":
            algo_id = data.get("algo_id", "")
            if algo_id:
                status = await loop.run_in_executor(None, self._executor.get_algo_task_status, algo_id)
                return {"success": True, "data": status}  # None 表示任务不存在
            else:
                tasks = await loop.run_in_executor(None, self._executor.list_algo_tasks)
                return {"success": True, "data": tasks}

        elif msg_type == "account_manage":
            return await loop.run_in_executor(None, self._handle_account_manage, data)

        elif msg_type == "management":
            command = data.get("command", "")
            if command == "shutdown":
                threading.Thread(target=lambda: (time.sleep(1), os._exit(0)), daemon=True).start()
                return {"success": True, "message": "关闭指令已执行"}
            return {"success": False, "message": f"未知管理指令: {command}"}

        else:
            logger.warning(f"[SimWS] 未知指令: {msg_type}")
            return {"success": False, "message": f"未知指令: {msg_type}"}

    def _handle_account_manage(self, data: dict) -> dict:
        """转发账号管理指令到仿真服务器 HTTP API。"""
        action = data.get("action", "")
        account_id = data.get("account_id", "")

        if action == "list":
            accounts = self._executor.list_accounts()
            return {"success": True, "data": accounts}

        elif action == "get":
            acc = self._executor.get_account(account_id)
            if not acc:
                return {"success": False, "message": "账号不存在"}
            return {"success": True, "data": acc}

        elif action == "create":
            return self._executor.create_account(
                account_id=data.get("account_id", ""),
                name=data.get("name", ""),
                initial_cash=data.get("initial_cash", 1_000_000.0),
            )

        elif action == "update":
            return self._executor.update_account(
                account_id=account_id,
                name=data.get("name"),
                cash=data.get("cash"),
            )

        elif action == "delete":
            return self._executor.delete_account(account_id)

        elif action == "reset":
            return self._executor.reset_account(account_id)

        return {"success": False, "message": f"未知操作: {action}"}

    # ── 回调事件轮询 ──────────────────────────────────────────────

    def _callback_poll_loop(self):
        """轮询仿真服务器 /api/callbacks，将 order/trade 回调转发给网关。"""
        import requests

        server_url = self._executor.server_url
        session = requests.Session()

        while not self._stop_event.is_set():
            time.sleep(0.5)
            if not self._connected:
                continue
            try:
                resp = session.get(
                    f"{server_url}/api/callbacks",
                    params={"since": self._callback_cursor},
                    timeout=3,
                )
                if resp.status_code != 200:
                    continue
                events = resp.json()
                for ev in events:
                    ev_id = ev.get("id", 0)
                    ev_type = ev.get("type", "")
                    ev_data = ev.get("data", {})

                    if ev_type in ("order_callback", "trade_callback"):
                        # 过滤算法子单回调（格式：{parent_id}_{序号}），
                        # 父订单聚合状态由 algo_executor 直接上报，子单无需转发给后端
                        oid = ev_data.get("order_id", "")
                        if oid:
                            parts = oid.rsplit("_", 1)
                            if len(parts) == 2 and parts[1].isdigit():
                                algo_exec = getattr(self._executor, "_algo_executor", None)
                                if algo_exec:
                                    with algo_exec._lock:
                                        is_sub = parts[0] in algo_exec.tasks
                                    if is_sub:
                                        if ev_id > self._callback_cursor:
                                            self._callback_cursor = ev_id
                                        continue  # 跳过子单回调

                        if ev_type == "order_callback":
                            _print_order_callback(ev_data)
                            self.report_order(ev_data)
                        else:
                            _print_trade_callback(ev_data)
                            self.report_trade(ev_data)

                    if ev_id > self._callback_cursor:
                        self._callback_cursor = ev_id
            except Exception as e:
                logger.debug(f"[SimWS] 回调轮询失败: {e}")

    def _build_register_data(self) -> dict:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
        try:
            mac = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff)
                            for i in range(0, 8 * 6, 8)][::-1])
        except Exception:
            mac = ""

        # 从仿真服务器获取已有账号列表
        try:
            accounts = [a.get("id", "") for a in self._executor.list_accounts()]
        except Exception:
            accounts = []

        return {
            "executor_id": self.executor_id,
            "executor_type": self.executor_type,
            "hostname": socket.gethostname(),
            "ip_address": ip,
            "mac_address": mac,
            "os_type": platform.system(),
            "python_version": platform.python_version(),
            "working_dir": str(Path(__file__).parent),
            "fund_accounts": accounts,
            "timestamp": int(time.time() * 1000),
        }

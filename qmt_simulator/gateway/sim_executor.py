"""
仿真执行器 HTTP 适配器

提供兼容交易网关执行器的调用接口，底层通过 HTTP 调用本项目服务端。

主要方法：
  submit_order_sync(order_data) -> dict
  cancel_order_sync(order_data) -> dict
  query_balance_sync(fund_account_id) -> dict
  query_positions_sync(fund_account_id) -> list
  start_algo_task(data) -> dict       ← 委托给 AlgoExecutor
  pause_algo_task(algo_id) -> dict
  resume_algo_task(algo_id) -> dict
  cancel_algo_task(algo_id) -> dict
  get_algo_task_status(algo_id) -> dict
  list_algo_tasks() -> list
"""
import logging
import time
from typing import Optional

try:
    import requests
except ImportError:
    raise ImportError("请安装 requests 库: pip install requests")

logger = logging.getLogger(__name__)


class SimExecutor:
    """HTTP 适配器：将执行器接口映射到仿真服务器 REST API。"""

    def __init__(self, server_url: str, algo_executor=None):
        self.server_url = server_url.rstrip("/")
        self._algo_executor = algo_executor   # 由外部注入 SimAlgoExecutor 实例
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        # 回调属性（与 qmt_executor 接口一致，由 ws_gateway_client 注入）
        self.on_order_callback = None
        self.on_trade_callback = None

    def set_algo_executor(self, algo_executor):
        self._algo_executor = algo_executor

    # ── 订单接口 ────────────────────────────────────────────────

    def submit_order_sync(self, order_data: dict) -> dict:
        """非阻塞提交订单，立即返回 submitted 状态。"""
        try:
            payload = {**order_data, "wait": False}
            resp = self._session.post(
                f"{self.server_url}/api/orders/submit",
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 提交订单失败: {e}")
            return {"success": False, "message": str(e), "external_order_id": ""}

    def submit_order_blocking(self, order_data: dict, timeout: float = 30.0) -> dict:
        """阻塞提交：等待订单到达终态后返回（包含成交明细）。"""
        try:
            payload = {**order_data, "wait": True, "timeout": timeout}
            resp = self._session.post(
                f"{self.server_url}/api/orders/submit",
                json=payload,
                timeout=timeout + 10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 阻塞提交失败: {e}")
            return {"success": False, "message": str(e), "filled_qty": 0}

    def cancel_order_sync(self, order_data: dict) -> dict:
        ext_id = order_data.get("external_order_id", "")
        if not ext_id:
            return {"success": False, "message": "external_order_id 为空"}
        try:
            resp = self._session.post(
                f"{self.server_url}/api/orders/{ext_id}/cancel",
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 撤单失败: {e}")
            return {"success": False, "message": str(e)}

    def query_balance_sync(self, fund_account_id: str) -> dict:
        try:
            resp = self._session.get(
                f"{self.server_url}/api/accounts/{fund_account_id}/balance",
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 查询余额失败: {e}")
            return {}

    def query_positions_sync(self, fund_account_id: str) -> list:
        try:
            resp = self._session.get(
                f"{self.server_url}/api/accounts/{fund_account_id}/positions",
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 查询持仓失败: {e}")
            return []

    def query_order_sync(self, external_order_id: str, account_id: str, fund_account_id: str):
        params = {"account_id": fund_account_id or account_id}
        try:
            if external_order_id:
                resp = self._session.get(
                    f"{self.server_url}/api/orders/{external_order_id}",
                    timeout=5,
                )
            else:
                resp = self._session.get(
                    f"{self.server_url}/api/orders",
                    params=params,
                    timeout=5,
                )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 查询订单失败: {e}")
            return {} if external_order_id else []

    # ── 算法接口（委托给 AlgoExecutor） ─────────────────────────

    def start_algo_task(self, data: dict) -> dict:
        if not self._algo_executor:
            return {"success": False, "message": "AlgoExecutor 未初始化"}
        return self._algo_executor.start_algo(data)

    def pause_algo_task(self, algo_id: str) -> dict:
        if not self._algo_executor:
            return {"success": False, "message": "AlgoExecutor 未初始化"}
        return self._algo_executor.pause_algo(algo_id)

    def resume_algo_task(self, algo_id: str) -> dict:
        if not self._algo_executor:
            return {"success": False, "message": "AlgoExecutor 未初始化"}
        return self._algo_executor.resume_algo(algo_id)

    def cancel_algo_task(self, algo_id: str) -> dict:
        if not self._algo_executor:
            return {"success": False, "message": "AlgoExecutor 未初始化"}
        return self._algo_executor.cancel_algo(algo_id)

    def get_algo_task_status(self, algo_id: str) -> Optional[dict]:
        if not self._algo_executor:
            return None
        return self._algo_executor.get_task_status(algo_id)

    def list_algo_tasks(self) -> list:
        if not self._algo_executor:
            return []
        return self._algo_executor.list_tasks()

    # ── 账号管理（通过服务器 API） ───────────────────────────────

    def list_accounts(self) -> list:
        try:
            resp = self._session.get(f"{self.server_url}/api/accounts", timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 查询账号列表失败: {e}")
            return []

    def get_account(self, account_id: str) -> Optional[dict]:
        try:
            resp = self._session.get(f"{self.server_url}/api/accounts/{account_id}", timeout=5)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"[SimExec] 查询账号失败: {e}")
            return None

    def create_account(self, account_id: str, name: str, initial_cash: float = 1_000_000.0) -> dict:
        try:
            resp = self._session.post(
                f"{self.server_url}/api/accounts",
                json={"id": account_id, "name": name, "initial_cash": initial_cash},
                timeout=10,
            )
            resp.raise_for_status()
            return {"success": True, "data": resp.json()}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def update_account(self, account_id: str, name: Optional[str] = None, cash: Optional[float] = None) -> dict:
        try:
            resp = self._session.put(
                f"{self.server_url}/api/accounts/{account_id}",
                json={"name": name, "cash": cash},
                timeout=10,
            )
            resp.raise_for_status()
            return {"success": True, "data": resp.json()}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def delete_account(self, account_id: str) -> dict:
        try:
            resp = self._session.delete(
                f"{self.server_url}/api/accounts/{account_id}",
                timeout=10,
            )
            resp.raise_for_status()
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def reset_account(self, account_id: str) -> dict:
        try:
            resp = self._session.post(
                f"{self.server_url}/api/accounts/{account_id}/reset",
                timeout=10,
            )
            resp.raise_for_status()
            return {"success": True, "data": resp.json()}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── 连接状态（兼容 QMT 接口） ────────────────────────────────

    @property
    def connected(self) -> bool:
        try:
            resp = self._session.get(f"{self.server_url}/api/status", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False

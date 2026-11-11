"""
XM 仿真执行器客户端

连接到：
  1. 仿真服务器（默认 http://localhost:19720）
  2. 后端网关 WebSocket（默认 ws://localhost:19600/api/v1/ws/executor）

功能与 gj_executor-qmt 相同：
  - 接收 submit_order / cancel_order / query 等交易指令
  - 完整算法订单支持（TWAP / ICEBERG / VWAP / POV / MARKET / LIMIT / LIMIT_UP / FAST_TRADE / STEALTH）
  - 每 3 秒上报资金数据
  - 自动重连网关

启动方式：
  python main.py
  SIM_SERVER_URL=http://192.168.1.100:19720 python main.py   # 远程仿真服务器
"""
import logging
import argparse
import os
import signal
import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Bridge the simulator to a compatible trading gateway")
    parser.add_argument("--sim-server-url", help="Simulator HTTP URL")
    parser.add_argument("--ws-url", help="Gateway WebSocket URL")
    parser.add_argument("--backend-url", help="Backend HTTP URL used by algorithm execution")
    parser.add_argument("--executor-id", help="Persistent executor ID")
    args = parser.parse_args()

    if args.sim_server_url:
        os.environ["SIM_SERVER_URL"] = args.sim_server_url
    if args.ws_url:
        os.environ["WS_URL"] = args.ws_url
    if args.backend_url:
        os.environ["BACKEND_URL"] = args.backend_url
    if args.executor_id:
        os.environ["EXECUTOR_ID"] = args.executor_id

    from . import settings as cfg

    print("=" * 60)
    print("XM 仿真执行器客户端")
    print("=" * 60)
    print(f"仿真服务器:  {cfg.SIM_SERVER_URL}")
    print(f"网关地址:    {cfg.WS_URL}")
    print()

    from .sim_executor import SimExecutor
    from .algo_executor import SimAlgoExecutor
    from .ws_gateway_client import WSGatewayClient

    # 初始化 HTTP 适配器
    sim_executor = SimExecutor(server_url=cfg.SIM_SERVER_URL)

    # 初始化算法执行器
    algo_executor = SimAlgoExecutor(
        sim_executor=sim_executor,
        backend_url=cfg.BACKEND_URL,
    )
    sim_executor.set_algo_executor(algo_executor)

    # 初始化网关 WebSocket 客户端
    gateway_client = WSGatewayClient(
        ws_url=cfg.WS_URL,
        executor_id=cfg.EXECUTOR_ID,
        executor_type=cfg.EXECUTOR_TYPE,
        sim_executor=sim_executor,
        reconnect_interval=cfg.RECONNECT_INTERVAL,
    )

    print(f"[Client] 执行器 ID: {gateway_client.executor_id}")

    # 将网关回调函数注入算法执行器（算法完成时上报 order/trade 给网关）
    algo_executor.on_order_callback = gateway_client.report_order
    algo_executor.on_trade_callback = gateway_client.report_trade
    algo_executor.on_progress_callback = gateway_client.report_algo_progress

    # ── 资金定时上报（每 3 秒）──────────────────────────────────
    def _balance_report_loop():
        from datetime import datetime as _dt
        while not gateway_client._stop_event.is_set():
            time.sleep(3)
            if not getattr(gateway_client, '_connected', False):
                continue
            try:
                accounts = sim_executor.list_accounts()
                for acc in accounts:
                    market_value = sum(
                        p.get('cost_price', 0) * p.get('quantity', 0)
                        for p in acc.get('positions', [])
                    )
                    cash = acc.get('cash', 0.0)
                    gateway_client.report_balance({
                        "fund_account_id": acc['id'],
                        "total_asset": round(cash + market_value, 4),
                        "available_cash": round(cash, 4),
                        "market_value": round(market_value, 4),
                        "frozen_cash": 0.0,
                        "updated_at": _dt.now().isoformat(),
                    })
            except Exception:
                pass

    threading.Thread(target=_balance_report_loop, daemon=True, name="sim-balance-report").start()
    print("[Client] 资金定时上报已启动（3s/次）")

    # 启动网关 WS 客户端
    gateway_client.start()
    print("[Client] 网关客户端已启动（后台连接中）")

    # 等待连接建立后上报就绪状态
    time.sleep(2)
    try:
        gateway_client.report_qmt_status(
            connected=True, status_code=0, status_msg="仿真客户端已就绪"
        )
    except Exception:
        pass

    # 信号处理
    def signal_handler(signum, frame):
        print("\n[Client] 收到退出信号，正在关闭...")
        gateway_client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, signal_handler)

    print("[Client] 运行中，Ctrl+C 退出")
    print()

    # 主循环
    while not gateway_client._stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()

"""仿真客户端配置"""
import os
from pathlib import Path

# ── 网关连接 ─────────────────────────────────────────────────
WS_URL: str = os.environ.get("WS_URL", "ws://localhost:19600/api/v1/ws/executor")
EXECUTOR_ID: str = os.environ.get("EXECUTOR_ID", "")   # 空则自动持久化/生成
EXECUTOR_TYPE: str = "simulate"
RECONNECT_INTERVAL: int = int(os.environ.get("RECONNECT_INTERVAL", "5"))

# ── 仿真服务器 ───────────────────────────────────────────────
SIM_SERVER_URL: str = os.environ.get("SIM_SERVER_URL", "http://localhost:19720")

# ── 后端行情服务（用于获取实时价格） ─────────────────────────
BACKEND_URL: str = os.environ.get("BACKEND_URL", "http://localhost:19600")

# ── 执行器 ID 持久化 ─────────────────────────────────────────
CACHE_DIR: Path = Path(os.environ.get(
    "GATEWAY_CACHE_DIR",
    str(Path(__file__).resolve().parents[2] / "data" / "gateway-cache"),
))

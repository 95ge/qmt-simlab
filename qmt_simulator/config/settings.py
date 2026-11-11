"""仿真执行器配置"""
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings:
    # FastAPI WebSocket 地址（已切换为 WebSocket 直连，不再使用 gRPC）
    ws_url: str = os.environ.get("WS_URL", "ws://localhost:19600/api/v1/ws/executor")
    # HTTP 管理界面端口
    http_host: str = os.environ.get("HTTP_HOST", "0.0.0.0")
    http_port: int = int(os.environ.get("HTTP_PORT", "19720"))
    # 执行器 ID（注册到后端时使用，空则自动生成并持久化）
    executor_id: str = os.environ.get("EXECUTOR_ID", "sim-executor")
    # 执行器类型
    executor_type: str = "simulate"
    # 数据库路径
    db_path: str = os.environ.get("DB_PATH", str(PROJECT_ROOT / "data" / "sim.db"))
    # 重连间隔（秒）
    reconnect_interval: int = 5
    # 行情/策略算法可选的外部服务地址
    backend_url: str = os.environ.get("BACKEND_URL", "http://localhost:19600")
    quote_retention_ms: int = int(os.environ.get("QUOTE_RETENTION_MS", "3600000"))
    quote_max_symbols: int = int(os.environ.get("QUOTE_MAX_SYMBOLS", "10000"))


settings = Settings()

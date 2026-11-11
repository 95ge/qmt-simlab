"""数据库引擎和 Session"""
import logging
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from .models import Base

logger = logging.getLogger(__name__)


def get_engine(db_path: str = "data/sim.db"):
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(
        f"sqlite:///{path.as_posix()}",
        connect_args={"check_same_thread": False},
    )


_engine = None
_SessionLocal = None

# 需要检查/补充的列：{表名: [(列名, 列定义)]}
_MIGRATIONS = {
    "trades": [
        ("account_id", "TEXT NOT NULL DEFAULT ''"),
        ("symbol",     "TEXT NOT NULL DEFAULT ''"),
        ("direction",  "TEXT NOT NULL DEFAULT ''"),
        ("commission", "REAL NOT NULL DEFAULT 0.0"),
    ],
    "accounts": [
        ("scenario_config", "TEXT"),
    ],
}


def _run_migrations(engine):
    """为旧版本数据库补全缺失列（SQLite 不支持 DROP/ALTER COLUMN，只能 ADD）。"""
    with engine.connect() as conn:
        for table, columns in _MIGRATIONS.items():
            for col_name, col_def in columns:
                try:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"))
                    conn.commit()
                    logger.info(f"Migration: added column {table}.{col_name}")
                except Exception:
                    pass  # 列已存在，忽略


def init_db(db_path: str = "data/sim.db"):
    global _engine, _SessionLocal
    _engine = get_engine(db_path)
    Base.metadata.create_all(_engine)
    _run_migrations(_engine)
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


def get_session() -> Session:
    return _SessionLocal()

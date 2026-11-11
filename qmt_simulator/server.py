"""Standalone QMT simulation server.

The server exposes a REST API and a small browser UI.  It deliberately keeps
the runtime bootstrap here so importing the simulator package does not create
database connections or start background threads.
"""

from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


def create_app() -> Any:
    """Build and initialise the FastAPI application."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from fastapi.staticfiles import StaticFiles

    from .config.settings import settings
    from .simulator.database import init_db
    from .simulator.account_manager import AccountManager
    from .simulator.order_engine import OrderEngine
    from .simulator.market_data import QuoteStore
    from .simulator.algo_engine import SimAlgoEngine
    from .simulator.scenario import scenario
    from .web.api import app, init_api

    db_path = Path(settings.db_path).expanduser()
    init_db(str(db_path))

    db_engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )
    session_factory = sessionmaker(bind=db_engine)
    account_manager = AccountManager(session_factory)
    quote_store = QuoteStore(retention_ms=settings.quote_retention_ms,
                             max_symbols=settings.quote_max_symbols)
    order_engine = OrderEngine(
        manager=account_manager,
        session_factory=session_factory,
        scenario=scenario,
        quote_store=quote_store,
    )
    algo_engine = SimAlgoEngine(
        order_engine=order_engine,
        account_manager=account_manager,
        backend_url=settings.backend_url,
    )

    @asynccontextmanager
    async def lifespan(application):
        quote_store.start_cleanup()
        try:
            yield
        finally:
            quote_store.stop_cleanup()

    app.router.lifespan_context = lifespan

    scenario_path = db_path.parent / "scenario.json"
    init_api(
        account_manager,
        order_engine,
        settings.executor_id,
        algo_engine,
        scenario_path=scenario_path,
    )

    static_dir = Path(__file__).parent / "web" / "static"
    if not any(getattr(route, "name", None) == "static" for route in app.routes):
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    accounts = account_manager.list_accounts()
    logger.info("QMT simulator ready: db=%s accounts=%s", db_path, len(accounts))
    return app


def run_server(host: str | None = None, port: int | None = None) -> None:
    """Start the HTTP server and block until it exits."""
    import uvicorn

    from .config.settings import settings

    application = create_app()
    bind_host = host or settings.http_host
    bind_port = port or settings.http_port
    uvicorn.run(application, host=bind_host, port=bind_port, log_level="info")


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone QMT simulator")
    parser.add_argument("--host", default=None, help="HTTP bind host")
    parser.add_argument("--port", type=int, default=None, help="HTTP port")
    args = parser.parse_args()
    run_server(args.host, args.port)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()

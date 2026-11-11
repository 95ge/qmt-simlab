"""HTTP client for the standalone QMT simulator service."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import requests

from .models import Account, Balance, Order, OrderResult, Position, Trade


class SimulationAPIError(requests.HTTPError):
    """Raised when the simulator returns an HTTP error or invalid JSON."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class SimulationClient:
    """Convenient, typed wrapper around the simulator REST API.

    The client is deliberately independent from the XM trading backend.  Point
    ``base_url`` at any reachable simulator instance and use it from scripts,
    notebooks or automated tests.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:19720",
        *,
        timeout: float = 10.0,
        session: Optional[requests.Session] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.setdefault("Accept", "application/json")
        self.session.headers.setdefault("Content-Type", "application/json")

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", self.timeout)
        try:
            response = self.session.request(method, f"{self.base_url}{path}", **kwargs)
        except requests.RequestException as exc:
            raise SimulationAPIError(str(exc)) from exc

        try:
            payload = response.json() if response.content else None
        except ValueError as exc:
            raise SimulationAPIError(
                "simulator returned invalid JSON",
                status_code=response.status_code,
            ) from exc

        if not 200 <= response.status_code < 300:
            detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
            raise SimulationAPIError(
                f"simulator request failed ({response.status_code}): {detail}",
                status_code=response.status_code,
                payload=payload,
            )
        return payload

    def health(self) -> dict:
        return self._request("GET", "/api/status")

    def status(self) -> dict:
        return self.health()

    # Account and portfolio -------------------------------------------------
    def list_accounts(self) -> List[Account]:
        return [Account.from_dict(item) for item in self._request("GET", "/api/accounts")]

    def get_account(self, account_id: str) -> Optional[Account]:
        try:
            return Account.from_dict(self._request("GET", f"/api/accounts/{account_id}"))
        except SimulationAPIError as exc:
            if exc.status_code == 404:
                return None
            raise

    def create_account(
        self,
        account_id: str,
        name: str,
        initial_cash: float = 1_000_000.0,
        positions: Optional[Iterable[dict]] = None,
    ) -> Account:
        payload = {
            "id": account_id,
            "name": name,
            "initial_cash": initial_cash,
            "positions": list(positions or []),
        }
        return Account.from_dict(self._request("POST", "/api/accounts", json=payload))

    def update_account(
        self,
        account_id: str,
        *,
        name: Optional[str] = None,
        cash: Optional[float] = None,
    ) -> Account:
        return Account.from_dict(
            self._request("PUT", f"/api/accounts/{account_id}", json={"name": name, "cash": cash})
        )

    def delete_account(self, account_id: str) -> dict:
        return self._request("DELETE", f"/api/accounts/{account_id}")

    def reset_account(self, account_id: str) -> Account:
        return Account.from_dict(self._request("POST", f"/api/accounts/{account_id}/reset"))

    def get_balance(self, account_id: str) -> Balance:
        return Balance.from_dict(self._request("GET", f"/api/accounts/{account_id}/balance"))

    def get_positions(self, account_id: str) -> List[Position]:
        return [Position.from_dict(item) for item in self._request("GET", f"/api/accounts/{account_id}/positions")]

    # Orders and trades -----------------------------------------------------
    def submit_order(
        self,
        symbol: str,
        direction: str,
        quantity: int,
        account_id: str,
        *,
        price: float = 0.0,
        price_type: str = "limit",
        order_id: Optional[str] = None,
        wait: bool = False,
        timeout: float = 30.0,
    ) -> OrderResult:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "direction": direction,
            "price_type": price_type,
            "price": price,
            "quantity": quantity,
            "fund_account_id": account_id,
            "wait": wait,
            "timeout": timeout,
        }
        if order_id:
            payload["order_id"] = order_id
        result = self._request("POST", "/api/orders/submit", json=payload)
        return OrderResult.from_dict({**result, "raw": result})

    def cancel_order(self, order_id: str) -> dict:
        return self._request("POST", f"/api/orders/{order_id}/cancel")

    def list_orders(
        self,
        *,
        account_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Order]:
        params = {"limit": limit, "offset": offset}
        if account_id:
            params["account_id"] = account_id
        if status:
            params["status"] = status
        return [Order.from_dict(item) for item in self._request("GET", "/api/orders", params=params)]

    def get_order(self, order_id: str) -> Optional[Order]:
        try:
            return Order.from_dict(self._request("GET", f"/api/orders/{order_id}"))
        except SimulationAPIError as exc:
            if exc.status_code == 404:
                return None
            raise

    def list_trades(self, *, account_id: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Trade]:
        params = {"limit": limit, "offset": offset}
        if account_id:
            params["account_id"] = account_id
        return [Trade.from_dict(item) for item in self._request("GET", "/api/trades", params=params)]

    # Scenarios, callbacks and algorithms ----------------------------------
    def get_scenario(self) -> dict:
        return self._request("GET", "/api/scenario")

    def get_account_config(self, account_id: str) -> dict:
        return self._request("GET", f"/api/accounts/{account_id}/config")

    def update_account_config(self, account_id: str, config: dict) -> dict:
        return self._request("PUT", f"/api/accounts/{account_id}/config", json=config)

    def callbacks(self, since: int = 0) -> List[dict]:
        return self._request("GET", "/api/callbacks", params={"since": since})

    def pending_fills(self) -> List[dict]:
        return self._request("GET", "/api/pending-fills")

    def decide_fill(
        self,
        order_id: str,
        action: str,
        *,
        fill_qty: Optional[int] = None,
        fill_price: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> dict:
        payload = {"action": action, "fill_qty": fill_qty, "fill_price": fill_price, "reason": reason}
        return self._request("POST", f"/api/pending-fills/{order_id}/decide", json=payload)

    def start_algo(self, **payload: Any) -> dict:
        return self._request("POST", "/api/algo/start", json=payload)

    def control_algo(self, algo_id: str, action: str) -> dict:
        return self._request("POST", f"/api/algo/{algo_id}/control", json={"action": action})

    def list_algos(self, *, status: Optional[str] = None, account_id: Optional[str] = None) -> list:
        params = {}
        if status:
            params["status"] = status
        if account_id:
            params["account_id"] = account_id
        return self._request("GET", "/api/algo", params=params)

    def get_algo(self, algo_id: str) -> Optional[dict]:
        try:
            return self._request("GET", f"/api/algo/{algo_id}")
        except SimulationAPIError as exc:
            if exc.status_code == 404:
                return None
            raise


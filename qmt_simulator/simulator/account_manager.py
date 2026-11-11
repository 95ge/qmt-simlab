"""账号管理：多账号 CRUD、资金变动、持仓更新"""
import json
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import sessionmaker

from .models import Account, Position


class AccountManager:
    def __init__(self, session_factory: sessionmaker):
        self._session_factory = session_factory
        self._lock = threading.Lock()

    # ── 账号 CRUD ────────────────────────────────────────────

    def create_account(
        self,
        account_id: str,
        name: str,
        initial_cash: float = 1_000_000.0,
        positions: Optional[List[Dict]] = None,
    ) -> Dict:
        with self._lock:
            db = self._session_factory()
            try:
                acc = Account(
                    id=account_id,
                    name=name,
                    initial_cash=initial_cash,
                    cash=initial_cash,
                    created_at=datetime.utcnow(),
                )
                db.add(acc)
                if positions:
                    for p in positions:
                        db.add(Position(
                            account_id=account_id,
                            symbol=p["symbol"],
                            quantity=p["quantity"],
                            available_qty=p.get("available_qty", p["quantity"]),
                            cost_price=p.get("cost_price", 0.0),
                        ))
                db.commit()
                return self._account_to_dict(db, acc)
            finally:
                db.close()

    def get_account(self, account_id: str) -> Optional[Dict]:
        db = self._session_factory()
        try:
            acc = db.query(Account).filter_by(id=account_id).first()
            if not acc:
                return None
            return self._account_to_dict(db, acc)
        finally:
            db.close()

    def list_accounts(self) -> List[Dict]:
        db = self._session_factory()
        try:
            accounts = db.query(Account).all()
            return [self._account_to_dict(db, a) for a in accounts]
        finally:
            db.close()

    def update_account(self, account_id: str, name: str = None, cash: float = None) -> Optional[Dict]:
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if not acc:
                    return None
                if name is not None:
                    acc.name = name
                if cash is not None:
                    acc.cash = cash
                    acc.initial_cash = cash  # 同步初始值，方便 reset
                db.commit()
                return self._account_to_dict(db, acc)
            finally:
                db.close()

    def delete_account(self, account_id: str) -> bool:
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if not acc:
                    return False
                db.query(Position).filter_by(account_id=account_id).delete()
                db.delete(acc)
                db.commit()
                return True
            finally:
                db.close()

    def reset_account(self, account_id: str) -> Optional[Dict]:
        """恢复到初始现金，清空持仓"""
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if not acc:
                    return None
                acc.cash = acc.initial_cash
                db.query(Position).filter_by(account_id=account_id).delete()
                db.commit()
                return self._account_to_dict(db, acc)
            finally:
                db.close()

    # ── 资金操作 ─────────────────────────────────────────────

    def deduct_cash(self, account_id: str, amount: float):
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if acc:
                    acc.cash -= amount
                    db.commit()
            finally:
                db.close()

    def add_cash(self, account_id: str, amount: float):
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if acc:
                    acc.cash += amount
                    db.commit()
            finally:
                db.close()

    def get_cash(self, account_id: str) -> float:
        db = self._session_factory()
        try:
            acc = db.query(Account).filter_by(id=account_id).first()
            return acc.cash if acc else 0.0
        finally:
            db.close()

    # ── 持仓操作 ─────────────────────────────────────────────

    def update_position_on_trade(self, account_id: str, symbol: str, direction: str, quantity: int, price: float):
        """成交后更新持仓（买入增加、卖出减少）"""
        with self._lock:
            db = self._session_factory()
            try:
                pos = db.query(Position).filter_by(account_id=account_id, symbol=symbol).first()
                if direction == "buy":
                    if pos:
                        # 加权平均成本价
                        total_qty = pos.quantity + quantity
                        pos.cost_price = (pos.cost_price * pos.quantity + price * quantity) / total_qty
                        pos.quantity = total_qty
                        pos.available_qty += quantity
                    else:
                        db.add(Position(
                            account_id=account_id,
                            symbol=symbol,
                            quantity=quantity,
                            available_qty=quantity,
                            cost_price=price,
                        ))
                elif direction == "sell" and pos:
                    pos.quantity -= quantity
                    pos.available_qty -= quantity
                    if pos.quantity <= 0:
                        db.delete(pos)
                db.commit()
            finally:
                db.close()

    def get_account_config(self, account_id: str) -> dict:
        """获取账号场景配置，无则返回空 dict（调用方用默认值）"""
        db = self._session_factory()
        try:
            acc = db.query(Account).filter_by(id=account_id).first()
            if not acc or not acc.scenario_config:
                return {}
            return json.loads(acc.scenario_config)
        except Exception:
            return {}
        finally:
            db.close()

    def set_account_config(self, account_id: str, config: dict) -> bool:
        """保存账号场景配置"""
        with self._lock:
            db = self._session_factory()
            try:
                acc = db.query(Account).filter_by(id=account_id).first()
                if not acc:
                    return False
                acc.scenario_config = json.dumps(config, ensure_ascii=False)
                db.commit()
                return True
            finally:
                db.close()

    # ── 内部工具 ─────────────────────────────────────────────

    def _account_to_dict(self, db, acc: Account) -> Dict:
        positions = db.query(Position).filter_by(account_id=acc.id).all()
        try:
            cfg = json.loads(acc.scenario_config) if acc.scenario_config else {}
        except Exception:
            cfg = {}
        return {
            "id": acc.id,
            "name": acc.name,
            "initial_cash": acc.initial_cash,
            "cash": acc.cash,
            "scenario_config": cfg,
            "created_at": acc.created_at.isoformat() if acc.created_at else None,
            "positions": [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "available_qty": p.available_qty,
                    "cost_price": p.cost_price,
                }
                for p in positions
            ],
        }

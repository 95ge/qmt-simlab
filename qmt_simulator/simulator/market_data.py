"""In-memory market data snapshots used by quote-driven simulation."""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _positive_float(value: Any, field: str) -> Optional[float]:
    if value is None or value == "":
        return None
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field} must be greater than 0")
    return number


def _positive_int(value: Any, field: str) -> Optional[int]:
    if value is None or value == "":
        return None
    number = int(value)
    if number < 0 or number != float(value):
        raise ValueError(f"{field} must be greater than or equal to 0")
    return number


@dataclass(frozen=True)
class MarketQuote:
    """Normalized top-of-book quote.

    ``ts`` is an epoch timestamp in milliseconds.  Bid/ask and size fields are
    optional so a last-price-only feed can still be used for valuation and
    algorithm price discovery.
    """

    symbol: str
    ts: int
    last_price: float
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_qty: Optional[int] = None
    ask_qty: Optional[int] = None
    last_qty: Optional[int] = None
    cum_volume: Optional[int] = None
    trading_status: str = "open"
    seq: Optional[int] = None
    source: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MarketQuote":
        symbol = str(data.get("symbol") or "").strip().upper()
        if not symbol:
            raise ValueError("symbol is required")

        ts = data.get("ts", data.get("timestamp"))
        if ts is None:
            ts = int(time.time() * 1000)
        if isinstance(ts, bool) or int(ts) != float(ts):
            raise ValueError("ts must be an integer epoch timestamp in milliseconds")
        ts = int(ts)
        if ts <= 0 or ts > int(time.time() * 1000) + 30_000:
            raise ValueError("ts must be positive and no more than 30 seconds in the future")

        last_price = _positive_float(data.get("last_price", data.get("price")), "last_price")
        if last_price is None:
            raise ValueError("last_price is required")

        bid_price = _positive_float(data.get("bid_price", data.get("bid")), "bid_price")
        ask_price = _positive_float(data.get("ask_price", data.get("ask")), "ask_price")
        if bid_price is not None and ask_price is not None and bid_price > ask_price:
            raise ValueError("bid_price cannot be greater than ask_price")
        status = str(data.get("trading_status") or "open")
        if status not in ("open", "closed", "halted"):
            raise ValueError("trading_status must be open, closed, or halted")

        return cls(
            symbol=symbol,
            ts=ts,
            last_price=last_price,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_qty=_positive_int(data.get("bid_qty", data.get("bid_volume")), "bid_qty"),
            ask_qty=_positive_int(data.get("ask_qty", data.get("ask_volume")), "ask_qty"),
            last_qty=_positive_int(data.get("last_qty", data.get("volume")), "last_qty"),
            cum_volume=_positive_int(data.get("cum_volume"), "cum_volume"),
            trading_status=status,
            seq=_positive_int(data.get("seq"), "seq"),
            source=str(data.get("source") or ""),
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def executable_price(self, direction: str) -> float:
        """Return the best executable price, falling back to last price."""
        if direction == "buy":
            return self.ask_price or self.last_price
        if direction == "sell":
            return self.bid_price or self.last_price
        return self.last_price

    def available_qty(self, direction: str) -> Optional[int]:
        """Return available opposite-side size, if the feed supplied it."""
        return self.ask_qty if direction == "buy" else self.bid_qty


QuoteListener = Callable[[MarketQuote], None]


class QuoteStore:
    """Thread-safe latest-quote store with update listeners.

    Ticks are intentionally kept in memory.  SQLite remains responsible for
    orders and trades; callers that need replay can persist the normalized
    stream separately without adding write pressure to the simulator.
    """

    def __init__(self, retention_ms: int = 3_600_000, max_symbols: int = 10_000):
        if retention_ms <= 0 or max_symbols <= 0:
            raise ValueError("retention_ms and max_symbols must be positive")
        self.retention_ms = retention_ms
        self.max_symbols = max_symbols
        self._quotes: Dict[str, MarketQuote] = OrderedDict()
        self._received: Dict[str, tuple] = {}
        self._eviction_listeners = []
        self._accepted_count = 0
        self._ignored_count = 0
        self._expired_count = 0
        self._evicted_count = 0
        self._last_received_at = None
        self._last_received_mono = None
        self._last_accepted_at = None
        self._last_cleanup_at = None
        self.cleanup_interval_seconds = 60
        self._stop_cleanup = threading.Event()
        self._cleanup_thread = None
        self._listeners: List[QuoteListener] = []
        self._lock = threading.RLock()
        self._publish_lock = threading.RLock()

    def subscribe_evictions(self, listener) -> None:
        with self._lock:
            self._eviction_listeners.append(listener)

    def _notify_evictions(self, removed):
        if not removed:
            return
        with self._lock:
            listeners = list(self._eviction_listeners)
        # Never hold the cache lock while acquiring a consumer's lock.
        for listener in listeners:
            listener(removed)

    def cleanup(self) -> int:
        """Remove idle snapshots by receipt time, without touching the ledger."""
        now = time.monotonic()
        with self._lock:
            removed = []
            # Snapshots are ordered by the last accepted update, so routine
            # reads and publishes only inspect the oldest entry.
            while self._quotes:
                symbol = next(iter(self._quotes))
                if (now - self._received[symbol][1]) * 1000 < self.retention_ms:
                    break
                removed.append(self._quotes.pop(symbol))
                self._received.pop(symbol)
            self._expired_count += len(removed)
            self._last_cleanup_at = int(time.time() * 1000)
        self._notify_evictions(removed)
        return len(removed)

    def start_cleanup(self, interval_seconds: float = 60) -> None:
        if interval_seconds <= 0:
            raise ValueError("cleanup interval must be positive")
        with self._lock:
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                return
            self.cleanup_interval_seconds = interval_seconds
            self._stop_cleanup.clear()
            self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True,
                                                    name="quote-cache-cleanup")
            self._cleanup_thread.start()

    def _cleanup_loop(self):
        while not self._stop_cleanup.wait(self.cleanup_interval_seconds):
            self.cleanup()

    def stop_cleanup(self):
        self._stop_cleanup.set()
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)

    def subscribe(self, listener: QuoteListener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def publish(self, data: MarketQuote | Dict[str, Any]) -> Tuple[MarketQuote, bool]:
        with self._publish_lock:
            return self._publish(data)

    def _publish(self, data: MarketQuote | Dict[str, Any]) -> Tuple[MarketQuote, bool]:
        quote = data if isinstance(data, MarketQuote) else MarketQuote.from_dict(data)
        self.cleanup()
        removed = []
        with self._lock:
            now = int(time.time() * 1000)
            mono = time.monotonic()
            self._last_received_at = now
            self._last_received_mono = mono
            previous = self._quotes.get(quote.symbol)
            if previous is not None:
                if quote.ts < previous.ts:
                    self._ignored_count += 1
                    return previous, False
                if (quote.ts == previous.ts and (quote.seq is None or previous.seq is None
                                                or quote.seq <= previous.seq)):
                    self._ignored_count += 1
                    return previous, False
                if (quote.seq is not None and previous.seq is not None and quote.seq <= previous.seq
                        and quote.ts // 86_400_000 == previous.ts // 86_400_000):
                    self._ignored_count += 1
                    return previous, False
            self._quotes.pop(quote.symbol, None)
            if previous is not None:
                removed.append(previous)
            self._quotes[quote.symbol] = quote
            self._received[quote.symbol] = (now, mono)
            self._accepted_count += 1
            self._last_accepted_at = now
            while len(self._quotes) > self.max_symbols:
                symbol, old = self._quotes.popitem(last=False)
                self._received.pop(symbol, None)
                removed.append(old)
                self._evicted_count += 1
            listeners = list(self._listeners)

        self._notify_evictions(removed)
        for listener in listeners:
            try:
                listener(quote)
            except Exception:
                logger.exception("market quote listener failed for %s", quote.symbol)
        return quote, True

    def publish_many(self, items: Iterable[MarketQuote | Dict[str, Any]]) -> Tuple[List[MarketQuote], int]:
        quotes: List[MarketQuote] = []
        accepted = 0
        for item in items:
            quote, was_accepted = self.publish(item)
            quotes.append(quote)
            accepted += int(was_accepted)
        return quotes, accepted

    def update(self, data: MarketQuote | Dict[str, Any]) -> MarketQuote:
        """Compatibility helper returning the normalized latest quote."""
        quote, _ = self.publish(data)
        return quote

    def get(self, symbol: str, max_age_ms: Optional[int] = None) -> Optional[MarketQuote]:
        self.cleanup()
        with self._lock:
            quote = self._quotes.get(str(symbol).strip().upper())
        if quote is None or max_age_ms is None:
            return quote
        age = int(time.time() * 1000) - quote.ts
        return quote if age <= max_age_ms else None

    def all(self) -> List[MarketQuote]:
        self.cleanup()
        with self._lock:
            return list(self._quotes.values())

    def clear(self) -> None:
        with self._lock:
            removed = list(self._quotes.values())
            self._quotes.clear()
            self._received.clear()
        self._notify_evictions(removed)

    def snapshots(self, max_age_ms: int = 3000, symbol: Optional[str] = None) -> List[dict]:
        self.cleanup()
        now, mono = int(time.time() * 1000), time.monotonic()
        with self._lock:
            quotes = (self._quotes.values() if symbol is None else
                      ([self._quotes[symbol]] if symbol in self._quotes else []))
            return [{**q.to_dict(), "age_ms": max(0, now - q.ts),
                     "stale": now - q.ts > max_age_ms,
                     "received_at": self._received[q.symbol][0],
                     "received_age_ms": max(0, int((mono - self._received[q.symbol][1]) * 1000))}
                    for q in quotes]

    def status(self, max_age_ms: int = 3000) -> dict:
        self.cleanup()
        now = int(time.time() * 1000)
        with self._lock:
            age = (max(0, int((time.monotonic() - self._last_received_mono) * 1000))
                   if self._last_received_mono is not None else None)
            fresh = sum(now - q.ts <= max_age_ms for q in self._quotes.values())
            return {
                "storage": "memory", "persistent": False, "symbol_count": len(self._quotes),
                "fresh_count": fresh, "stale_count": len(self._quotes) - fresh,
                "received_count": self._accepted_count + self._ignored_count,
                "accepted_count": self._accepted_count, "ignored_count": self._ignored_count,
                "last_received_at": self._last_received_at, "last_accepted_at": self._last_accepted_at,
                "last_received_age_ms": age,
                "feed_status": "never" if age is None else ("recent" if age <= max_age_ms else "idle"),
                "retention_ms": self.retention_ms, "max_symbols": self.max_symbols,
                "expired_count": self._expired_count, "evicted_count": self._evicted_count,
                "last_cleanup_at": self._last_cleanup_at,
                "cleanup_interval_seconds": self.cleanup_interval_seconds,
                "max_age_ms": max_age_ms,
            }

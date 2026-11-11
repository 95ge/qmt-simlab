"""场景配置：控制订单随机结果概率和成交延迟，以及手续费设置"""
import threading
from dataclasses import dataclass, asdict


@dataclass
class ScenarioConfig:
    # 概率（必须合计 1.0）
    filled_prob: float = 0.60
    partial_filled_prob: float = 0.20
    rejected_prob: float = 0.10
    canceled_prob: float = 0.10
    # 延迟（秒）
    initial_delay: float = 5.0
    batch_interval_min: float = 1.0
    batch_interval_max: float = 2.0
    batch_count_min: int = 2
    batch_count_max: int = 4

    def __post_init__(self):
        self._lock = threading.Lock()

    def update_probs(self, filled: float, partial_filled: float, rejected: float, canceled: float):
        total = filled + partial_filled + rejected + canceled
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"概率之和必须为 1.0，当前为 {total:.4f}")
        with self._lock:
            self.filled_prob = filled
            self.partial_filled_prob = partial_filled
            self.rejected_prob = rejected
            self.canceled_prob = canceled

    def update_delay(
        self,
        initial_delay: float = None,
        batch_interval_min: float = None,
        batch_interval_max: float = None,
        batch_count_min: int = None,
        batch_count_max: int = None,
    ):
        with self._lock:
            if initial_delay is not None:
                self.initial_delay = initial_delay
            if batch_interval_min is not None:
                self.batch_interval_min = batch_interval_min
            if batch_interval_max is not None:
                self.batch_interval_max = batch_interval_max
            if batch_count_min is not None:
                self.batch_count_min = batch_count_min
            if batch_count_max is not None:
                self.batch_count_max = batch_count_max

    def to_dict(self) -> dict:
        # ``dataclasses.asdict`` deep-copies every field and cannot copy a
        # ``threading.Lock``.  The order engine reads this method from a
        # worker thread, so build the serialisable snapshot explicitly.
        with self._lock:
            return {
                "filled_prob": self.filled_prob,
                "partial_filled_prob": self.partial_filled_prob,
                "rejected_prob": self.rejected_prob,
                "canceled_prob": self.canceled_prob,
                "initial_delay": self.initial_delay,
                "batch_interval_min": self.batch_interval_min,
                "batch_interval_max": self.batch_interval_max,
                "batch_count_min": self.batch_count_min,
                "batch_count_max": self.batch_count_max,
            }


@dataclass
class CommissionConfig:
    """手续费配置（A股典型费率）"""
    # 是否启用手续费
    enabled: bool = True
    # 佣金费率（买卖双向），默认万2.5
    commission_rate: float = 0.00025
    # 最低佣金（元），默认5元
    min_commission: float = 5.0
    # 印花税率（仅卖出，A股千1）
    stamp_duty_rate: float = 0.001
    # 过户费率（买卖双向，沪市万0.2，深市可设为0）
    transfer_fee_rate: float = 0.00002

    def __post_init__(self):
        self._lock = threading.Lock()

    def calculate(self, direction: str, amount: float) -> float:
        """根据方向和成交金额计算手续费（元）"""
        if not self.enabled or amount <= 0:
            return 0.0
        with self._lock:
            # 佣金（买卖双向，最低 min_commission）
            commission = max(amount * self.commission_rate, self.min_commission)
            # 印花税（仅卖出）
            stamp_duty = amount * self.stamp_duty_rate if direction == "sell" else 0.0
            # 过户费（买卖双向）
            transfer_fee = amount * self.transfer_fee_rate
        return round(commission + stamp_duty + transfer_fee, 4)

    def update(
        self,
        enabled: bool = None,
        commission_rate: float = None,
        min_commission: float = None,
        stamp_duty_rate: float = None,
        transfer_fee_rate: float = None,
    ):
        with self._lock:
            if enabled is not None:
                self.enabled = enabled
            if commission_rate is not None:
                if commission_rate < 0:
                    raise ValueError("佣金费率不能为负")
                self.commission_rate = commission_rate
            if min_commission is not None:
                if min_commission < 0:
                    raise ValueError("最低佣金不能为负")
                self.min_commission = min_commission
            if stamp_duty_rate is not None:
                if stamp_duty_rate < 0:
                    raise ValueError("印花税率不能为负")
                self.stamp_duty_rate = stamp_duty_rate
            if transfer_fee_rate is not None:
                if transfer_fee_rate < 0:
                    raise ValueError("过户费率不能为负")
                self.transfer_fee_rate = transfer_fee_rate

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "commission_rate": self.commission_rate,
            "min_commission": self.min_commission,
            "stamp_duty_rate": self.stamp_duty_rate,
            "transfer_fee_rate": self.transfer_fee_rate,
        }


# 全局单例
scenario = ScenarioConfig()
commission = CommissionConfig()


def get_default_config_dict() -> dict:
    """返回全局默认配置的字典（作为账号未设置时的 fallback）"""
    d = scenario.to_dict()
    d["commission"] = commission.to_dict()
    d["execution_mode"] = "random"
    d["quote_max_age_ms"] = 3000
    d["quote_fallback"] = "pending"
    d["use_depth"] = True
    return d

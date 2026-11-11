"""Python SDK for the standalone qmt-simlab simulator."""

from .client import SimulationAPIError, SimulationClient
from .models import (
    Account,
    Balance,
    Order,
    OrderResult,
    Position,
    Trade,
)
from .xtquant_compat import (
    StockAccount,
    XtAccountStatus,
    XtAsset,
    XtCancelError,
    XtCancelOrderResponse,
    XtOrder,
    XtOrderError,
    XtOrderResponse,
    XtPosition,
    XtQuantTrader,
    XtQuantTraderCallback,
    XtTrade,
    xtconstant,
)

__version__ = "0.3.0"

__all__ = [
    "Account",
    "Balance",
    "Order",
    "OrderResult",
    "Position",
    "SimulationAPIError",
    "SimulationClient",
    "StockAccount",
    "Trade",
    "XtAccountStatus",
    "XtAsset",
    "XtCancelError",
    "XtCancelOrderResponse",
    "XtOrder",
    "XtOrderError",
    "XtOrderResponse",
    "XtPosition",
    "XtQuantTrader",
    "XtQuantTraderCallback",
    "XtTrade",
    "xtconstant",
    "__version__",
]


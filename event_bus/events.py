"""Canonical event enums for the hummingbot ecosystem.

These are the canonical definitions, mirroring ``hummingbot/core/event/events.py``.
Names and values must stay identical to the parent definitions; consumers rely on
them. This module must never import hummingbot (L0 package).
"""

from enum import Enum

__all__ = ["MarketEvent", "OrderBookEvent"]


class MarketEvent(Enum):
    ReceivedAsset = 101
    BuyOrderCompleted = 102
    SellOrderCompleted = 103
    # Trade = 104  Deprecated
    WithdrawAsset = 105  # Locally Deprecated, but still present in hummingsim
    OrderCancelled = 106
    OrderFilled = 107
    OrderExpired = 108
    OrderUpdate = 109
    TradeUpdate = 110
    OrderFailure = 198
    TransactionFailure = 199
    BuyOrderCreated = 200
    SellOrderCreated = 201
    FundingPaymentCompleted = 202
    FundingInfo = 203
    RangePositionLiquidityAdded = 300
    RangePositionLiquidityRemoved = 301
    RangePositionUpdateFailure = 303


class OrderBookEvent(int, Enum):
    TradeEvent = 901
    OrderBookDataSourceUpdateEvent = 904

"""Pin the canonical MarketEvent / OrderBookEvent definitions."""

from enum import Enum

import event_bus
import event_bus.events
from event_bus import MarketEvent, OrderBookEvent

MARKET_EVENT_MEMBERS = [
    ("ReceivedAsset", 101),
    ("BuyOrderCompleted", 102),
    ("SellOrderCompleted", 103),
    ("WithdrawAsset", 105),
    ("OrderCancelled", 106),
    ("OrderFilled", 107),
    ("OrderExpired", 108),
    ("OrderUpdate", 109),
    ("TradeUpdate", 110),
    ("OrderFailure", 198),
    ("TransactionFailure", 199),
    ("BuyOrderCreated", 200),
    ("SellOrderCreated", 201),
    ("FundingPaymentCompleted", 202),
    ("FundingInfo", 203),
    ("RangePositionLiquidityAdded", 300),
    ("RangePositionLiquidityRemoved", 301),
    ("RangePositionUpdateFailure", 303),
]

ORDER_BOOK_EVENT_MEMBERS = [
    ("TradeEvent", 901),
    ("OrderBookDataSourceUpdateEvent", 904),
]


def test_reexported_from_package() -> None:
    assert event_bus.events.MarketEvent is event_bus.MarketEvent
    assert event_bus.events.OrderBookEvent is event_bus.OrderBookEvent


def test_bases() -> None:
    assert OrderBookEvent.__bases__ == (int, Enum)
    assert MarketEvent.__bases__ == (Enum,)


def test_market_event_members() -> None:
    assert [(m.name, m.value) for m in MarketEvent] == MARKET_EVENT_MEMBERS


def test_order_book_event_members() -> None:
    assert [(m.name, m.value) for m in OrderBookEvent] == ORDER_BOOK_EVENT_MEMBERS


def test_int_semantics() -> None:
    assert issubclass(OrderBookEvent, int)
    assert OrderBookEvent.TradeEvent == 901
    assert not issubclass(MarketEvent, int)
    assert MarketEvent.OrderFilled != 107


def test_in_all() -> None:
    assert "MarketEvent" in event_bus.__all__
    assert "OrderBookEvent" in event_bus.__all__

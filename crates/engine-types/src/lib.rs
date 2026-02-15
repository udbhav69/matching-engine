//! Domain types for the matching engine.
//!
//! All prices are represented as integer ticks and all quantities as integer lots.
//! This eliminates floating-point non-determinism across platforms.

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Scalar newtypes
// ---------------------------------------------------------------------------

/// Price in integer ticks. A tick is the smallest price increment.
/// `Price(0)` is reserved as "market" (no price limit).
pub type Price = u64;

/// Quantity in integer lots.
pub type Qty = u64;

/// Monotonically increasing order identifier, unique per partition.
pub type OrderId = u64;

/// Global sequence number stamped by the engine on every inbound command.
pub type SeqNo = u64;

/// Symbol identifier (interned to u32 for cache-friendliness).
pub type Symbol = u32;

/// Timestamp in nanoseconds since epoch. Used for logging/audit only;
/// matching is strictly sequenced by `SeqNo`, not wall-clock time.
pub type Timestamp = u64;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "Buy"),
            Side::Sell => write!(f, "Sell"),
        }
    }
}

/// Time-in-force qualifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good-til-cancel: rests on book until filled or explicitly canceled.
    GTC,
    /// Immediate-or-cancel: fill what you can, cancel the rest.
    IOC,
    /// Fill-or-kill: fill entirely or reject.
    FOK,
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Limit order with explicit price.
    Limit,
    /// Market order — matches at any price. Never rests on book.
    Market,
}

/// Condition flags (combinable as needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderCondition {
    /// No special condition.
    None,
    /// All-or-none: fill entirely in a single match cycle or rest/reject.
    AON,
    /// Stop order: activated when the market trades at or through the stop price.
    Stop,
}

// ---------------------------------------------------------------------------
// Inbound commands
// ---------------------------------------------------------------------------

/// A new order submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub condition: OrderCondition,
    pub price: Price,
    pub qty: Qty,
    /// For stop orders: the trigger price.
    pub stop_price: Price,
}

/// Cancel request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrder {
    pub order_id: OrderId,
    pub symbol: Symbol,
}

/// Replace (amend) request — can change price and/or quantity.
/// Quantity can only be reduced (partial cancel) or the order loses priority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceOrder {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub new_price: Price,
    pub new_qty: Qty,
}

/// Envelope for all inbound commands, stamped with a sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub seq_no: SeqNo,
    pub timestamp: Timestamp,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandPayload {
    New(NewOrder),
    Cancel(CancelOrder),
    Replace(ReplaceOrder),
}

// ---------------------------------------------------------------------------
// Outbound events
// ---------------------------------------------------------------------------

/// A single fill between an aggressor and a resting order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub qty: Qty,
    pub maker_filled_completely: bool,
    pub taker_filled_completely: bool,
}

/// Reason an order or action was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// Order ID not found on book.
    UnknownOrder,
    /// Duplicate order ID.
    DuplicateOrderId,
    /// Invalid price (e.g. zero price on limit order).
    InvalidPrice,
    /// Invalid quantity (zero).
    InvalidQuantity,
    /// FOK could not be fully filled.
    FOKNotFillable,
    /// AON could not be fully filled.
    AONNotFillable,
    /// Replace would increase quantity (not allowed without losing priority).
    ReplaceQtyIncrease,
    /// No resting quantity to cancel.
    NothingToCancel,
    /// Market order with no liquidity on opposite side.
    NoLiquidity,
    /// Symbol not found.
    UnknownSymbol,
    /// Book is in an invalid state.
    InternalError,
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Best bid and offer snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bbo {
    pub bid_price: Option<Price>,
    pub bid_qty: Qty,
    pub ask_price: Option<Price>,
    pub ask_qty: Qty,
}

/// A single price level in the depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price: Price,
    pub qty: Qty,
    pub order_count: u32,
}

/// All events the engine can emit for a single command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineEvent {
    OrderAccepted {
        seq_no: SeqNo,
        order_id: OrderId,
    },
    OrderRejected {
        seq_no: SeqNo,
        order_id: OrderId,
        reason: RejectReason,
    },
    OrderCanceled {
        seq_no: SeqNo,
        order_id: OrderId,
        remaining_qty: Qty,
    },
    CancelRejected {
        seq_no: SeqNo,
        order_id: OrderId,
        reason: RejectReason,
    },
    OrderReplaced {
        seq_no: SeqNo,
        order_id: OrderId,
        new_price: Price,
        new_qty: Qty,
    },
    ReplaceRejected {
        seq_no: SeqNo,
        order_id: OrderId,
        reason: RejectReason,
    },
    Trade(Fill),
    BboChanged(Bbo),
    DepthChanged {
        side: Side,
        levels: Vec<DepthLevel>,
    },
}

// ---------------------------------------------------------------------------
// Order tracker — used inside the book
// ---------------------------------------------------------------------------

/// Tracks the lifecycle state of an order on the book.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderState {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub condition: OrderCondition,
    pub price: Price,
    pub original_qty: Qty,
    pub filled_qty: Qty,
    pub stop_price: Price,
    /// True if the order is currently resting on the book.
    pub is_open: bool,
}

impl OrderState {
    /// Remaining quantity available for matching.
    #[inline]
    pub fn open_qty(&self) -> Qty {
        self.original_qty.saturating_sub(self.filled_qty)
    }

    /// Create from a NewOrder command.
    pub fn from_new_order(order: &NewOrder) -> Self {
        Self {
            order_id: order.order_id,
            symbol: order.symbol,
            side: order.side,
            order_type: order.order_type,
            tif: order.tif,
            condition: order.condition,
            price: order.price,
            original_qty: order.qty,
            filled_qty: 0,
            stop_price: order.stop_price,
            is_open: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Sentinel price meaning "no price limit" (market orders).
pub const MARKET_PRICE: Price = 0;

/// Maximum depth levels to track for market data.
pub const MAX_DEPTH_LEVELS: usize = 10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_opposite() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
    }

    #[test]
    fn order_state_open_qty() {
        let state = OrderState {
            order_id: 1,
            symbol: 0,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::GTC,
            condition: OrderCondition::None,
            price: 100,
            original_qty: 50,
            filled_qty: 20,
            stop_price: 0,
            is_open: true,
        };
        assert_eq!(state.open_qty(), 30);
    }

    #[test]
    fn order_state_open_qty_saturates() {
        let state = OrderState {
            order_id: 1,
            symbol: 0,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::GTC,
            condition: OrderCondition::None,
            price: 100,
            original_qty: 10,
            filled_qty: 15, // should not happen, but must not panic
            stop_price: 0,
            is_open: true,
        };
        assert_eq!(state.open_qty(), 0);
    }

    #[test]
    fn command_serialization_roundtrip() {
        let cmd = Command {
            seq_no: 1,
            timestamp: 1000,
            payload: CommandPayload::New(NewOrder {
                order_id: 42,
                symbol: 1,
                side: Side::Buy,
                order_type: OrderType::Limit,
                tif: TimeInForce::GTC,
                condition: OrderCondition::None,
                price: 10050,
                qty: 100,
                stop_price: 0,
            }),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }
}

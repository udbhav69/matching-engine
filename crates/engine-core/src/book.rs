//! Order book: price-level management and order storage.
//!
//! The book maintains two sides (bids and asks) as sorted maps of price levels.
//! Each price level is a FIFO queue of order IDs. Order state is stored in a
//! flat HashMap for O(1) lookup by OrderId.

use engine_types::*;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// A single price level: a FIFO queue of order IDs and aggregate quantity.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PriceLevel {
    pub orders: VecDeque<OrderId>,
    pub total_qty: Qty,
}

/// One side of the order book.
///
/// For bids: stored in a BTreeMap with *negated* keys so that iteration
/// gives best (highest) bid first.
/// For asks: stored in a BTreeMap with natural ordering so iteration
/// gives best (lowest) ask first.
///
/// We use a unified representation: the `levels` map always stores keys
/// such that `levels.iter()` yields best-to-worst.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BookSide {
    /// For bids: key = `u64::MAX - price` (so highest price sorts first).
    /// For asks: key = price.
    pub levels: BTreeMap<u64, PriceLevel>,
    pub side: SideTag,
}

/// Tag to remember which side this is, needed for key encoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SideTag {
    #[default]
    Bid,
    Ask,
}

impl BookSide {
    pub fn new_bids() -> Self {
        Self {
            levels: BTreeMap::new(),
            side: SideTag::Bid,
        }
    }

    pub fn new_asks() -> Self {
        Self {
            levels: BTreeMap::new(),
            side: SideTag::Ask,
        }
    }

    /// Encode a price into the sort key for this side.
    #[inline]
    pub fn key(&self, price: Price) -> u64 {
        match self.side {
            SideTag::Bid => u64::MAX - price,
            SideTag::Ask => price,
        }
    }

    /// Decode a sort key back to the actual price.
    #[inline]
    pub fn price_from_key(&self, key: u64) -> Price {
        match self.side {
            SideTag::Bid => u64::MAX - key,
            SideTag::Ask => key,
        }
    }

    /// Insert an order at the back of its price level (FIFO).
    pub fn insert(&mut self, price: Price, order_id: OrderId, qty: Qty) {
        let key = self.key(price);
        let level = self.levels.entry(key).or_default();
        level.orders.push_back(order_id);
        level.total_qty += qty;
    }

    /// Remove a specific order from a price level. Returns the quantity
    /// that was attributed to this order (looked up from `states`).
    pub fn remove(
        &mut self,
        price: Price,
        order_id: OrderId,
        qty: Qty,
    ) -> bool {
        let key = self.key(price);
        if let Some(level) = self.levels.get_mut(&key) {
            if let Some(pos) = level.orders.iter().position(|&id| id == order_id) {
                level.orders.remove(pos);
                level.total_qty = level.total_qty.saturating_sub(qty);
                if level.orders.is_empty() {
                    self.levels.remove(&key);
                }
                return true;
            }
        }
        false
    }

    /// Reduce the aggregate quantity at a price level (after a partial fill
    /// of the front order).
    pub fn reduce_qty(&mut self, price: Price, delta: Qty) {
        let key = self.key(price);
        if let Some(level) = self.levels.get_mut(&key) {
            level.total_qty = level.total_qty.saturating_sub(delta);
        }
    }

    /// Pop the front order at the best price level.
    /// Returns `None` if this side is empty.
    pub fn pop_front(&mut self) -> Option<(Price, OrderId)> {
        let &key = self.levels.keys().next()?;
        let level = self.levels.get_mut(&key)?;
        let order_id = level.orders.pop_front()?;
        let empty = level.orders.is_empty();
        let price = self.price_from_key(key);
        if empty {
            self.levels.remove(&key);
        }
        Some((price, order_id))
    }

    /// Peek at the best price level without modifying the book.
    pub fn best(&self) -> Option<(Price, &PriceLevel)> {
        let (&key, level) = self.levels.iter().next()?;
        Some((self.price_from_key(key), level))
    }

    /// Check if the side is empty.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Number of distinct price levels.
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Total quantity across all levels.
    pub fn total_qty(&self) -> Qty {
        self.levels.values().map(|l| l.total_qty).sum()
    }

    /// Get top N depth levels.
    pub fn depth(&self, n: usize) -> Vec<DepthLevel> {
        self.levels
            .iter()
            .take(n)
            .map(|(&key, level)| DepthLevel {
                price: self.price_from_key(key),
                qty: level.total_qty,
                order_count: level.orders.len() as u32,
            })
            .collect()
    }
}

/// The full order book for a single symbol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OrderBook {
    pub symbol: Symbol,
    pub bids: BookSide,
    pub asks: BookSide,
    /// All orders (open and recently closed) tracked by ID.
    pub orders: HashMap<OrderId, OrderState>,
    /// The last sequence number processed.
    pub last_seq_no: SeqNo,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BookSide::new_bids(),
            asks: BookSide::new_asks(),
            orders: HashMap::new(),
            last_seq_no: 0,
        }
    }

    /// Get the book side for a given order side.
    pub fn side_mut(&mut self, side: Side) -> &mut BookSide {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Get the opposite book side (where resting orders are matched against).
    pub fn opposite_side_mut(&mut self, side: Side) -> &mut BookSide {
        match side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        }
    }

    pub fn opposite_side(&self, side: Side) -> &BookSide {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }

    /// Current BBO snapshot.
    pub fn bbo(&self) -> Bbo {
        let (bid_price, bid_qty) = self
            .bids
            .best()
            .map(|(p, l)| (Some(p), l.total_qty))
            .unwrap_or((None, 0));
        let (ask_price, ask_qty) = self
            .asks
            .best()
            .map(|(p, l)| (Some(p), l.total_qty))
            .unwrap_or((None, 0));
        Bbo {
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        }
    }

    /// Check the book invariant: best bid must be strictly less than best ask.
    pub fn is_uncrossed(&self) -> bool {
        match (self.bids.best(), self.asks.best()) {
            (Some((bid, _)), Some((ask, _))) => bid < ask,
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bid_side_ordering() {
        let mut bids = BookSide::new_bids();
        bids.insert(100, 1, 10);
        bids.insert(102, 2, 20);
        bids.insert(99, 3, 5);

        // Best bid should be 102 (highest price)
        let (price, level) = bids.best().unwrap();
        assert_eq!(price, 102);
        assert_eq!(level.total_qty, 20);
    }

    #[test]
    fn ask_side_ordering() {
        let mut asks = BookSide::new_asks();
        asks.insert(100, 1, 10);
        asks.insert(98, 2, 20);
        asks.insert(105, 3, 5);

        // Best ask should be 98 (lowest price)
        let (price, level) = asks.best().unwrap();
        assert_eq!(price, 98);
        assert_eq!(level.total_qty, 20);
    }

    #[test]
    fn fifo_within_price_level() {
        let mut asks = BookSide::new_asks();
        asks.insert(100, 1, 10);
        asks.insert(100, 2, 20);
        asks.insert(100, 3, 5);

        let (_, level) = asks.best().unwrap();
        assert_eq!(level.orders.len(), 3);
        assert_eq!(level.orders[0], 1); // first in
        assert_eq!(level.orders[1], 2);
        assert_eq!(level.orders[2], 3);
    }

    #[test]
    fn remove_order() {
        let mut bids = BookSide::new_bids();
        bids.insert(100, 1, 10);
        bids.insert(100, 2, 20);

        assert!(bids.remove(100, 1, 10));
        let (_, level) = bids.best().unwrap();
        assert_eq!(level.orders.len(), 1);
        assert_eq!(level.orders[0], 2);
        assert_eq!(level.total_qty, 20);
    }

    #[test]
    fn remove_last_at_level_cleans_up() {
        let mut bids = BookSide::new_bids();
        bids.insert(100, 1, 10);
        assert!(bids.remove(100, 1, 10));
        assert!(bids.is_empty());
    }

    #[test]
    fn depth_snapshot() {
        let mut asks = BookSide::new_asks();
        asks.insert(100, 1, 10);
        asks.insert(100, 2, 5);
        asks.insert(101, 3, 20);
        asks.insert(102, 4, 7);

        let depth = asks.depth(2);
        assert_eq!(depth.len(), 2);
        assert_eq!(depth[0].price, 100);
        assert_eq!(depth[0].qty, 15);
        assert_eq!(depth[0].order_count, 2);
        assert_eq!(depth[1].price, 101);
        assert_eq!(depth[1].qty, 20);
    }

    #[test]
    fn book_uncrossed_invariant() {
        let mut book = OrderBook::new(1);
        book.bids.insert(100, 1, 10);
        book.asks.insert(101, 2, 10);
        assert!(book.is_uncrossed());

        // Crossed book
        book.asks.insert(99, 3, 10);
        assert!(!book.is_uncrossed());
    }
}

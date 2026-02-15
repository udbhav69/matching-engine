//! Market data feed: BBO, depth, and trade event aggregation.

use engine_types::*;

/// Tracks market data state and detects changes for event emission.
#[derive(Debug, Clone)]
pub struct MarketDataTracker {
    pub symbol: Symbol,
    last_bbo: Option<Bbo>,
    last_bid_depth: Vec<DepthLevel>,
    last_ask_depth: Vec<DepthLevel>,
}

impl MarketDataTracker {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            last_bbo: None,
            last_bid_depth: Vec::new(),
            last_ask_depth: Vec::new(),
        }
    }

    /// Given a new BBO, determine if it changed. Returns the BBO event if changed.
    pub fn update_bbo(&mut self, bbo: Bbo) -> Option<EngineEvent> {
        if self.last_bbo.as_ref() != Some(&bbo) {
            self.last_bbo = Some(bbo);
            Some(EngineEvent::BboChanged(bbo))
        } else {
            None
        }
    }

    /// Given new depth levels, detect changes. Returns depth events if changed.
    pub fn update_depth(
        &mut self,
        bid_depth: Vec<DepthLevel>,
        ask_depth: Vec<DepthLevel>,
    ) -> Vec<EngineEvent> {
        let mut events = Vec::new();

        if bid_depth != self.last_bid_depth {
            self.last_bid_depth = bid_depth.clone();
            events.push(EngineEvent::DepthChanged {
                side: Side::Buy,
                levels: bid_depth,
            });
        }
        if ask_depth != self.last_ask_depth {
            self.last_ask_depth = ask_depth.clone();
            events.push(EngineEvent::DepthChanged {
                side: Side::Sell,
                levels: ask_depth,
            });
        }

        events
    }

    pub fn last_bbo(&self) -> Option<&Bbo> {
        self.last_bbo.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbo_change_detected() {
        let mut tracker = MarketDataTracker::new(1);
        let bbo1 = Bbo {
            bid_price: Some(100),
            bid_qty: 10,
            ask_price: Some(101),
            ask_qty: 20,
        };
        assert!(tracker.update_bbo(bbo1).is_some());
        // Same BBO: no event.
        assert!(tracker.update_bbo(bbo1).is_none());
        // Changed BBO: event.
        let bbo2 = Bbo {
            bid_price: Some(100),
            bid_qty: 15,
            ask_price: Some(101),
            ask_qty: 20,
        };
        assert!(tracker.update_bbo(bbo2).is_some());
    }

    #[test]
    fn depth_change_detected() {
        let mut tracker = MarketDataTracker::new(1);
        let bid_depth = vec![DepthLevel { price: 100, qty: 10, order_count: 1 }];
        let ask_depth = vec![DepthLevel { price: 101, qty: 20, order_count: 2 }];
        let events = tracker.update_depth(bid_depth.clone(), ask_depth.clone());
        assert_eq!(events.len(), 2);

        // Same depth: no events.
        let events = tracker.update_depth(bid_depth, ask_depth);
        assert_eq!(events.len(), 0);
    }
}

//! Matching engine: processes commands against the order book.
//!
//! The matcher is the single-threaded core loop for one symbol partition.
//! It takes inbound [`Command`]s, executes them against the [`OrderBook`],
//! and emits a vector of [`EngineEvent`]s.
//!
//! Matching rules:
//! - Price-time priority (best price first, then FIFO within a level).
//! - Limit orders match at the resting order's price (maker price).
//! - Market orders match at whatever price is available; they never rest.
//! - IOC: fill what you can, cancel remainder.
//! - FOK: fill entirely or reject (no partial).
//! - AON: fill entirely in one match sweep or rest/reject depending on TIF.
//! - Stop orders: rest as pending until triggered by a trade at/through
//!   the stop price, then enter the book as their underlying type.

use crate::book::OrderBook;
use engine_types::*;

/// Result of processing a single command.
pub struct MatchResult {
    pub events: Vec<EngineEvent>,
}

/// The matching engine for a single symbol.
pub struct MatchingEngine {
    pub book: OrderBook,
}

impl MatchingEngine {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            book: OrderBook::new(symbol),
        }
    }

    /// Process a command and return the resulting events.
    pub fn process(&mut self, cmd: &Command) -> MatchResult {
        self.book.last_seq_no = cmd.seq_no;
        let mut events = Vec::new();

        match &cmd.payload {
            CommandPayload::New(order) => {
                self.process_new_order(cmd.seq_no, order, &mut events);
            }
            CommandPayload::Cancel(cancel) => {
                self.process_cancel(cmd.seq_no, cancel, &mut events);
            }
            CommandPayload::Replace(replace) => {
                self.process_replace(cmd.seq_no, replace, &mut events);
            }
        }

        // Emit BBO after every command that potentially changes the book.
        let bbo = self.book.bbo();
        events.push(EngineEvent::BboChanged(bbo));

        MatchResult { events }
    }

    /// Process a new order: validate, then match, then rest or cancel remainder.
    fn process_new_order(
        &mut self,
        seq_no: SeqNo,
        order: &NewOrder,
        events: &mut Vec<EngineEvent>,
    ) {
        // --- Validation ---
        if order.qty == 0 {
            events.push(EngineEvent::OrderRejected {
                seq_no,
                order_id: order.order_id,
                reason: RejectReason::InvalidQuantity,
            });
            return;
        }
        if order.order_type == OrderType::Limit && order.price == MARKET_PRICE {
            events.push(EngineEvent::OrderRejected {
                seq_no,
                order_id: order.order_id,
                reason: RejectReason::InvalidPrice,
            });
            return;
        }
        if self.book.orders.contains_key(&order.order_id) {
            events.push(EngineEvent::OrderRejected {
                seq_no,
                order_id: order.order_id,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }

        // Accept the order.
        events.push(EngineEvent::OrderAccepted {
            seq_no,
            order_id: order.order_id,
        });

        let mut state = OrderState::from_new_order(order);

        // --- Handle stop orders ---
        if order.condition == OrderCondition::Stop {
            // Stop orders rest as pending (not on book) until triggered.
            state.is_open = true;
            self.book.orders.insert(order.order_id, state);
            return;
        }

        // --- FOK pre-check ---
        if order.tif == TimeInForce::FOK {
            let available = self.available_qty_at_or_better(order.side, order.price, order.order_type);
            if available < order.qty {
                events.push(EngineEvent::OrderRejected {
                    seq_no,
                    order_id: order.order_id,
                    reason: RejectReason::FOKNotFillable,
                });
                return;
            }
        }

        // --- AON pre-check ---
        if order.condition == OrderCondition::AON {
            let available = self.available_qty_at_or_better(order.side, order.price, order.order_type);
            if available < order.qty {
                if order.tif == TimeInForce::IOC {
                    events.push(EngineEvent::OrderRejected {
                        seq_no,
                        order_id: order.order_id,
                        reason: RejectReason::AONNotFillable,
                    });
                    return;
                }
                // GTC AON: rest on book, will try to match when conditions change
                state.is_open = true;
                let side = state.side;
                let price = state.price;
                let qty = state.open_qty();
                let oid = state.order_id;
                self.book.orders.insert(oid, state);
                self.book.side_mut(side).insert(price, oid, qty);
                return;
            }
        }

        // --- Matching ---
        self.match_order(&mut state, seq_no, events);

        let remaining = state.open_qty();

        // --- Post-match: rest or cancel remainder ---
        if remaining > 0 {
            match (order.order_type, order.tif) {
                (OrderType::Market, _) => {
                    // Market orders never rest.
                    if state.filled_qty == 0 {
                        events.push(EngineEvent::OrderRejected {
                            seq_no,
                            order_id: order.order_id,
                            reason: RejectReason::NoLiquidity,
                        });
                    } else {
                        events.push(EngineEvent::OrderCanceled {
                            seq_no,
                            order_id: order.order_id,
                            remaining_qty: remaining,
                        });
                    }
                    state.is_open = false;
                }
                (_, TimeInForce::IOC) => {
                    // IOC: cancel remainder.
                    events.push(EngineEvent::OrderCanceled {
                        seq_no,
                        order_id: order.order_id,
                        remaining_qty: remaining,
                    });
                    state.is_open = false;
                }
                (OrderType::Limit, TimeInForce::GTC) | (OrderType::Limit, TimeInForce::FOK) => {
                    // GTC limit: rest on book.
                    state.is_open = true;
                    let side = state.side;
                    let price = state.price;
                    self.book
                        .side_mut(side)
                        .insert(price, order.order_id, remaining);
                }
            }
        } else {
            state.is_open = false;
        }

        self.book.orders.insert(order.order_id, state);
    }

    /// Run the matching loop: aggressor `state` vs. the opposite side of the book.
    fn match_order(
        &mut self,
        aggressor: &mut OrderState,
        _seq_no: SeqNo,
        events: &mut Vec<EngineEvent>,
    ) {
        loop {
            if aggressor.open_qty() == 0 {
                break;
            }

            // Peek at best resting price.
            let resting_price = match self.book.opposite_side(aggressor.side).best() {
                Some((p, _)) => p,
                None => break,
            };

            // Check price compatibility.
            if aggressor.order_type == OrderType::Limit {
                match aggressor.side {
                    Side::Buy => {
                        if aggressor.price < resting_price {
                            break;
                        }
                    }
                    Side::Sell => {
                        if aggressor.price > resting_price {
                            break;
                        }
                    }
                }
            }

            // Pop the front order at the best level.
            let (_, maker_id) = match self.book.opposite_side_mut(aggressor.side).pop_front() {
                Some(x) => x,
                None => break,
            };

            // Look up maker and compute fill qty. We read from the orders map
            // without holding a mutable ref across the book-side mutation.
            let maker_open = match self.book.orders.get(&maker_id) {
                Some(o) => o.open_qty(),
                None => continue,
            };

            let fill_qty = aggressor.open_qty().min(maker_open);
            let fill_price = resting_price;

            // Update maker state.
            let (maker_complete, maker_remaining) = {
                let maker = self.book.orders.get_mut(&maker_id).unwrap();
                maker.filled_qty += fill_qty;
                let complete = maker.open_qty() == 0;
                if complete {
                    maker.is_open = false;
                }
                (complete, maker.open_qty())
            };

            // Update opposite book side.
            let opposite_side = self.book.opposite_side_mut(aggressor.side);
            opposite_side.reduce_qty(resting_price, fill_qty);

            if !maker_complete {
                let key = opposite_side.key(resting_price);
                if let Some(level) = opposite_side.levels.get_mut(&key) {
                    level.orders.push_front(maker_id);
                } else {
                    opposite_side.insert(resting_price, maker_id, maker_remaining);
                }
            }

            aggressor.filled_qty += fill_qty;
            let taker_complete = aggressor.open_qty() == 0;

            events.push(EngineEvent::Trade(Fill {
                maker_order_id: maker_id,
                taker_order_id: aggressor.order_id,
                price: fill_price,
                qty: fill_qty,
                maker_filled_completely: maker_complete,
                taker_filled_completely: taker_complete,
            }));
        }
    }

    /// Calculate how much qty is available on the opposite side at or better
    /// than the given price. Used for FOK/AON pre-checks.
    fn available_qty_at_or_better(
        &self,
        aggressor_side: Side,
        price: Price,
        order_type: OrderType,
    ) -> Qty {
        let opposite = self.book.opposite_side(aggressor_side);
        let mut total = 0u64;
        for (&key, level) in opposite.levels.iter() {
            let level_price = opposite.price_from_key(key);
            if order_type == OrderType::Limit {
                match aggressor_side {
                    Side::Buy => {
                        if level_price > price {
                            break;
                        }
                    }
                    Side::Sell => {
                        if level_price < price {
                            break;
                        }
                    }
                }
            }
            total += level.total_qty;
        }
        total
    }

    /// Process a cancel request.
    fn process_cancel(
        &mut self,
        seq_no: SeqNo,
        cancel: &CancelOrder,
        events: &mut Vec<EngineEvent>,
    ) {
        let state = match self.book.orders.get(&cancel.order_id) {
            Some(s) if s.is_open => s.clone(),
            Some(_) => {
                events.push(EngineEvent::CancelRejected {
                    seq_no,
                    order_id: cancel.order_id,
                    reason: RejectReason::NothingToCancel,
                });
                return;
            }
            None => {
                events.push(EngineEvent::CancelRejected {
                    seq_no,
                    order_id: cancel.order_id,
                    reason: RejectReason::UnknownOrder,
                });
                return;
            }
        };

        let remaining = state.open_qty();
        self.book
            .side_mut(state.side)
            .remove(state.price, cancel.order_id, remaining);

        let order = self.book.orders.get_mut(&cancel.order_id).unwrap();
        order.is_open = false;

        events.push(EngineEvent::OrderCanceled {
            seq_no,
            order_id: cancel.order_id,
            remaining_qty: remaining,
        });
    }

    /// Process a replace request.
    ///
    /// Rules:
    /// - The order must be open (resting on book).
    /// - Quantity can only be reduced (down-amend). Increasing quantity is rejected.
    /// - Price change causes the order to lose time priority (removed and re-inserted).
    /// - Quantity reduction only (same price) preserves time priority.
    fn process_replace(
        &mut self,
        seq_no: SeqNo,
        replace: &ReplaceOrder,
        events: &mut Vec<EngineEvent>,
    ) {
        let state = match self.book.orders.get(&replace.order_id) {
            Some(s) if s.is_open => s.clone(),
            Some(_) => {
                events.push(EngineEvent::ReplaceRejected {
                    seq_no,
                    order_id: replace.order_id,
                    reason: RejectReason::NothingToCancel,
                });
                return;
            }
            None => {
                events.push(EngineEvent::ReplaceRejected {
                    seq_no,
                    order_id: replace.order_id,
                    reason: RejectReason::UnknownOrder,
                });
                return;
            }
        };

        // Validate new_qty: must cover already-filled amount, and total must not increase.
        if replace.new_qty == 0 {
            events.push(EngineEvent::ReplaceRejected {
                seq_no,
                order_id: replace.order_id,
                reason: RejectReason::InvalidQuantity,
            });
            return;
        }

        if replace.new_qty > state.original_qty {
            events.push(EngineEvent::ReplaceRejected {
                seq_no,
                order_id: replace.order_id,
                reason: RejectReason::ReplaceQtyIncrease,
            });
            return;
        }

        if replace.new_price == MARKET_PRICE {
            events.push(EngineEvent::ReplaceRejected {
                seq_no,
                order_id: replace.order_id,
                reason: RejectReason::InvalidPrice,
            });
            return;
        }

        // new_qty must be greater than filled_qty (must leave open qty)
        if replace.new_qty <= state.filled_qty {
            events.push(EngineEvent::ReplaceRejected {
                seq_no,
                order_id: replace.order_id,
                reason: RejectReason::InvalidQuantity,
            });
            return;
        }

        let old_open_qty = state.open_qty();
        let new_open_qty = replace.new_qty - state.filled_qty;

        // Remove from book.
        self.book
            .side_mut(state.side)
            .remove(state.price, replace.order_id, old_open_qty);

        // Update state.
        let order = self.book.orders.get_mut(&replace.order_id).unwrap();
        order.original_qty = replace.new_qty;
        order.price = replace.new_price;

        // Re-insert at back of new price level (loses priority if price changed,
        // but also if qty changed to simplify; matching Liquibook behavior).
        self.book
            .side_mut(state.side)
            .insert(replace.new_price, replace.order_id, new_open_qty);

        events.push(EngineEvent::OrderReplaced {
            seq_no,
            order_id: replace.order_id,
            new_price: replace.new_price,
            new_qty: replace.new_qty,
        });

        // After replace, the new price may cross the opposite side: re-match.
        // Clone the current state to run matching.
        let mut aggressor_state = self.book.orders.get(&replace.order_id).unwrap().clone();
        let pre_fill = aggressor_state.filled_qty;
        self.match_order(&mut aggressor_state, seq_no, events);

        if aggressor_state.filled_qty > pre_fill {
            // Remove from resting side if any fills happened.
            let open = aggressor_state.open_qty();
            let side = aggressor_state.side;
            let price = aggressor_state.price;

            if open == 0 {
                self.book.side_mut(side).remove(price, replace.order_id, 0);
                aggressor_state.is_open = false;
            } else {
                // Update the resting qty.
                let filled_delta = aggressor_state.filled_qty - pre_fill;
                self.book.side_mut(side).reduce_qty(price, filled_delta);
            }

            *self.book.orders.get_mut(&replace.order_id).unwrap() = aggressor_state;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn new_limit_order(id: OrderId, side: Side, price: Price, qty: Qty) -> NewOrder {
        NewOrder {
            order_id: id,
            symbol: 1,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::GTC,
            condition: OrderCondition::None,
            price,
            qty,
            stop_price: 0,
        }
    }

    fn new_market_order(id: OrderId, side: Side, qty: Qty) -> NewOrder {
        NewOrder {
            order_id: id,
            symbol: 1,
            side,
            order_type: OrderType::Market,
            tif: TimeInForce::GTC,
            condition: OrderCondition::None,
            price: MARKET_PRICE,
            qty,
            stop_price: 0,
        }
    }

    fn cmd(seq_no: SeqNo, payload: CommandPayload) -> Command {
        Command {
            seq_no,
            timestamp: seq_no * 1000,
            payload,
        }
    }

    fn has_event(events: &[EngineEvent], pred: impl Fn(&EngineEvent) -> bool) -> bool {
        events.iter().any(pred)
    }

    fn count_trades(events: &[EngineEvent]) -> usize {
        events.iter().filter(|e| matches!(e, EngineEvent::Trade(_))).count()
    }

    fn total_filled(events: &[EngineEvent]) -> Qty {
        events
            .iter()
            .filter_map(|e| match e {
                EngineEvent::Trade(f) => Some(f.qty),
                _ => None,
            })
            .sum()
    }

    // --- New order tests ---

    #[test]
    fn limit_buy_rests_on_empty_book() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));

        assert!(has_event(&result.events, |e| matches!(e, EngineEvent::OrderAccepted { order_id: 1, .. })));
        assert_eq!(count_trades(&result.events), 0);
        assert_eq!(engine.book.bids.level_count(), 1);
        assert!(engine.book.orders[&1].is_open);
    }

    #[test]
    fn limit_sell_rests_on_empty_book() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));

        assert!(has_event(&result.events, |e| matches!(e, EngineEvent::OrderAccepted { order_id: 1, .. })));
        assert_eq!(engine.book.asks.level_count(), 1);
    }

    #[test]
    fn limit_buy_matches_resting_sell() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Buy, 100, 10))));

        assert_eq!(count_trades(&result.events), 1);
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::Trade(f) if f.price == 100 && f.qty == 10 && f.maker_order_id == 1 && f.taker_order_id == 2)
        }));
        assert!(engine.book.asks.is_empty());
        assert!(engine.book.bids.is_empty());
    }

    #[test]
    fn partial_fill_leaves_maker_on_book() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 20))));
        let result = engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Buy, 100, 10))));

        assert_eq!(total_filled(&result.events), 10);
        assert!(!engine.book.asks.is_empty());
        assert_eq!(engine.book.orders[&1].open_qty(), 10);
    }

    #[test]
    fn price_time_priority() {
        let mut engine = MatchingEngine::new(1);
        // Two sells at different prices.
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 101, 10))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Sell, 100, 10))));

        // Buy should match the 100 ask first (better price).
        let result = engine.process(&cmd(3, CommandPayload::New(new_limit_order(3, Side::Buy, 101, 5))));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::Trade(f) if f.price == 100 && f.maker_order_id == 2)
        }));
    }

    #[test]
    fn fifo_priority_within_price() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Sell, 100, 10))));

        // Buy 5: should match order 1 first (FIFO).
        let result = engine.process(&cmd(3, CommandPayload::New(new_limit_order(3, Side::Buy, 100, 5))));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::Trade(f) if f.maker_order_id == 1 && f.qty == 5)
        }));
    }

    #[test]
    fn market_buy_fills_at_best_ask() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::New(new_market_order(2, Side::Buy, 10))));

        assert_eq!(count_trades(&result.events), 1);
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::Trade(f) if f.price == 100)
        }));
    }

    #[test]
    fn market_order_no_liquidity_rejected() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_market_order(1, Side::Buy, 10))));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderRejected { reason: RejectReason::NoLiquidity, .. })
        }));
    }

    #[test]
    fn market_order_partial_fill_canceled() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 5))));
        let result = engine.process(&cmd(2, CommandPayload::New(new_market_order(2, Side::Buy, 10))));

        assert_eq!(total_filled(&result.events), 5);
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderCanceled { remaining_qty: 5, .. })
        }));
    }

    #[test]
    fn reject_zero_qty() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 0))));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderRejected { reason: RejectReason::InvalidQuantity, .. })
        }));
    }

    #[test]
    fn reject_duplicate_order_id() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderRejected { reason: RejectReason::DuplicateOrderId, .. })
        }));
    }

    #[test]
    fn reject_limit_with_zero_price() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 0, 10))));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderRejected { reason: RejectReason::InvalidPrice, .. })
        }));
    }

    // --- Cancel tests ---

    #[test]
    fn cancel_resting_order() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::Cancel(CancelOrder {
            order_id: 1,
            symbol: 1,
        })));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderCanceled { order_id: 1, remaining_qty: 10, .. })
        }));
        assert!(engine.book.bids.is_empty());
        assert!(!engine.book.orders[&1].is_open);
    }

    #[test]
    fn cancel_unknown_order() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::Cancel(CancelOrder {
            order_id: 99,
            symbol: 1,
        })));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::CancelRejected { reason: RejectReason::UnknownOrder, .. })
        }));
    }

    #[test]
    fn cancel_already_filled_order() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Buy, 100, 10))));
        // Order 1 is fully filled, cancel should be rejected.
        let result = engine.process(&cmd(3, CommandPayload::Cancel(CancelOrder {
            order_id: 1,
            symbol: 1,
        })));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::CancelRejected { reason: RejectReason::NothingToCancel, .. })
        }));
    }

    // --- Replace tests ---

    #[test]
    fn replace_reduce_qty() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 20))));
        let result = engine.process(&cmd(2, CommandPayload::Replace(ReplaceOrder {
            order_id: 1,
            symbol: 1,
            new_price: 100,
            new_qty: 15,
        })));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderReplaced { order_id: 1, new_qty: 15, .. })
        }));
        assert_eq!(engine.book.orders[&1].original_qty, 15);
    }

    #[test]
    fn replace_increase_qty_rejected() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::Replace(ReplaceOrder {
            order_id: 1,
            symbol: 1,
            new_price: 100,
            new_qty: 20,
        })));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::ReplaceRejected { reason: RejectReason::ReplaceQtyIncrease, .. })
        }));
    }

    #[test]
    fn replace_change_price() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        let result = engine.process(&cmd(2, CommandPayload::Replace(ReplaceOrder {
            order_id: 1,
            symbol: 1,
            new_price: 101,
            new_qty: 10,
        })));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderReplaced { new_price: 101, .. })
        }));
        assert_eq!(engine.book.orders[&1].price, 101);

        // Old price level should be empty, new one populated.
        let bbo = engine.book.bbo();
        assert_eq!(bbo.bid_price, Some(101));
    }

    #[test]
    fn replace_crosses_book_fills() {
        let mut engine = MatchingEngine::new(1);
        // Sell at 100.
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        // Buy at 99 (no match).
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Buy, 99, 10))));
        // Replace buy price to 100 -> should fill.
        let result = engine.process(&cmd(3, CommandPayload::Replace(ReplaceOrder {
            order_id: 2,
            symbol: 1,
            new_price: 100,
            new_qty: 10,
        })));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::Trade(f) if f.qty == 10)
        }));
    }

    // --- BBO event test ---

    #[test]
    fn bbo_emitted_after_every_command() {
        let mut engine = MatchingEngine::new(1);
        let result = engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));

        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::BboChanged(bbo) if bbo.bid_price == Some(100) && bbo.bid_qty == 10)
        }));
    }

    // --- Book invariant test ---

    #[test]
    fn book_never_crossed_after_matching() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Buy, 100, 10))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Sell, 101, 10))));
        assert!(engine.book.is_uncrossed());

        // This cross should resolve via matching.
        engine.process(&cmd(3, CommandPayload::New(new_limit_order(3, Side::Buy, 101, 5))));
        assert!(engine.book.is_uncrossed());
    }

    // --- IOC ---

    #[test]
    fn ioc_fills_partial_cancels_rest() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 5))));

        let ioc_order = NewOrder {
            order_id: 2,
            symbol: 1,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::IOC,
            condition: OrderCondition::None,
            price: 100,
            qty: 10,
            stop_price: 0,
        };
        let result = engine.process(&cmd(2, CommandPayload::New(ioc_order)));

        assert_eq!(total_filled(&result.events), 5);
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderCanceled { remaining_qty: 5, .. })
        }));
        assert!(!engine.book.orders[&2].is_open);
    }

    #[test]
    fn ioc_no_match_cancels_all() {
        let mut engine = MatchingEngine::new(1);

        let ioc_order = NewOrder {
            order_id: 1,
            symbol: 1,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::IOC,
            condition: OrderCondition::None,
            price: 100,
            qty: 10,
            stop_price: 0,
        };
        let result = engine.process(&cmd(1, CommandPayload::New(ioc_order)));

        assert_eq!(count_trades(&result.events), 0);
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderCanceled { remaining_qty: 10, .. })
        }));
    }

    // --- FOK ---

    #[test]
    fn fok_fills_entirely_or_rejects() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));

        // FOK for 10: should fill.
        let fok_order = NewOrder {
            order_id: 2,
            symbol: 1,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::FOK,
            condition: OrderCondition::None,
            price: 100,
            qty: 10,
            stop_price: 0,
        };
        let result = engine.process(&cmd(2, CommandPayload::New(fok_order)));
        assert_eq!(total_filled(&result.events), 10);
    }

    #[test]
    fn fok_insufficient_liquidity_rejected() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 5))));

        let fok_order = NewOrder {
            order_id: 2,
            symbol: 1,
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::FOK,
            condition: OrderCondition::None,
            price: 100,
            qty: 10,
            stop_price: 0,
        };
        let result = engine.process(&cmd(2, CommandPayload::New(fok_order)));
        assert!(has_event(&result.events, |e| {
            matches!(e, EngineEvent::OrderRejected { reason: RejectReason::FOKNotFillable, .. })
        }));
        // Resting order should be untouched.
        assert_eq!(engine.book.orders[&1].open_qty(), 5);
    }

    // --- Multi-level sweep ---

    #[test]
    fn aggressive_buy_sweeps_multiple_price_levels() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 10))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Sell, 101, 10))));
        engine.process(&cmd(3, CommandPayload::New(new_limit_order(3, Side::Sell, 102, 10))));

        let result = engine.process(&cmd(4, CommandPayload::New(new_limit_order(4, Side::Buy, 102, 25))));

        // Should fill 10@100 + 10@101 + 5@102 = 25
        assert_eq!(total_filled(&result.events), 25);
        assert_eq!(count_trades(&result.events), 3);
        // Remaining 5 at 102 should still be on the book.
        assert_eq!(engine.book.orders[&3].open_qty(), 5);
    }

    // --- Accounting invariant ---

    #[test]
    fn filled_plus_open_plus_canceled_equals_original() {
        let mut engine = MatchingEngine::new(1);
        engine.process(&cmd(1, CommandPayload::New(new_limit_order(1, Side::Sell, 100, 20))));
        engine.process(&cmd(2, CommandPayload::New(new_limit_order(2, Side::Buy, 100, 12))));
        engine.process(&cmd(3, CommandPayload::Cancel(CancelOrder {
            order_id: 1,
            symbol: 1,
        })));

        let order = &engine.book.orders[&1];
        // filled=12, canceled remainder=8, total=20
        assert_eq!(order.filled_qty, 12);
        assert_eq!(order.original_qty, 20);
        assert!(!order.is_open);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for generating a random side.
    fn side_strategy() -> impl Strategy<Value = Side> {
        prop_oneof![Just(Side::Buy), Just(Side::Sell)]
    }

    /// Strategy for generating a random limit order command.
    fn limit_order_strategy(
        _id_range: std::ops::RangeInclusive<u64>,
    ) -> impl Strategy<Value = (Side, Price, Qty)> {
        (
            side_strategy(),
            95u64..=105u64,   // price range
            1u64..=50u64,     // qty range
        )
    }

    /// Helper: verify all engine invariants after a sequence of commands.
    fn check_invariants(engine: &MatchingEngine) {
        // 1. Book must be uncrossed.
        assert!(
            engine.book.is_uncrossed(),
            "Book is crossed! bid={:?} ask={:?}",
            engine.book.bids.best().map(|(p, _)| p),
            engine.book.asks.best().map(|(p, _)| p),
        );

        // 2. No resting order with zero or negative open qty.
        for order in engine.book.orders.values() {
            if order.is_open {
                assert!(
                    order.open_qty() > 0,
                    "Open order {} has zero open qty (orig={}, filled={})",
                    order.order_id,
                    order.original_qty,
                    order.filled_qty,
                );
            }
        }

        // 3. Filled qty never exceeds original qty.
        for order in engine.book.orders.values() {
            assert!(
                order.filled_qty <= order.original_qty,
                "Order {} overfilled: filled={} > original={}",
                order.order_id,
                order.filled_qty,
                order.original_qty,
            );
        }

        // 4. FIFO: within each price level, order IDs appear in insertion order.
        //    (We can't easily verify this without tracking insertion order, but
        //    we can at least verify orders on the book exist in the orders map.)
        for (_, level) in &engine.book.bids.levels {
            for &oid in &level.orders {
                assert!(
                    engine.book.orders.contains_key(&oid),
                    "Bid level contains unknown order {}",
                    oid,
                );
                let order = &engine.book.orders[&oid];
                assert!(order.is_open, "Bid level contains closed order {}", oid);
                assert_eq!(order.side, Side::Buy);
            }
        }
        for (_, level) in &engine.book.asks.levels {
            for &oid in &level.orders {
                assert!(
                    engine.book.orders.contains_key(&oid),
                    "Ask level contains unknown order {}",
                    oid,
                );
                let order = &engine.book.orders[&oid];
                assert!(order.is_open, "Ask level contains closed order {}", oid);
                assert_eq!(order.side, Side::Sell);
            }
        }

        // 5. Aggregate quantity at each level matches sum of open qtys.
        for (&key, level) in &engine.book.bids.levels {
            let sum: Qty = level
                .orders
                .iter()
                .map(|&oid| engine.book.orders[&oid].open_qty())
                .sum();
            assert_eq!(
                level.total_qty, sum,
                "Bid level qty mismatch at key {}: tracked={} computed={}",
                key, level.total_qty, sum,
            );
        }
        for (&key, level) in &engine.book.asks.levels {
            let sum: Qty = level
                .orders
                .iter()
                .map(|&oid| engine.book.orders[&oid].open_qty())
                .sum();
            assert_eq!(
                level.total_qty, sum,
                "Ask level qty mismatch at key {}: tracked={} computed={}",
                key, level.total_qty, sum,
            );
        }
    }

    proptest! {
        /// Random sequence of limit orders: invariants must always hold.
        #[test]
        fn random_limit_orders_maintain_invariants(
            orders in proptest::collection::vec(limit_order_strategy(1..=1000), 1..200)
        ) {
            let mut engine = MatchingEngine::new(1);
            for (i, (side, price, qty)) in orders.into_iter().enumerate() {
                let seq = (i + 1) as SeqNo;
                let cmd = Command {
                    seq_no: seq,
                    timestamp: seq * 1000,
                    payload: CommandPayload::New(NewOrder {
                        order_id: seq,
                        symbol: 1,
                        side,
                        order_type: OrderType::Limit,
                        tif: TimeInForce::GTC,
                        condition: OrderCondition::None,
                        price,
                        qty,
                        stop_price: 0,
                    }),
                };
                engine.process(&cmd);
                check_invariants(&engine);
            }
        }

        /// Random limit orders followed by random cancels: invariants hold.
        #[test]
        fn random_orders_and_cancels(
            order_count in 10u64..100u64,
            cancel_indices in proptest::collection::vec(0usize..100, 1..50),
        ) {
            let mut engine = MatchingEngine::new(1);
            let _rng_price = 95u64;
            let mut seq = 0u64;

            // Insert orders.
            for i in 1..=order_count {
                seq += 1;
                let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                let price = 95 + (i % 11);
                let cmd = Command {
                    seq_no: seq,
                    timestamp: seq * 1000,
                    payload: CommandPayload::New(NewOrder {
                        order_id: seq,
                        symbol: 1,
                        side,
                        order_type: OrderType::Limit,
                        tif: TimeInForce::GTC,
                        condition: OrderCondition::None,
                        price,
                        qty: 10,
                        stop_price: 0,
                    }),
                };
                engine.process(&cmd);
            }

            // Cancel random orders.
            for &idx in &cancel_indices {
                let oid = (idx as u64 % order_count) + 1;
                seq += 1;
                let cmd = Command {
                    seq_no: seq,
                    timestamp: seq * 1000,
                    payload: CommandPayload::Cancel(CancelOrder {
                        order_id: oid,
                        symbol: 1,
                    }),
                };
                engine.process(&cmd);
                check_invariants(&engine);
            }
        }

        /// Deterministic replay: same commands produce same events.
        #[test]
        fn deterministic_replay(
            orders in proptest::collection::vec(limit_order_strategy(1..=1000), 1..50)
        ) {
            let cmds: Vec<_> = orders
                .into_iter()
                .enumerate()
                .map(|(i, (side, price, qty))| {
                    let seq = (i + 1) as SeqNo;
                    Command {
                        seq_no: seq,
                        timestamp: seq * 1000,
                        payload: CommandPayload::New(NewOrder {
                            order_id: seq,
                            symbol: 1,
                            side,
                            order_type: OrderType::Limit,
                            tif: TimeInForce::GTC,
                            condition: OrderCondition::None,
                            price,
                            qty,
                            stop_price: 0,
                        }),
                    }
                })
                .collect();

            // Run 1.
            let mut engine1 = MatchingEngine::new(1);
            let events1: Vec<_> = cmds.iter().flat_map(|c| engine1.process(c).events).collect();

            // Run 2.
            let mut engine2 = MatchingEngine::new(1);
            let events2: Vec<_> = cmds.iter().flat_map(|c| engine2.process(c).events).collect();

            prop_assert_eq!(&events1, &events2, "Event streams differ across runs");
            prop_assert_eq!(engine1.book.last_seq_no, engine2.book.last_seq_no);
        }
    }
}
